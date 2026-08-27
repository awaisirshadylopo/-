# Import necessary modules
import json
import os
import logging
import pandas as pd
import psycopg2
from psycopg2 import extras
from psycopg2.extras import execute_values
import boto3
import traceback
import io

logging.basicConfig(format="%(levelname)s - %(message)s", force=True)
logger = logging.getLogger("mls-etl-action-func")
logger.setLevel("INFO")

rds_connection = None
avalon_connection = None


def get_connection(secret, connection, sqlExecLimit):
    try:
        if connection is not None and connection.closed == 0:
            with connection.cursor() as cur:
                cur.execute("SELECT 1")
            return connection
    except Exception:
        pass

    return psycopg2.connect(
        dbname=secret["dbname"],
        user=secret["username"],
        password=secret["password"],
        host=secret["host"],
        port=secret["port"],
        connect_timeout=15,
        options=f"-c statement_timeout={sqlExecLimit}",
    )


# Function to fetch secrets from AWS Secrets Manager
def fetch_secrets(secret_name):
    # Initialize AWS Secrets Manager client
    client = boto3.client("secretsmanager")

    # Retrieve secret value using the provided secret name
    response = client.get_secret_value(SecretId=secret_name)

    # Parse and return the secret as a dictionary
    secret = json.loads(response["SecretString"])
    return secret


def ensure_folder_structure(s3, bucket, path):
    """Creates zero-byte folder marker objects in S3 for each level of the path."""
    parts = path.strip("/").split("/")
    cumulative_path = ""
    for part in parts:
        cumulative_path += part + "/"
        s3.put_object(Bucket=bucket, Key=cumulative_path)  # Creates "folder"


def Upload_data_into_S3_DataLake(
    df_upload, source_id, source_type, source_name, batch_id, class_Name
):
    """
    Uploads a DataFrame as a Parquet file to S3.

    S3 path structure:
        {source_type}/{source_id}_{source_name}/{batch_id}/{class_Name}/{source_name}_{class_Name}.parquet

    Example:
        direct_idx/123_MyMLS/456/etl_action_pool/MyMLS_etl_action_pool_INSERT.parquet
    """
    # Construct filename and folder path
    filename = f"{source_name}_{class_Name}.parquet"
    folder_path = f"{source_type}/{source_id}_{source_name}/{batch_id}/{class_Name}/"
    s3_key = folder_path + filename

    # Clean column names — replace dots with underscores (Parquet/Athena requirement)
    df_upload.columns = df_upload.columns.astype(str).str.replace(".", "_")

    # Clean values — convert to str, then replace null-like strings with None
    df_upload = df_upload.astype(str).replace(["nan", "None", ""], None)

    # Convert DataFrame to Parquet in memory (no disk I/O needed in Lambda)
    buffer = io.BytesIO()
    df_upload.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)

    # Upload to S3
    s3 = boto3.client("s3")
    bucket_name = os.environ.get("bucket_name")

    # Create folder marker objects (cosmetic, not strictly required in S3)
    ensure_folder_structure(s3, bucket_name, folder_path)

    # Upload the parquet file
    s3.put_object(Bucket=bucket_name, Key=s3_key, Body=buffer.getvalue())

    log_msg = {
        "Status": "Uploaded to S3",
        "s3_key": s3_key,
        "bucket": bucket_name,
        "row_count": len(df_upload),
    }
    logger.info(log_msg)


# Function to retrieve source listings from the generic_idx_listing table
def get_source_listings_from_generic_idx_listing(source_id, rds_cursor):
    # Build and execute a SQL query to retrieve source_listing_id
    generic_listing_ids = "SELECT distinct source_listing_id from stage.direct_idx_listing where source_id = {}".format(
        source_id
    )
    rds_cursor.execute(generic_listing_ids)
    generic_listings = rds_cursor.fetchall()
    generic_listings = [i[0] for i in generic_listings]
    generic_listings.append("0")  # Adding a dummy value to avoid empty list issues
    log_msg = {
        "source_id": source_id,
        "Listing Count": len(generic_listings),
        "Status": "Got Lisiting from Generic IDX Lisiting table",
    }

    logger.info(log_msg)

    return generic_listings


# Function to get required columns from the public.listing table
def get_required_columns_from_public_listings(
    source_id, generic_listings, avalon_cursor
):
    # Build and execute a SQL query to retrieve required columns
    public_listing_data = f"""
        SELECT 
        distinct on (source_listing_id)
        source_id, 
        batch_id, id as listing_id, source_listing_id, 
        case when modification_timestamp::text is null then '1990-01-01 06:22:03.500 +0500' else modification_timestamp::text end::timestamp, 
        case when media_modification_timestamp::text is null then '1990-01-01 06:22:03.500 +0500' else media_modification_timestamp::text end::timestamp,
        source_status, price, coalesce(y_creation_date, '1990-01-01')::timestamp AS y_creation_date, coalesce(y_last_update_date, '1990-01-01')::timestamp AS y_last_update_date,
		listing_status_id, sold_date AS sold_date, sold_price,coalesce(source_last_update_date, '1990-01-01')::timestamp AS source_last_update_date,
        coalesce(inactive_date, '1990-01-01')::timestamp AS inactive_date 
        from public.listing 
        where source_id = {source_id} and source_listing_id in {generic_listings}
    """
    log_msg = {
        "source_id": source_id,
        "Listing Count": len(generic_listings),
        "Query": public_listing_data,
    }
    logger.info(log_msg)
    public_listing_data = public_listing_data.replace("[", "(").replace("]", ")")
    avalon_cursor.execute(public_listing_data)
    public_listing_data = avalon_cursor.fetchall()
    column_names = [desc[0] for desc in avalon_cursor.description]
    public_listing_df = pd.DataFrame(public_listing_data, columns=column_names)
    log_msg = {"Status": "Required columns fetched from public listings"}
    logger.info(log_msg)

    return public_listing_df


# Function to delete records from the listing_lookup table
def delete_from_listing_lookup(source_id, rds_cursor, rds_connection):
    delete_lookup_data = "DELETE from stage.listing_lookup where source_id ={}".format(
        source_id
    )
    rds_cursor.execute(delete_lookup_data)
    rds_connection.commit()
    log_msg = {
        "source_id": source_id,
        "Listing Count": rds_cursor.rowcount,
        "Status": "Deleted from Lisiting Lookup table",
    }
    logger.info(log_msg)


# Function to load records into the listing_lookup table
def load_into_listing_lookup(public_listing_df, rds_cursor, rds_connection):

    # Convert DataFrame to list of tuples
    data_tuples = [tuple(x) for x in public_listing_df.to_numpy()]
    # Create column names string
    columns = ", ".join(public_listing_df.columns)
    # Create a placeholder string, e.g., (%s, %s, %s) for 3 columns
    placeholders = ", ".join(["%s"] * len(public_listing_df.columns))
    # SQL query for inserting data
    insert_query = (
        f"INSERT INTO stage.listing_lookup ({columns}) VALUES ({placeholders})"
    )
    rds_cursor.executemany(insert_query, data_tuples)
    rds_connection.commit()

    if public_listing_df.empty:
        log_msg = {"Status": "No Listing match"}
        logger.info(log_msg)
    else:
        log_msg = {"Status": "added in the listing lookup table"}
        logger.info(log_msg)


# Function to delete records from etl_insert and etl_update tables
def delete_etl_insert_and_update(
    source_id, rds_cursor, rds_connection, avalon_cursor, avalon_connection
):
    del_insert = (
        "DELETE from stage.etl_direct_idx_insert_listings where source_id = {}".format(
            source_id
        )
    )
    del_update = (
        "DELETE from stage.etl_direct_idx_update_listings where source_id = {}".format(
            source_id
        )
    )
    del_delete = (
        "DELETE from stage.etl_direct_idx_delete_listings where source_id = {}".format(
            source_id
        )
    )
    del_mlsgrid_photos_temp = (
        "DELETE from stage.etl_mlsgrid_photos_temp where source_id = {}".format(
            source_id
        )
    )
    del_etl_action_mlsgrid_photos = (
        "DELETE from stage.etl_action_mlsgrid_photos where source_id = {}".format(
            source_id
        )
    )
    rds_cursor.execute(del_insert)
    rds_cursor.execute(del_update)
    rds_cursor.execute(del_mlsgrid_photos_temp)
    rds_cursor.execute(del_etl_action_mlsgrid_photos)
    rds_connection.commit()
    avalon_cursor.execute(del_delete)
    avalon_connection.commit()

    log_msg = {"Status": "Deleted ETL insert and updates"}
    logger.info(log_msg)


# Function to perform ETL insert operation
def etl_insert(
    generic_listings, public_listing_df, source_id, rds_cursor, rds_connection
):
    lookup_listings = list(public_listing_df["source_listing_id"])
    insert_listings = [item for item in generic_listings if item not in lookup_listings]

    if not len(insert_listings) == 0:
        # Build and execute a SQL query to fetch data for ETL insert
        insert_query = """
        SELECT distinct on (s.source_listing_id) s.batch_id, s.source_id, s.source_listing_id, s.mls_number, s.y_creation_date from stage.direct_idx_listing s 
        JOIN listing_status ls 
        ON s.listing_status = ls.status AND s.source_id = ls.source_id 
        where s.source_id = {} and s.source_listing_id in {} 
        AND (ls.load_flag IS TRUE OR ls.display_flag IS TRUE) 
        AND ( ls.ylopo_status <> 'SOLD' OR s.sold_date IS NULL OR (ls.ylopo_status = 'SOLD' AND s.sold_date >= CURRENT_TIMESTAMP - interval '3 year'))
        """.format(source_id, str(insert_listings).replace("[", "(").replace("]", ")"))
        rds_cursor.execute(insert_query)
        insert_query = rds_cursor.fetchall()
        column_names = [desc[0] for desc in rds_cursor.description]
        insert_df = pd.DataFrame(insert_query, columns=column_names)
        data = insert_df.values.tolist()
        insert_query = """
        INSERT INTO stage.etl_direct_idx_insert_listings ( batch_id, source_id, source_listing_id,
         mls_number, y_creation_date)
        VALUES (%s,%s,%s,%s,%s);
        """
        # Execute the insertion query
        rds_cursor.executemany(insert_query, data)
        rds_connection.commit()
        log_msg = {"Lisiting added in elt_insert table": len(insert_df)}
        logger.info(log_msg)

        return len(insert_df)
    else:
        log_msg = {"Status": "No Data"}
        logger.info(log_msg)

        return 0


# Function to perform ETL update operation
def etl_update(
    generic_listings, public_listing_df, source_id, rds_cursor, rds_connection
):
    # Build and execute a SQL query to fetch data for ETL update
    update_query = f"""
        SELECT 
        distinct on (l.source_listing_id)
        l.batch_id, 
        l.source_id, 
        l.source_listing_id,
        ll.listing_id as target_listing_id,
        ll.media_modification_timestamp 
        from stage.direct_idx_listing l 
        inner join stage.listing_lookup ll 
        on l.source_listing_id  = ll.source_listing_id and l.source_id = ll.source_id 
        where ll.source_id = {source_id} and 
        (l.modification_timestamp > ll.modification_timestamp OR l.media_modification_timestamp > ll.media_modification_timestamp  OR l.modification_timestamp is null OR l.media_modification_timestamp is null) 
    """
    rds_cursor.execute(update_query)
    update_df = rds_cursor.fetchall()
    column_names = [desc[0] for desc in rds_cursor.description]
    update_df = pd.DataFrame(update_df, columns=column_names)
    data = update_df.values.tolist()

    # Define the SQL query for insertion
    insert_query = """
    INSERT INTO stage.etl_direct_idx_update_listings (  batch_id, source_id, 
    source_listing_id, target_listing_id,media_modification_timestamp)
    VALUES (%s,%s,%s,%s,%s);
    """

    # Execute the insertion query
    rds_cursor.executemany(insert_query, data)
    rds_connection.commit()
    log_msg = {"Lisiting added in elt_update table": len(update_df)}
    logger.info(log_msg)

    return len(update_df)


# Function to perform ETL Delete operation
def etl_delete(
    public_listing_df,
    source_id,
    batch_id,
    rds_cursor,
    rds_connection,
    avalon_cursor,
    avalon_connection,
):
    # Build and execute a SQL query to fetch data for ETL update
    delete_query = """select distinct on (temp.listing_id)
	temp.source_listing_id
	,temp.listing_id as target_listing_id
	,temp.source_id
	,temp.batch_id
	,temp.listing_status
	,temp.source_last_update_date
	,temp.source_status
	,temp.sold_price
	,temp.sold_date
	from  (
	select l.listing_id, l.source_listing_id, l.source_id, s.batch_id
	, s.listing_status
	, s.source_last_update_date
	,case when upper(sm.ylopo_status)='SOLD' then 'SOLD' else 'INACTIVE' end as source_status
	,case when upper(sm.ylopo_status)='SOLD' then s.sold_price else null end as sold_price
	,case when upper(sm.ylopo_status)='SOLD' then s.sold_date else null end as sold_date
	,1 as rank
	from stage.listing_lookup l
	join listing_status ls on l.listing_status_id = ls.id
	
	join stage.direct_idx_listing s
		on l.source_listing_id = s.source_listing_id 
		and s.source_id = {0} 
		and s.batch_id = {1}
	join listing_status sm
		on sm.status = s.listing_status
		and sm.source_id=s.source_id	
	where
	l.source_id = {0} 
	and sm.display_flag is false
	and s.listing_status != ls.status	

	UNION 
	
    SELECT l.listing_id,l.source_listing_id, l.source_id, sl.batch_id, s.status as listing_status , sl.source_last_update_date
	,case when upper(s.ylopo_status)='SOLD' then 'SOLD' else 'INACTIVE' end as source_status, sl.sold_price, sl.sold_date, 2 as rank
	from stage.listing_lookup l
    join stage.direct_idx_listing sl
		on l.source_listing_id = sl.source_listing_id 
		and sl.source_id = {0} 
		and sl.batch_id = {1}
	JOIN listing_status s
		ON l.source_id=s.source_id
		AND l.listing_status_id=s.id
	Where s.display_flag is false and l.source_id={0} and l.source_status='ACTIVE'
	and l.id not in (select target_listing_id from stage.etl_direct_idx_update_listings where source_id={0})	
	
    UNION 
	
    SELECT l.listing_id,l.source_listing_id, l.source_id, sl.batch_id, s.status as listing_status , sl.source_last_update_date
	,case when upper(s.ylopo_status)='SOLD' then 'SOLD' else 'INACTIVE' end as source_status,sl.sold_price,sl.sold_date,3 as rank
	from stage.listing_lookup l
    join stage.direct_idx_listing sl
		on l.source_listing_id = sl.source_listing_id 
		and sl.source_id = {0} 
		and sl.batch_id = {1}
	JOIN listing_status s
		ON l.source_id=s.source_id
		AND l.listing_status_id=s.id
	Where s.display_flag is false and l.source_id={0} and l.source_status='INACTIVE' and upper(s.ylopo_status)='SOLD'
	and l.id not in (select target_listing_id from stage.etl_direct_idx_update_listings where source_id={0})
	
    UNION 
	
    SELECT l.listing_id,l.source_listing_id, l.source_id, sl.batch_id, s.status as listing_status , sl.source_last_update_date
	,case when upper(s.ylopo_status)='SOLD' then 'SOLD' else 'INACTIVE' end as source_status
	,sl.sold_price , sl.sold_date , 4 as rank
	from stage.listing_lookup l
    join stage.direct_idx_listing sl
		on l.source_listing_id = sl.source_listing_id 
		and sl.source_id = {0} 
		and sl.batch_id = {1}
	JOIN listing_status s
		ON l.source_id=s.source_id
		AND l.listing_status_id=s.id
	Where s.display_flag is false and l.source_id={0} and l.source_status='SOLD' and upper(s.ylopo_status)='SOLD' and l.inactive_date is null	
	)temp 
	order by temp.listing_id,temp.rank""".format(source_id, batch_id)
    rds_cursor.execute(delete_query)
    delete_df = rds_cursor.fetchall()

    if not delete_df:
        log_msg = {"Listing added in elt_delete table": 0}
        logger.info(log_msg)
        return 0
    else:
        rds_cursor.execute(delete_query)
        results = rds_cursor.fetchall()

        column_names = [desc[0] for desc in rds_cursor.description]
        df = pd.DataFrame(results, columns=column_names)
        cols = ",".join(list(df.columns))
        insert_query = """
        INSERT INTO stage.etl_direct_idx_delete_listings  ({}) VALUES %s
        """.format(cols)
        extras.execute_values(avalon_cursor, insert_query, results)

        return len(df)


# Function to clean values in the dataframe
def clean_value(value):
    if pd.isna(value) or str(value).lower() in ["none", "nan", "na", "NaT", ""]:
        return None
    else:
        return value


def temp_listhub2_insert(
    batch_id, source_id, rds_cursor, rds_connection, avalon_cursor, avalon_connection
):
    # Define the SQL query for insertion
    insert_query = """
    select target_listing_id from stage.etl_direct_idx_update_listings where source_id = 905
    """
    # Execute the insertion query
    rds_cursor.execute(insert_query)
    p_temp_listings = rds_cursor.fetchall()
    p_temp_listings = [l[0] for l in p_temp_listings]
    p_temp_listings = ", ".join(map(str, p_temp_listings))

    query = """
    select * from listing where source_id = 905 and id in ({0}) """.format(
        p_temp_listings
    )
    avalon_cursor.execute(query)
    results = avalon_cursor.fetchall()
    column_names = [desc[0] for desc in avalon_cursor.description]
    df = pd.DataFrame(results, columns=column_names)
    df.rename(columns={"batch_id": "old_batch_id"}, inplace=True)
    df.rename(columns={"id": "listing_id"}, inplace=True)
    df["new_batch_id"] = batch_id
    # List of columns to exclude from conversion
    exclude_columns = ["source_id", "new_batch_id", "old_batch_id", "listingid"]
    # Identify all columns except the ones in the exclude list
    columns_to_convert = [col for col in df.columns if col not in exclude_columns]
    # Convert the identified columns to text
    df[columns_to_convert] = df[columns_to_convert].astype(str)
    df.fillna(pd.NaT)
    df.fillna("")
    df = df.apply(lambda col: col.map(clean_value))
    data_values = [tuple(row) for row in df.values]
    cols = ",".join(list(df.columns))
    insert_query = """
    INSERT INTO stage.temp_Listhub2_listings_update ({}) VALUES %s
    """.format(cols)
    extras.execute_values(rds_cursor, insert_query, data_values)

    rds_connection.commit()


def mlsgrid_photos_temp(
    batch_id,
    source_id,
    rds_cursor,
    rds_connection,
    cursor_homelisting,
    homelisting_connection,
):
    """
    ======================================================================================================================================

    Description:

        This function is responsible for processing MLS Grid listing photos for updated listings.
        It identifies photo inserts, updates, and deletes by comparing source media records with
        existing listing photos and prepares photo ETL actions for downstream ingestion.

    STEPS:

        1. Fetch listings from stage.etl_direct_idx_update_listings for the provided source_id and batch_id.
        2. Load listing IDs into a temporary table to optimize photo lookup operations.
        3. Fetch existing listing photos from the target(homelisting) for photo level comparison.
        4. Store existing listing photos into a temporary staging table for comparison processing.
        5. Create indexes on temporary photo tables to improve lookup performance.
        6. Compare source photos with existing photos to identify:
            - New photos that need to be inserted.
            - Existing photos with updated media URLs or timestamps that require update.
            - Photos removed from source that need to be deleted.
        7. Generate photo ETL actions with appropriate action flags (I, D/I, D).
        8. Insert generated photo actions into stage.etl_action_mlsgrid_photos table.
        9. Commit database transactions after successful processing.

    Example Use:

        mlsgrid_photos_temp(
            batch_id,
            source_id,
            rds_cursor,
            rds_connection,
            avalon_cursor,
            avalon_connection
        )

    Development History:

    Date (MM-DD-YYYY)        Author                  Change Description
    -----------------------------------------------------------------------------------------------------------------------------------------
    07-09-2026               Ammar Azkar             Initial version - Optimized MLS Grid photo processing with bulk operations and optimized comparisons.
    ======================================================================================================================================
    """
    try:

        # ----------------------------------------------------------------------
        # Step 1: Create Temp table required in this function
        # ----------------------------------------------------------------------

        # this holds the listings that have came for updation
        cursor_homelisting.execute("""
            DROP TABLE IF EXISTS etl_direct_idx_update_listings_temp ;
            CREATE TEMP TABLE etl_direct_idx_update_listings_temp (
                listing_id bigint,
                source_id int
            )
            """)
        # cursor_homelisting.execute(create_temp_table )
        homelisting_connection.commit()

        # this temp table holds the previously ingested photos from target.
        rds_cursor.execute("""
            DROP TABLE IF EXISTS etl_mlsgrid_photos_temp ;
            CREATE TEMP TABLE etl_mlsgrid_photos_temp (
                source_id int,
                batch_id  bigint,
                listing_id bigint,
                mls_number text,
                source_listing_id text,
                media_modification_timestamp TIMESTAMP,
                media_url text,
                photo_order int,
                photo_id bigint
            )
            """)
        # cursor_homelisting.execute(create_temp_table )
        rds_connection.commit()

        # ----------------------------------------------------------------------
        # Step 2: Fetch update bucket's listings from RDS and load into Homelisting.
        # ----------------------------------------------------------------------

        # declaring variables
        batch_size = 1000
        inserted_count = 0

        # query to fetch listigns from update bucket
        update_listings = """SELECT target_listing_id, source_id FROM stage.etl_direct_idx_update_listings WHERE source_id = {0} AND batch_id = {1}""".format(
            source_id, batch_id
        )
        rds_cursor.execute(update_listings)

        insert_listings_Ids = """
                                INSERT INTO etl_direct_idx_update_listings_temp (listing_id, source_id )
                                VALUES %s
                                """
        while True:  # looping here to insert in chunks to enhace performance

            rows = rds_cursor.fetchmany(batch_size)

            if not rows:
                break

            execute_values(
                cursor_homelisting, insert_listings_Ids, rows, page_size=batch_size
            )

            homelisting_connection.commit()

            inserted_count += len(rows)

        if inserted_count == 0:
            logging.info(
                {
                    "Status": "No listings found for media processing",
                    "source_id": source_id,
                    "batch_id": batch_id,
                }
            )

            return
        # ----------------------------------------------------------------------
        # Step 3: Fetch existing listing photos from homelistings and hold in RDS
        # ----------------------------------------------------------------------

        # fetching existing pre-ingested photos against listings from update bucket
        listing_photo_query = """SELECT l.source_id,
                                        p.batch_id,
                                        p.listing_id,
                                        l.mls_number,
                                        l.source_listing_id,
                                        p.media_modification_timestamp,
                                        p.media_url,
                                        ROW_NUMBER() OVER (PARTITION BY p.listing_id ORDER BY p.id) AS photo_order,
                                        p.id AS photo_id
                                FROM etl_direct_idx_update_listings_temp ul
                                JOIN listing_photo p ON p.listing_id = ul.listing_id 
                                JOIN listing l ON l.id = p.listing_id and l.source_id = {0}
                                WHERE l.source_id = {0};""".format(source_id)
        cursor_homelisting.execute(listing_photo_query)

        insert_query = """
                                INSERT INTO etl_mlsgrid_photos_temp (source_id,batch_id,listing_id,mls_number,source_listing_id,media_modification_timestamp,media_url,photo_order,photo_id)
                                VALUES %s
                                """
        photo_count = 0

        while True:  # looping here to insert in chunks to enhace performance

            rows = cursor_homelisting.fetchmany(batch_size)

            if not rows:
                break

            execute_values(rds_cursor, insert_query, rows, page_size=batch_size)
            rds_connection.commit()

            photo_count += len(rows)

        logger.info(
            {
                "Status": "Inserted existing listing photos into temporary table",
                "source_id": source_id,
                "batch_id": batch_id,
                "photo_count": photo_count,
            }
        )
        # ----------------------------------------------------------------------
        # Step 4: Create indexes for performance
        # ----------------------------------------------------------------------
        rds_cursor.execute("""
            CREATE INDEX idx_source_listing_id_media_url
            ON etl_mlsgrid_photos_temp (source_listing_id, media_url);
            """)

        # ----------------------------------------------------------------------
        # Step 5: Generate media insert/update/delete actions
        # ----------------------------------------------------------------------
        listings_for_all_media_update = """
                                        SELECT DISTINCT m.listing_id
                                        FROM etl_mlsgrid_photos_temp m
                                        JOIN stage.direct_idx_photo s ON s.source_listing_id = m.source_listing_id AND s.photo_order= m.photo_order and s.source_id = {0}
                                        WHERE m.source_id = {0} AND s.media_url != m.media_url;
                                        """.format(source_id)
        rds_cursor.execute(listings_for_all_media_update)
        results = rds_cursor.fetchall()
        listing_ids = [row[0] for row in results]

        if not listing_ids:
            listing_ids = "0"  # Assigning dummy value if empty
        else:
            listing_ids = ", ".join(map(str, listing_ids))

        etl_action_mlsgrid_photos = """
        
                            DROP TABLE IF EXISTS temp_updated_photos;
                            SELECT s.id,
                                    s.source_creation_date,
                                    s.source_last_update_date,
                                    s.media_modification_timestamp,
                                    s.media_url,
                                    s.source_id,
                                    s.source_listing_id,
                                    t.target_listing_id,
                                    s.y_creation_date AS y_creation_date,
                                    s.batch_id AS batch_id,
                                    s.y_last_update_date AS y_last_update_date,
                                    s.photo_order
                            INTO TEMP TABLE temp_updated_photos
                            FROM stage.direct_idx_photo s
                            JOIN stage.etl_direct_idx_update_listings t
                            ON s.source_listing_id = t.source_listing_id AND s.source_id ={0} and t.source_id = {0};
                            
                            
                            INSERT INTO stage.etl_action_mlsgrid_photos (
                                    source_creation_date,
                                    source_last_update_date,
                                    media_modification_timestamp,
                                    media_url,
                                    source_id,
                                    source_listing_id,
                                    listing_id,
                                    y_creation_date,
                                    batch_id,
                                    y_last_update_date,
                                    photo_id,
                                    URL_flag,
                                    photo_order
                                )
                            SELECT a.source_creation_date,
                                   a.source_last_update_date,
                                   a.media_modification_timestamp,
                                   a.media_url,
                                   a.source_id,
                                   a.source_listing_id,
                                   a.listing_id,
                                   a.y_creation_date,
                                   a.batch_id,
                                   a.y_last_update_date,
                                   a.photo_id,
                                   a.URL_flag,
                                   ROW_NUMBER() OVER (PARTITION BY a.listing_id ORDER BY a.id) AS photo_order
                            FROM (
                            (SELECT s.id,
                                    s.source_creation_date,
                                    s.source_last_update_date,
                                    s.media_modification_timestamp,
                                    s.media_url,
                                    s.source_id,
                                    s.source_listing_id,
                                    s.target_listing_id AS listing_id,
                                    s.y_creation_date AS y_creation_date,
                                    s.batch_id AS batch_id,
                                    s.y_last_update_date AS y_last_update_date,
                                    m.photo_id,
                                    'I' AS URL_flag
                            FROM temp_updated_photos s
                            LEFT JOIN etl_mlsgrid_photos_temp m
                            ON s.source_listing_id = m.source_listing_id AND s.media_url = m.media_url
                            WHERE s.source_id = {0} AND m.media_url IS NULL AND s.target_listing_id NOT IN ({1}))
                            
                            UNION ALL
                            
                            (SELECT s.id,
                                    s.source_creation_date,
                                    s.source_last_update_date,
                                    s.media_modification_timestamp,
                                    s.media_url,
                                    s.source_id,
                                    s.source_listing_id,
                                    s.target_listing_id AS listing_id,
                                    s.y_creation_date AS y_creation_date,
                                    s.batch_id AS batch_id,
                                    s.y_last_update_date AS y_last_update_date,
                                    m.photo_id,
                                    'D/I' AS URL_flag
                            FROM temp_updated_photos s
                            JOIN etl_mlsgrid_photos_temp m
                            ON s.source_listing_id = m.source_listing_id AND s.media_url = m.media_url
                            WHERE s.source_id = {0} AND m.source_id = {0} AND s.media_modification_timestamp > m.media_modification_timestamp
                            AND s.target_listing_id NOT IN ({1}))
                            
                            UNION ALL
                            
                            (SELECT s.id,
                                    s.source_creation_date,
                                    s.source_last_update_date,
                                    s.media_modification_timestamp,
                                    s.media_url,
                                    s.source_id,
                                    s.source_listing_id,
                                    s.target_listing_id AS listing_id,
                                    s.y_creation_date AS y_creation_date,
                                    s.batch_id AS batch_id,
                                    s.y_last_update_date AS y_last_update_date,
                                    m.photo_id,
                                    'D/I' AS URL_flag
                            FROM temp_updated_photos s
                            LEFT JOIN etl_mlsgrid_photos_temp m
                            ON s.source_listing_id = m.source_listing_id AND s.media_url = m.media_url
                            WHERE s.source_id = {0} AND s.target_listing_id IN ({1}))
                            
                            UNION ALL
                            
                            (SELECT m.id,
                                    s.source_creation_date,
                                    s.source_last_update_date,
                                    m.media_modification_timestamp,
                                    m.media_url,
                                    m.source_id,
                                    m.source_listing_id,
                                    m.listing_id,
                                    s.y_creation_date AS y_creation_date,
                                    m.batch_id AS batch_id,
                                    s.y_last_update_date AS y_last_update_date,
                                    m.photo_id,
                                    'D' AS URL_flag
                            FROM etl_mlsgrid_photos_temp m
                            LEFT JOIN temp_updated_photos s
                            ON s.source_listing_id = m.source_listing_id AND s.media_url = m.media_url
                            WHERE m.source_id = {0} AND s.media_url IS NULL))a ORDER BY a.listing_id, photo_order;""".format(
            source_id, listing_ids
        )
        rds_cursor.execute(etl_action_mlsgrid_photos)
        # results = rds_cursor.fetchall()
        # column_names = [desc[0] for desc in rds_cursor.description]
        # df = pd.DataFrame(results, columns=column_names)
        # cols = ",".join(list(df.columns))

        # insert_query = (
        #     """INSERT INTO stage.etl_action_mlsgrid_photos ({}) VALUES %s""".format(cols)
        # )
        # extras.execute_values(rds_cursor, insert_query, results)
        rds_connection.commit()
        logger.info(
            {
                "Status": "Inserted photo ETL actions for MLS GRID successfully",
                "source_id": source_id,
                "batch_id": batch_id,
                "inserted_action_count": rds_cursor.rowcount,
            }
        )
    except Exception as e:

        # --------------------------------------------------
        # Step 6: Rollback failed transactions
        # --------------------------------------------------
        try:
            rds_connection.rollback()
        except Exception:
            pass

        try:
            homelisting_connection.rollback()
        except Exception:
            pass

        # Do not attach full event object here.
        # It causes Lambda circular reference errors.
        raise


def mlsgrid_photos_temp_archive(  # Retired on 2026-07-09 due to performance Issues
    batch_id, source_id, rds_cursor, rds_connection, avalon_cursor, avalon_connection
):
    update_listings = """SELECT target_listing_id FROM stage.etl_direct_idx_update_listings WHERE source_id = {0} AND batch_id = {1}""".format(
        source_id, batch_id
    )
    rds_cursor.execute(update_listings)
    results = rds_cursor.fetchall()
    target_listing_ids = [row[0] for row in results]
    target_listing_ids = ", ".join(map(str, target_listing_ids))
    listing_photo_query = """SELECT l.source_id,
                                    p.batch_id,
                                    p.listing_id,
                                    l.mls_number,
                                    l.source_listing_id,
                                    p.media_modification_timestamp,
                                    p.media_url,
                                    ROW_NUMBER() OVER (PARTITION BY p.listing_id ORDER BY p.id) AS photo_order,
                                    p.id AS photo_id
                            FROM listing_photo p JOIN listing l ON l.id = p.listing_id and l.source_id = {0}
                            WHERE l.source_id = {0} and p.listing_id in ({1});""".format(
        source_id, target_listing_ids
    )
    avalon_cursor.execute(listing_photo_query)
    results = avalon_cursor.fetchall()
    column_names = [desc[0] for desc in avalon_cursor.description]
    df = pd.DataFrame(results, columns=column_names)
    cols = ",".join(list(df.columns))
    insert_query = (
        """INSERT INTO stage.etl_mlsgrid_photos_temp ({}) VALUES %s""".format(cols)
    )
    extras.execute_values(rds_cursor, insert_query, results)
    rds_connection.commit()
    listings_for_all_media_update = """
                                    SELECT DISTINCT m.listing_id
                                    FROM stage.etl_mlsgrid_photos_temp m
                                    JOIN stage.etl_direct_idx_update_listings t
                                    ON m.source_listing_id = t.source_listing_id AND m.source_id = t.source_id
                                    JOIN
                                    (
                                    SELECT source_id,
                                    	   source_listing_id,
                                    	   media_url,
                                    	   ROW_NUMBER() OVER (PARTITION BY source_listing_id ORDER BY id) AS photo_order
                                    FROM stage.direct_idx_photo
                                    WHERE source_id = {0}
                                    )s
                                    ON s.source_listing_id = m.source_listing_id AND s.photo_order= m.photo_order
                                    WHERE m.source_id = {0} AND s.media_url != m.media_url;
                                    """.format(source_id)
    rds_cursor.execute(listings_for_all_media_update)
    results = rds_cursor.fetchall()
    listing_ids = [row[0] for row in results]

    if not listing_ids:
        listing_ids = "0"  # Assigning dummy value if empty
    else:
        listing_ids = ", ".join(map(str, listing_ids))

    etl_action_mlsgrid_photos = """
						SELECT a.source_creation_date,
							   a.source_last_update_date,
							   a.media_modification_timestamp,
							   a.media_url,
							   a.source_id,
							   a.source_listing_id,
							   a.listing_id,
							   a.y_creation_date,
							   a.batch_id,
							   a.y_last_update_date,
							   a.photo_id,
							   a.URL_flag,
							   ROW_NUMBER() OVER (PARTITION BY a.listing_id ORDER BY a.id) AS photo_order
						FROM (
						(SELECT s.id,
								s.source_creation_date,
								s.source_last_update_date,
								s.media_modification_timestamp,
								s.media_url,
								s.source_id,
								s.source_listing_id,
								t.target_listing_id AS listing_id,
								s.y_creation_date AS y_creation_date,
								s.batch_id AS batch_id,
								s.y_last_update_date AS y_last_update_date,
								m.photo_id,
								'I' AS URL_flag
						FROM stage.direct_idx_photo s
						JOIN stage.etl_direct_idx_update_listings t
						ON s.source_listing_id = t.source_listing_id AND s.source_id = {0} and  t.source_id = {0}
						LEFT JOIN stage.etl_mlsgrid_photos_temp m
						ON s.source_listing_id = m.source_listing_id AND s.media_url = m.media_url and m.source_id = {0}
						WHERE s.source_id = {0} AND m.media_url IS NULL AND t.target_listing_id NOT IN ({1}))
						
						UNION ALL
						
						(SELECT s.id,
								s.source_creation_date,
								s.source_last_update_date,
								s.media_modification_timestamp,
								s.media_url,
								s.source_id,
								s.source_listing_id,
								t.target_listing_id AS listing_id,
								s.y_creation_date AS y_creation_date,
								s.batch_id AS batch_id,
								s.y_last_update_date AS y_last_update_date,
								m.photo_id,
								'D/I' AS URL_flag
						FROM stage.direct_idx_photo s
						JOIN stage.etl_direct_idx_update_listings t
						ON s.source_listing_id = t.source_listing_id AND s.source_id = {0} and  t.source_id = {0}
						JOIN stage.etl_mlsgrid_photos_temp m
						ON s.source_listing_id = m.source_listing_id AND s.media_url = m.media_url and m.source_id = {0}
						WHERE s.source_id = {0} AND m.source_id = {0} AND s.media_modification_timestamp > m.media_modification_timestamp
						AND t.target_listing_id NOT IN ({1}))
						
						UNION ALL
						
						(SELECT s.id,
								s.source_creation_date,
								s.source_last_update_date,
								s.media_modification_timestamp,
								s.media_url,
								s.source_id,
								s.source_listing_id,
								t.target_listing_id AS listing_id,
								s.y_creation_date AS y_creation_date,
								s.batch_id AS batch_id,
								s.y_last_update_date AS y_last_update_date,
								m.photo_id,
								'D/I' AS URL_flag
						FROM stage.direct_idx_photo s
						JOIN stage.etl_direct_idx_update_listings t
						ON s.source_listing_id = t.source_listing_id AND s.source_id = {0} and t.source_id = {0}
						LEFT JOIN stage.etl_mlsgrid_photos_temp m
						ON s.source_listing_id = m.source_listing_id AND s.media_url = m.media_url and m.source_id = {0}
						WHERE s.source_id = {0} AND t.target_listing_id IN ({1}))
						
						UNION ALL
						
						(SELECT s.id,
								s.source_creation_date,
								s.source_last_update_date,
								s.media_modification_timestamp,
								s.media_url,
								t.source_id,
								s.source_listing_id,
								t.target_listing_id AS listing_id,
								s.y_creation_date AS y_creation_date,
								t.batch_id AS batch_id,
								s.y_last_update_date AS y_last_update_date,
								m.photo_id,
								'D' AS URL_flag
						FROM stage.etl_mlsgrid_photos_temp m
						JOIN stage.etl_direct_idx_update_listings t
						ON m.source_listing_id = t.source_listing_id AND m.source_id = {0} and  t.source_id = {0}
						LEFT JOIN stage.direct_idx_photo s
						ON s.source_listing_id = m.source_listing_id AND s.media_url = m.media_url and m.source_id = {0}
						WHERE m.source_id = {0} AND s.media_url IS NULL))a ORDER BY a.listing_id, photo_order;""".format(
        source_id, listing_ids
    )
    rds_cursor.execute(etl_action_mlsgrid_photos)
    results = rds_cursor.fetchall()
    column_names = [desc[0] for desc in rds_cursor.description]
    df = pd.DataFrame(results, columns=column_names)
    cols = ",".join(list(df.columns))

    insert_query = (
        """INSERT INTO stage.etl_action_mlsgrid_photos ({}) VALUES %s""".format(cols)
    )
    extras.execute_values(rds_cursor, insert_query, results)
    rds_connection.commit()


def etl_actions_logging(
    batch_id,
    source_id,
    source_type,
    source_name,
    rds_cursor,
    rds_connection,
    avalon_cursor,
    avalon_connection,
):

    all_actions_df = pd.DataFrame()

    etl_action_insert_update = """
        SELECT 
        l.source_id,
        l.batch_id,
        ll.listing_id::varchar as listing_id,
        l.source_listing_id::varchar as source_listing_id,
        CURRENT_TIMESTAMP as creation_time,
                                                                                
        'UPDATE' as action_type
        from stage.direct_idx_listing l 
        inner join stage.listing_lookup ll on l.source_listing_id = ll.source_listing_id and l.source_id = ll.source_id
        where l.source_id = {0} and l.batch_id = {1} 
        and (l.modification_timestamp > ll.modification_timestamp OR l.media_modification_timestamp > ll.media_modification_timestamp  OR l.modification_timestamp is null OR l.media_modification_timestamp is null) 
        
        UNION
        
        SELECT 
        l.source_id,
        l.batch_id,
        l.target_listing_id::varchar as listing_id,
        l.source_listing_id::varchar as source_listing_id,
        CURRENT_TIMESTAMP as creation_time,
                                                           
        'INSERT' as action_type
        from stage.etl_direct_idx_insert_listings l 
        where l.source_id = {0} and l.batch_id = {1}""".format(source_id, batch_id)

    rds_cursor.execute(etl_action_insert_update)
    results = rds_cursor.fetchall()

    if results:
        column_names = [desc[0] for desc in rds_cursor.description]
        df = pd.DataFrame(results, columns=column_names)
        cols = ",".join(list(df.columns))

        insert_query = (
            """INSERT INTO idx_listing_etl_action_pool ({}) VALUES %s""".format(cols)
        )
        extras.execute_values(avalon_cursor, insert_query, results)
        rds_connection.commit()
        avalon_connection.commit()

        log_msg = {
            "Status": f"Inserted {len(results)} Insert/Update actions into idx_listing_etl_action_pool"
        }
        logger.info(log_msg)

        all_actions_df = pd.concat([all_actions_df, df], ignore_index=True)

    etl_action_delete = """                        
        SELECT 
        l.source_id,
        l.batch_id,
        l.target_listing_id::varchar as listing_id,
        l.source_listing_id::varchar as source_listing_id,
        CURRENT_TIMESTAMP as creation_time,
                                                           
        'DELETE' as action_type
        from stage.etl_direct_idx_delete_listings l 
        where l.source_id = {0} and l.batch_id = {1}""".format(source_id, batch_id)

    avalon_cursor.execute(etl_action_delete)
    results = avalon_cursor.fetchall()

    if results:
        column_names = [desc[0] for desc in avalon_cursor.description]
        df = pd.DataFrame(results, columns=column_names)
        cols = ",".join(list(df.columns))

        insert_query = (
            """INSERT INTO idx_listing_etl_action_pool ({}) VALUES %s""".format(cols)
        )
        extras.execute_values(avalon_cursor, insert_query, results)
        rds_connection.commit()
        avalon_connection.commit()

        log_msg = {
            "Status": f"Inserted {len(results)} Delete actions into idx_listing_etl_action_pool"
        }
        logger.info(log_msg)

        all_actions_df = pd.concat([all_actions_df, df], ignore_index=True)

    # S3 upload with ALL action types combined
    if not all_actions_df.empty:
        action_summary = all_actions_df["action_type"].value_counts().to_dict()
        log_msg = {
            "Status": "Uploading combined etl_action_pool to S3",
            "total_rows": len(all_actions_df),
            "action_breakdown": action_summary,
        }
        logger.info(log_msg)

        Upload_data_into_S3_DataLake(
            df_upload=all_actions_df,
            source_id=source_id,
            source_type=source_type,
            source_name=source_name,
            batch_id=batch_id,
            class_Name="etl_action_pool",
        )
    else:
        log_msg = {"Status": "No actions to upload to S3 for etl_action_pool"}
        logger.info(log_msg)


def add_media_offset_homelisting(
    source_id, batch_id, rds_cursor, rds_connection, avalon_cursor, avalon_connection
):
    try:

        # --------------------------------------------------
        # Step 1: Update stage.direct_idx_listing for missing media
        # --------------------------------------------------
        update_stage_listing = """
            WITH media_listings AS (
                SELECT DISTINCT
                    source_listing_id,
                    source_id
                FROM stage.direct_idx_photo lp
                WHERE source_id = {0}
                  AND batch_id = {1}
            )

            SELECT l.source_listing_id
            INTO TEMP TABLE missing_media_temp
            FROM stage.direct_idx_listing l
            LEFT JOIN media_listings ml
                   ON ml.source_listing_id = l.source_listing_id
                  AND ml.source_id = l.source_id
                  AND ml.source_id = {0} AND l.source_id = {0}
            WHERE l.source_id = {0}
              AND l.batch_id = {1}
              AND ml.source_listing_id IS NULL;

            UPDATE stage.direct_idx_listing
            SET media_modification_timestamp =
                media_modification_timestamp - interval '5 minutes'
            WHERE source_id = {0}
              AND source_listing_id IN (
                    SELECT source_listing_id
                    FROM missing_media_temp
              );
        """.format(source_id, batch_id)

        rds_cursor.execute(update_stage_listing)
        rds_connection.commit()

        logger.info(
            f"Stage listing media offset updated successfully for source_id={source_id}"
        )

    except Exception as e:
        logger.error(
            f"ETL process to add media offset failed for source_id={source_id}: {str(e)}",
            exc_info=True,
        )

        # Rollback in case of failure
        try:
            avalon_connection.rollback()
            logger.info("Transaction rolled back successfully")
        except Exception as rollback_error:
            logger.error(f"Rollback failed: {rollback_error}")

        raise


# Lambda function handler
def lambda_handler(event, context):
    """
     This  function perform insert and updates in respective ETL Tables
     Takes the following example event and returns the same  event
    {  "source_id": 796, "mls_board": "GJARA2",  "source_type": "Trestle",  "batch_creation_date": "2024-01-02 13:21:38.536000",
       "batch_id": 7051414,  "last_refresh_date": "2024-01-01T23:39:12.000000Z",  "status": true,  "run_host": "Serverless-Trestle","success": false
     }
    """
    global rds_connection
    global avalon_connection

    logger.info(event)

    try:
        sqlExecLimit = context.get_remaining_time_in_millis()

        # Fetching database secrets from AWS Secrets Manager
        rds_secret_name = os.environ.get("rdsDatabase")
        rds_secrets = fetch_secrets(rds_secret_name)
        rds_connection = get_connection(rds_secrets, rds_connection, sqlExecLimit)

        avalon_secret_name = os.environ.get("listingDatabase")
        avalon_secrets = fetch_secrets(avalon_secret_name)
        avalon_connection = get_connection(avalon_secrets, avalon_connection, sqlExecLimit)

        if rds_connection and avalon_connection:
            log_msg = {"Status": "Connection Successfull With RDS and Listing DB"}
            logger.info(log_msg)

            rds_cursor = rds_connection.cursor()
            avalon_cursor = avalon_connection.cursor()
            source_id = event["source_id"]
            source_type = event["source_type"]
            batch_id = event["batch_id"]
            source_name = event["source_name"]
            last_refresh_date = event["last_refresh_date"]

            query = f""" update public.listing l
                    set modification_timestamp = '1990-01-01'::timestamp,
                    media_modification_timestamp = '1990-01-01'::timestamp
                    where source_id = {source_id} and batch_id = {batch_id};"""

            avalon_cursor.execute(query)
            avalon_connection.commit()
            log_msg = {
                "source_id": source_id,
                "source_name": source_name,
                "batch_id": batch_id,
                "updated_count": avalon_cursor.rowcount,
            }

            logger.info(log_msg)

            generic_listings = get_source_listings_from_generic_idx_listing(
                source_id, rds_cursor
            )
            public_listing_df = get_required_columns_from_public_listings(
                source_id, generic_listings, avalon_cursor
            )
            delete_from_listing_lookup(source_id, rds_cursor, rds_connection)
            load_into_listing_lookup(public_listing_df, rds_cursor, rds_connection)
            delete_etl_insert_and_update(
                source_id, rds_cursor, rds_connection, avalon_cursor, avalon_connection
            )
            insert_count = etl_insert(
                generic_listings,
                public_listing_df,
                source_id,
                rds_cursor,
                rds_connection,
            )

            update_count = etl_update(
                generic_listings,
                public_listing_df,
                source_id,
                rds_cursor,
                rds_connection,
            )

            # if source_id == 905 and update_count > 0:
            #     temp_listhub2_insert(
            #         batch_id,
            #         source_id,
            #         rds_cursor,
            #         rds_connection,
            #         avalon_cursor,
            #         avalon_connection,
            #     )
            delete_count = etl_delete(
                public_listing_df,
                source_id,
                batch_id,
                rds_cursor,
                rds_connection,
                avalon_cursor,
                avalon_connection,
            )
            del_etl_status = (
                """ delete from public.etl_status where batch_id = {0};""".format(
                    batch_id
                )
            )
            avalon_cursor.execute(del_etl_status)
            avalon_connection.commit()

            # Build and execute a SQL query to insert data into etl_status table
            insert_query = f"""
            INSERT INTO public.etl_status 
            (batch_id, insert_count, update_count, delete_count, source_id, last_refresh_date)
            VALUES 
            ({batch_id},{insert_count},{update_count},{delete_count}, {source_id}, '{last_refresh_date}')
            """
            avalon_cursor.execute(insert_query)
            avalon_connection.commit()

            add_media_offset_homelisting(
                source_id,
                batch_id,
                rds_cursor,
                rds_connection,
                avalon_cursor,
                avalon_connection,
            )
            # UPDATED: Pass source_type and source_name so etl_actions_logging can upload to S3
            etl_actions_logging(
                batch_id,
                source_id,
                source_type,
                source_name,
                rds_cursor,
                rds_connection,
                avalon_cursor,
                avalon_connection,
            )

            # if source_type in ["MLS Grid V2 API"] and update_count > 0:
            #     # if source_id != 705:
            #     mlsgrid_photos_temp(
            #         batch_id,
            #         source_id,
            #         rds_cursor,
            #         rds_connection,
            #         avalon_cursor,
            #         avalon_connection,
            #     )
        return event

    except Exception as e:

        log_msg = {"Error": str(e), "Error At line": traceback.format_exc()}
        event["status"] = False
        event["error"] = log_msg
        return event

    finally:
        # Close database connections and cursors in the finally block
        if avalon_cursor:
            avalon_cursor.close()
        if rds_cursor:
            rds_cursor.close()
        if avalon_connection:
            avalon_connection.close()
        if rds_connection:
            rds_connection.close()
