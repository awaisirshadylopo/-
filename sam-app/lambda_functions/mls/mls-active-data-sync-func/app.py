import json
import os
from urllib.parse import unquote
import logging
import psycopg2
from psycopg2 import extras
import pandas as pd
import boto3
import warnings
import traceback
from botocore.exceptions import ClientError
from attributes_queries import attribute_query_dict
import numpy as np
from psycopg2.extras import execute_values
import hashlib

warnings.filterwarnings("ignore")

logger = logging.getLogger("mls-data-sync-func")
logger.setLevel("INFO")

db_connection_local = None
db_connection_stage = None


class SecretManagerHelper:
    @staticmethod
    def get_secret(secret_name, region_name):

        # Create a Secrets Manager client
        session = boto3.session.Session()
        client = session.client(service_name="secretsmanager", region_name=region_name)
        try:
            get_secret_value_response_db = client.get_secret_value(SecretId=secret_name)
            # Decrypts secret using the associated KMS key.
            secret_db = get_secret_value_response_db["SecretString"]
            secret_dict_db = json.loads(secret_db)
            return secret_dict_db
        except ClientError as e:
            raise str(e)


# def db_conn(db_secret, sqlExecLimit):
#     db_username = db_secret.get("username")
#     db_password = db_secret.get("password")
#     db_host = db_secret.get("host")
#     db_name = db_secret.get("dbname")
#     db_port = db_secret.get("port")

#     try:
#         connection = psycopg2.connect(
#             database=db_name,
#             user=db_username,
#             password=db_password,
#             host=db_host,
#             port=db_port,
#             options=f"-c statement_timeout={sqlExecLimit}",
#         )

#         log_msg = {"Status": "Connection Established Successfully"}
#         logger.info(log_msg)

#         return connection
#     except Exception as e:

#         log_msg = {
#             "Status": "Connection Failed",
#             "Error": str(e),
#             "Error At line": traceback.format_exc(),
#         }
#         raise Exception(log_msg)

def get_connection(secret, connection, sql_exec_limit):
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
        options=f"-c statement_timeout={sql_exec_limit}",
    )


def decode_url(encoded_string):
    if encoded_string is not None:
        return unquote(encoded_string)
    else:
        return None


def sync_listing(sync_ids, cursor_local, cursor_stage, db_connection_local):
    listing_query = """
     select DISTINCT ON (s.source_listing_id) 
        l.y_creation_date as y_creation_date,
        l.y_creation_date as y_last_update_date,
        s.batch_id as batch_id, 
        s.source_id as source_id, 
        s.architecture_style,
        s.bathrooms,
        s.bedrooms,
        s.disclose_address::boolean,
        s.disclose_days_on_market::boolean,
        s.disclose_map::boolean,
        s.full_bathrooms,
        s.half_bathrooms,
        s.idx_contact_info,
        s.is_new_construction::boolean,
        s.lead_routing_email,
        s.listing_title,
        s.listing_url,
        s.living_area_sq_ft,
        s.media_modification_timestamp,
        s.mls_number,
        s.modification_timestamp,
        s.num_floors,
        s.num_parking_spaces,
        s.on_market_date,
        s.one_quarter_bathrooms,
        s.original_price,
        s.partial_bathrooms,
        s.photo_count,
        s.price,
        s.price_max,
        s.price_min,
        s.price_type,
        s.price_update_date,
        s.prior_price,
        s.provider_category,
        s.provider_name,
        s.provider_url,
        s.room_count,
        s.source_creation_date,
        s.source_last_update_date,
        s.source_listing_id,
        s.three_quarter_bathrooms,
        s.year_built,
        s.idx_contact_info_office,
        s.source_mls_url,
        ls.id as listing_status_id, 
        --case when mr.expression is null then null else REGEXP_REPLACE(s.mls_number,mr.expression,'','g') end as mls_number_normalized,
        --CASE WHEN s.source_id = 996 THEN REGEXP_REPLACE(s.mls_number, '^R', 'RX-', 'g') WHEN s.source_id = 861 then REGEXP_REPLACE(REGEXP_REPLACE(s.mls_number, '^PWB', ''), '^(..)', '\\1-') WHEN mr.expression IS NULL THEN NULL   ELSE REGEXP_REPLACE(s.mls_number, mr.expression, '', 'g') END AS mls_number_normalized, archived at 22-01-2026
        CASE WHEN s.source_id = 996 THEN REGEXP_REPLACE(s.mls_number, '^R', 'RX-', 'g') WHEN s.source_id = 861 then REGEXP_REPLACE(REGEXP_REPLACE(s.mls_number, '^PWB', ''), '^(..)', '\\1-') WHEN mr.expression IS NULL THEN NULL   ELSE REGEXP_REPLACE(s.mls_number, mr.expression, coalesce(mr.expression_replace_with,''), 'g') END AS mls_number_normalized,
        lpst.id as Property_sub_type_id,
        lpt.id as property_type_id,
        lc.id as listing_category_id,
        mb.id as  mls_board_id,
        (CASE WHEN ls.ylopo_status = 'SOLD' THEN s.sold_date ELSE NULL END) as sold_date,
        (CASE WHEN ls.ylopo_status = 'SOLD' THEN s.sold_price ELSE NULL END) as sold_price,
	CASE WHEN ls.display_flag is false AND upper(ls.ylopo_status) != 'SOLD' THEN 'INACTIVE'
	WHEN upper(ls.ylopo_status) = 'SOLD' THEN 'SOLD'
	ELSE 'ACTIVE' END  AS source_status,

    NULLIF(CASE
        WHEN lot_size IS NOT NULL AND lot_size_unit IS NOT NULL THEN lot_size
        WHEN lot_size_acres >= 1 THEN lot_size_acres
        WHEN lot_size_sqft IS NULL AND lot_size_acres IS NOT NULL AND lot_size_acres < 1 AND lot_size_acres > 0 THEN lot_size_acres
        WHEN lot_size_sqft IS NOT NULL AND lot_size_acres < 1 THEN lot_size_sqft
		 WHEN lot_size_sqft > 0 AND lot_size_acres is null THEN lot_size_sqft
        ELSE NULL
    END, 0.0) AS lot_size,
    
    CASE
        WHEN lot_size IS NOT NULL AND lot_size_unit IS NOT NULL THEN lot_size_unit
        WHEN lot_size_acres >= 1 THEN 'acres'
        WHEN lot_size_sqft IS NULL AND lot_size_acres IS NOT NULL AND lot_size_acres < 1 AND lot_size_acres > 0 THEN 'acres'
        WHEN lot_size_sqft IS NOT NULL AND lot_size_acres < 1 THEN 'sqft'
		WHEN lot_size_sqft > 0 AND lot_size_acres is null THEN 'sqft'
        ELSE NULL
    END AS lot_size_units,
	
	case when 
    (NULLIF(CASE
        WHEN lot_size_sqft IS NOT NULL THEN NULLIF(NULLIF(round(lot_size_sqft::numeric,2),'0.00'),'0')
        WHEN (lot_size_sqft IS NULL AND lot_size_acres IS NOT NULL AND lot_size_acres != 0) THEN NULLIF(NULLIF(round(lot_size_acres::numeric * 43560,2),'0.00'),'0')
        WHEN (lot_size IS NOT NULL AND lot_size != 0) THEN
            CASE
                WHEN lot_size_unit = 'sqft' THEN NULLIF(NULLIF(round(lot_size::numeric,2),'0.00'),'0')
				WHEN lot_size_unit = 'acres' THEN NULLIF(NULLIF(round(lot_size::numeric * 43560,2),'0.00'),'0')
                ELSE NULL
            END
        ELSE NULL
    END, 0.0)) > 9999999999999.99 then null ELSE
	NULLIF(CASE
        WHEN lot_size_sqft IS NOT NULL THEN NULLIF(NULLIF(round(lot_size_sqft::numeric,2),'0.00'),'0')
        WHEN (lot_size_sqft IS NULL AND lot_size_acres IS NOT NULL AND lot_size_acres != 0) THEN NULLIF(NULLIF(round(lot_size_acres::numeric * 43560,2),'0.00'),'0')
        WHEN (lot_size IS NOT NULL AND lot_size != 0) THEN
            CASE
                WHEN lot_size_unit = 'sqft' THEN NULLIF(NULLIF(round(lot_size::numeric,2),'0.00'),'0')
				WHEN lot_size_unit = 'acres' THEN NULLIF(NULLIF(round(lot_size::numeric * 43560,2),'0.00'),'0')
                ELSE NULL
            END
        ELSE NULL
    END, 0.0)

   end AS lot_size_sqft,
    
    NULLIF(CASE
        WHEN lot_size_acres IS NOT NULL THEN lot_size_acres
        WHEN (lot_size_acres IS NULL AND lot_size_sqft IS NOT NULL AND lot_size_sqft != 0) THEN round(lot_size_sqft::numeric / 43560,2)
        WHEN (lot_size IS NOT NULL AND lot_size != 0) THEN
            CASE
                WHEN lot_size_unit = 'acres' THEN lot_size
				WHEN lot_size_unit = 'sqft' THEN round(lot_size::numeric / 43560,2)
                ELSE NULL
            END
        ELSE NULL
    END, 0.0) AS lot_size_acres
    ,cumulative_days_on_market
    ,originating_system_modification_timestamp
    

    from stage.DIRECT_idx_listing s
    join listing_status ls
    on s.listing_status=ls.status
    and s.source_id=ls.source_id
    inner join 
        stage.etl_direct_idx_insert_listings l
        on s.source_listing_id = l.source_listing_id
        and l.source_id = s.source_id


    inner JOIN
            public.listing_property_sub_type lpst ON
            s.source_id = lpst.source_id
            AND coalesce(s.property_sub_type, '') = coalesce(lpst.property_sub_type,'')
    inner JOIN
            public.listing_property_type lpt ON
            s.source_id = lpt.source_id
            AND coalesce(s.property_type, '') = coalesce(lpt.property_type,'')

    inner JOIN
            public.listing_category lc ON
            s.source_id = lc.source_id
            and (s.listing_category = lc.category or (s.listing_category is null and lc.category is null))

    inner JOIN
            public.mls_board mb ON
            s.source_id = mb.source_id
            AND s.mls = mb.mls_source_id
    left join 
        idx_config.listing_mls_number_regex mr
        on s.source_id = mr.source_Id

    where (ls.load_flag is true or ls.display_flag is true)
    and s.source_id in {}

    order by s.source_listing_id

    """.format(str(sync_ids).replace("[", "(").replace("]", ")"))
    cursor_local.execute(listing_query)
    results = cursor_local.fetchall()

    column_names = [desc[0] for desc in cursor_local.description]
    df = pd.DataFrame(results, columns=column_names)
    cols = ",".join(list(df.columns))
    insert_query = """
    INSERT INTO public.listing ({}) VALUES %s
    """.format(cols)
    extras.execute_values(cursor_stage, insert_query, results)
    update_etl_direct_idx_insert_listings(
        sync_ids, cursor_local, cursor_stage, db_connection_local
    )


def update_etl_direct_idx_insert_listings_count(
    sync_ids,
    batch_ids,
    cursor_local,
    cursor_stage,
    db_connection_local,
    db_connection_stage,
):

    insert_count_select = """
        SELECT count(*) from stage.etl_direct_idx_insert_listings where source_id in {} and target_listing_id is not null """.format(
        str(sync_ids).replace("[", "(").replace("]", ")")
    )
    cursor_local.execute(insert_count_select)
    insert_count = cursor_local.fetchone()

    insert_count_update = """
        UPDATE public.etl_status SET insert_count = {0} WHERE batch_id = {1} """.format(
        insert_count[0], str(batch_ids).replace("[", "(").replace("]", ")")
    )
    cursor_stage.execute(insert_count_update)
    db_connection_stage.commit()


def update_etl_direct_idx_insert_listings(
    sync_ids, cursor_local, cursor_stage, db_connection_local
):
    select_source_listing_ids = """
        SELECT source_listing_id  from stage.etl_direct_idx_insert_listings where source_id in {} """.format(
        str(sync_ids).replace("[", "(").replace("]", ")")
    )
    cursor_local.execute(select_source_listing_ids)
    source_listing_ids = cursor_local.fetchall()
    list_source_listing_ids = [result[0] for result in source_listing_ids]
    if len(list_source_listing_ids) != 0:
        query = """SELECT source_listing_id, source_id, id AS target_listing_id,batch_id FROM public.listing WHERE source_id in {} and source_listing_id IN {}""".format(
            str(sync_ids).replace("[", "(").replace("]", ")"),
            str(list_source_listing_ids).replace("[", "(").replace("]", ")"),
        )
        cursor_stage.execute(query)
        results2 = cursor_stage.fetchall()
        column_names = [desc[0] for desc in cursor_stage.description]
        df = pd.DataFrame(results2, columns=column_names)

        for index, row in df.iterrows():
            update_query = """
                UPDATE stage.etl_direct_idx_insert_listings
                    SET 
                        target_listing_id = {target_listing_id}
                 WHERE source_listing_id = '{source_listing_id}' and source_id = {source_id}
        """.format(
                source_id=row["source_id"],
                target_listing_id=row["target_listing_id"],
                source_listing_id=row["source_listing_id"],
            )

            cursor_local.execute(update_query)

            update_etl_Action_query = """
                UPDATE idx_listing_etl_action_pool
                    SET 
                        listing_id = {target_listing_id}
                 WHERE source_listing_id = '{source_listing_id}' and source_id = {source_id} and batch_id  = {batch_id}
        """.format(
                source_id=row["source_id"],
                target_listing_id=row["target_listing_id"],
                source_listing_id=row["source_listing_id"],
                batch_id=row["batch_id"],
            )

            cursor_stage.execute(update_etl_Action_query)
        else:
            log_msg = {"Message": "NO listing for insertion"}
            logger.info(log_msg)

    db_connection_local.commit()


def sync_description(sync_ids, cursor_local, cursor_stage, db_connection_local):
    description = """select
    s.key_name,
    s.key_value,
    s.source_creation_date,
    s.source_last_update_date,
    s.batch_id as batch_id,
    t.y_creation_date as y_creation_date,
    t.y_creation_date as y_last_update_date,
    t.target_listing_id as listing_id   

    from stage.DIRECT_idx_description s 
    join stage.etl_direct_idx_insert_listings t

    on s.source_listing_id=t.source_listing_id
    and s.batch_id = t.batch_id
    where 
    s.key_value is not NULL and t.target_listing_id is not null and

    s.source_id in {} """.format(str(sync_ids).replace("[", "(").replace("]", ")"))

    cursor_local.execute(description)
    result = cursor_local.fetchall()
    column_names = [desc[0] for desc in cursor_local.description]
    df = pd.DataFrame(result, columns=column_names)
    df["key_value"] = df["key_value"].apply(decode_url)
    cols = ",".join(list(df.columns))
    insert_query = """
        INSERT INTO public.listing_description ({}) VALUES %s
        """.format(cols)
    extras.execute_values(cursor_stage, insert_query, result)


def price_history_initial(
    listing_ids, cursor_local, cursor_stage, db_connection_local, db_connection_stage
):
    if listing_ids and len(listing_ids) > 0:

        initial_price = """SELECT
        l.id AS listing_id,
        l.prior_price AS price,
        l.batch_id,
        l.price_update_date AS y_creation_date
        FROM
            listing l
        LEFT JOIN
            listing_price_history h ON l.id = h.listing_id
        WHERE
            h.listing_id IS NULL and 
            l.id in {}""".format(str(listing_ids).replace("[", "(").replace("]", ")"))
        cursor_stage.execute(initial_price)
        result = cursor_stage.fetchall()
        column_names = [desc[0] for desc in cursor_stage.description]
        df = pd.DataFrame(result, columns=column_names)
        cols = ",".join(list(df.columns))
        insert_query = """
             INSERT INTO public.listing_price_history ({}) VALUES %s
             """.format(cols)
        extras.execute_values(cursor_stage, insert_query, result)
    else:
        pass


def load_price_history(
    listing_ids, cursor_local, cursor_stage, db_connection_local, db_connection_stage
):
    if listing_ids and len(listing_ids) > 0:
        price_history = """select 
        l.id as listing_id,
        l.price,
        l.price_update_date as y_creation_date,
        l.batch_id 
        from listing l 
        where  l.id in {}""".format(
            str(listing_ids).replace("[", "(").replace("]", ")")
        )
        cursor_stage.execute(price_history)
        result = cursor_stage.fetchall()
        column_names = [desc[0] for desc in cursor_stage.description]
        df = pd.DataFrame(result, columns=column_names)
        cols = ",".join(list(df.columns))
        insert_query = """
                INSERT INTO public.listing_price_history ({}) VALUES %s
                """.format(cols)
        extras.execute_values(cursor_stage, insert_query, result)
    else:
        pass


def sync_photos(sync_ids, cursor_local, cursor_stage, db_connection_local):
    photos_query = """  
    SELECT
        s.source_creation_date,
        s.source_last_update_date,
        s.media_modification_timestamp,
        s.media_url,
        t.target_listing_id AS listing_id,
        t.y_creation_date AS y_creation_date,
        s.batch_id AS batch_id,
        t.y_creation_date AS y_last_update_date
    FROM
        stage.DIRECT_idx_photo s
    JOIN
        stage.etl_direct_idx_insert_listings t
    ON
        s.source_listing_id = t.source_listing_id and
        s.source_id=t.source_id
    WHERE
        s.source_id in {} and t.target_listing_id is not null
    ORDER BY
        s.source_listing_id,s.id;
""".format(str(sync_ids).replace("[", "(").replace("]", ")"))
    cursor_local.execute(photos_query)
    result = cursor_local.fetchall()
    column_names = [desc[0] for desc in cursor_local.description]
    df = pd.DataFrame(result, columns=column_names)
    cols = ",".join(list(df.columns))
    insert_query = """
                INSERT INTO public.listing_photo ({}) VALUES %s
                """.format(cols)
    extras.execute_values(cursor_stage, insert_query, result)


def sync_listing_participant_rel(
    sync_ids, cursor_local, cursor_stage, db_connection_local
):
    agents_query = """
    select  distinct on (t.target_listing_id,s.source_participant_id,rank)
    t.target_listing_id as listing_id, 
    s.source_creation_date, 
    s.source_last_update_date, 
    s.source_participant_id as participant_id, 
    s.source_office_id as source_real_estate_office_id, rank
    ,t.y_creation_date as y_creation_date
    ,t.y_creation_date as y_last_update_date,
    t.batch_id as batch_id 
    from stage.DIRECT_idx_agent s
    join stage.etl_direct_idx_insert_listings t
    on t.source_listing_id=s.source_listing_id 
    and t.source_id = s.source_id
    and t.batch_id = s.batch_id
    where s.source_id in {} and s.source_participant_id is not null and t.target_listing_id is not null
""".format(str(sync_ids).replace("[", "(").replace("]", ")"))
    cursor_local.execute(agents_query)
    result = cursor_local.fetchall()
    column_names = [desc[0] for desc in cursor_local.description]
    df = pd.DataFrame(result, columns=column_names)
    cols = ",".join(list(df.columns))
    insert_query = """
                    INSERT INTO public.listing_participant_rel ({}) VALUES %s
                    """.format(cols)
    extras.execute_values(cursor_stage, insert_query, result)


def update_listing_participant_rel_insert_1(
    sync_ids, cursor_local, cursor_stage, db_connection_local
):
    agents_query = """
        SELECT DISTINCT 
            t.target_listing_id AS listing_id, 
            s.source_creation_date, 
            s.source_last_update_date, 
            s.source_participant_id AS participant_id, 
            s.source_office_id AS source_real_estate_office_id, 
            s.rank,
            s.y_creation_date AS y_creation_date,
            s.y_creation_date AS y_last_update_date,
            s.batch_id AS batch_id
        FROM stage.DIRECT_idx_agent s
        JOIN stage.etl_direct_idx_update_listings t
            ON t.source_listing_id = s.source_listing_id 
            and s.source_id =  t.source_id
            and s.batch_id = t.batch_id
        WHERE s.source_id IN {} 
          AND s.source_participant_id IS NOT NULL 
          AND t.target_listing_id IS NOT NULL
   """.format(str(sync_ids).replace("[", "(").replace("]", ")"))
    cursor_local.execute(agents_query)
    result = cursor_local.fetchall()
    column_names = [desc[0] for desc in cursor_local.description]
    df = pd.DataFrame(result, columns=column_names)
    cols = ",".join(list(df.columns))
    insert_query = """
                    INSERT INTO public.listing_participant_rel ({}) VALUES %s
                    """.format(cols)
    extras.execute_values(cursor_stage, insert_query, result)


def update_listing_participant_rel_insert(
    sync_ids, cursor_local, cursor_stage, db_connection_local
):
    # Step 1: Get data from stage
    offices_query = """
        SELECT DISTINCT on (t.target_listing_id,s.rank,s.source_participant_id)
            t.target_listing_id AS listing_id, 
            s.source_creation_date, 
            s.source_last_update_date, 
            s.source_participant_id AS participant_id, 
            s.source_office_id AS source_real_estate_office_id, 
            s.rank,
            s.y_creation_date AS y_creation_date,
            s.y_creation_date AS y_last_update_date,
            s.batch_id AS batch_id
        FROM stage.DIRECT_idx_agent s
        JOIN stage.etl_direct_idx_update_listings t
            ON t.source_listing_id = s.source_listing_id 
            and s.source_id =  t.source_id
            and s.batch_id = t.batch_id
        WHERE s.source_id IN {} 
          AND s.source_participant_id IS NOT NULL 
          AND t.target_listing_id IS NOT NULL
    """.format(str(sync_ids).replace("[", "(").replace("]", ")"))

    cursor_local.execute(offices_query)
    result = cursor_local.fetchall()
    column_names = [desc[0] for desc in cursor_local.description]
    df_stage = pd.DataFrame(result, columns=column_names)

    if df_stage.empty:
        return

    # Step 2: Fetch existing listing_id, rank combinations
    listing_rank_tuples = tuple(
        df_stage[["listing_id", "rank"]]
        .drop_duplicates()
        .itertuples(index=False, name=None)
    )

    # Avoid empty tuple issue in SQL
    if not listing_rank_tuples:
        return

    # Create query string for filtering existing rows
    query_existing = """
        SELECT listing_id, rank 
        FROM public.listing_participant_rel
        WHERE (listing_id, rank) IN %s
    """
    cursor_stage.execute(query_existing, (listing_rank_tuples,))
    existing_pairs = set(cursor_stage.fetchall())

    # Step 3: Filter out existing rows from df_stage
    df_stage_filtered = df_stage[
        ~df_stage.set_index(["listing_id", "rank"]).index.isin(existing_pairs)
    ]

    if df_stage_filtered.empty:
        return

    # Step 4: Insert filtered records
    insert_query = (
        """INSERT INTO public.listing_participant_rel ({}) VALUES %s""".format(
            ",".join(df_stage_filtered.columns)
        )
    )
    extras.execute_values(cursor_stage, insert_query, df_stage_filtered.values.tolist())


def sync_openhouse(sync_ids, cursor_local, cursor_stage, db_connection_local):
    open_house_2 = """
    select DISTINCT ON (s.source_listing_id, s.date, s.start_time)
    t.target_listing_id as listing_id
    ,t.y_creation_date as y_creation_date
    ,t.y_creation_date as y_last_update_date
    ,upper(coalesce(s.openhouse_type,'IN-PERSON')) as openhouse_type
    ,s.source_creation_date
    ,s.source_last_update_date
    ,s.source_date
    ,s.source_time
    ,s.contact_name
    ,s.contact_phone
    ,s.date
    ,s.start_time
    ,s.end_time
    ,s.virtual_tour_url
    ,s.ylopo_action
    ,t.batch_id as batch_id     
    from stage.DIRECT_idx_openhouse s 
    join stage.etl_direct_idx_insert_listings t   
    on s.source_listing_id=t.source_listing_id
    where s.source_date is not null and t.target_listing_id is not null
    and s.source_id in {}
    ORDER BY s.source_listing_id, s.date DESC, s.start_time DESC;
    """.format(str(sync_ids).replace("[", "(").replace("]", ")"))
    cursor_local.execute(open_house_2)
    result = cursor_local.fetchall()
    column_names = [desc[0] for desc in cursor_local.description]
    df = pd.DataFrame(result, columns=column_names)
    cols = ",".join(list(df.columns))
    insert_query = """INSERT INTO public.listing_openhouse ({}) VALUES %s
                        """.format(cols)
    extras.execute_values(cursor_stage, insert_query, result)


def sync_office_Get_Office_Rel(
    sync_ids, cursor_local, cursor_stage, db_connection_local
):
    offices_query = """
    select  distinct on (t.target_listing_id ,s.rank,s.source_office_id)
    t.target_listing_id as listing_id,  
    s.source_creation_date, 
    s.source_last_update_date, 
    s.source_office_id as office_id,
    s.rank,
    t.y_creation_date as y_creation_date,
    t.y_creation_date as y_last_update_date,
    t.batch_id as batch_id

    from stage.direct_idx_office s
    join stage.etl_direct_idx_insert_listings t
        on t.source_listing_id=s.source_listing_id
        and t.source_id = s.source_id
        and t.batch_id  = s.batch_id
    
    where 
    s.source_id in {} and t.target_listing_id is not null
    and s.source_office_id is not null """.format(
        str(sync_ids).replace("[", "(").replace("]", ")")
    )
    cursor_local.execute(offices_query)
    result = cursor_local.fetchall()
    column_names = [desc[0] for desc in cursor_local.description]
    df = pd.DataFrame(result, columns=column_names)
    cols = ",".join(list(df.columns))
    insert_query = """INSERT INTO public.listing_real_estate_office_rel ({}) VALUES %s
                            """.format(cols)
    extras.execute_values(cursor_stage, insert_query, result)


def update_office_Get_Office_Rel_insert_1(
    sync_ids, cursor_local, cursor_stage, db_connection_local
):
    offices_query = """	  
    select  distinct on (t.target_listing_id,s.rank,s.source_office_id)
    t.target_listing_id as listing_id,  
    s.source_creation_date, 
    s.source_last_update_date, 
    s.source_office_id as office_id,
    s.rank,
    s.y_creation_date as y_creation_date,
    s.y_creation_date as y_last_update_date,
    s.batch_id as batch_id

    from stage.direct_idx_office s
    join stage.etl_direct_idx_update_listings t
        on t.source_listing_id=s.source_listing_id
        and t.source_id = s.source_id
        and t.batch_id  = s.batch_id
    
    where 
    s.source_id in {} 
	and t.target_listing_id is not null
    and s.source_office_id is not null 
    """.format(str(sync_ids).replace("[", "(").replace("]", ")"))
    cursor_local.execute(offices_query)
    result = cursor_local.fetchall()
    column_names = [desc[0] for desc in cursor_local.description]
    df = pd.DataFrame(result, columns=column_names)
    cols = ",".join(list(df.columns))
    insert_query = """INSERT INTO public.listing_real_estate_office_rel ({}) VALUES %s
                            """.format(cols)
    extras.execute_values(cursor_stage, insert_query, result)


def update_office_Get_Office_Rel_insert(
    sync_ids, cursor_local, cursor_stage, db_connection_local
):
    # Step 1: Get data from stage
    offices_query = """
    SELECT DISTINCT on (t.target_listing_id,s.rank,s.source_office_id)
        t.target_listing_id AS listing_id,  
        s.source_creation_date, 
        s.source_last_update_date, 
        s.source_office_id AS office_id,
        s.rank,
        s.y_creation_date AS y_creation_date,
        s.y_creation_date AS y_last_update_date,
        s.batch_id AS batch_id
    FROM stage.direct_idx_office s
    JOIN stage.etl_direct_idx_update_listings t
        ON t.source_listing_id = s.source_listing_id
        AND t.source_id = s.source_id
        AND t.batch_id = s.batch_id
    WHERE 
        s.source_id IN {} 
        AND t.target_listing_id IS NOT NULL
        AND s.source_office_id IS NOT NULL
    """.format(str(sync_ids).replace("[", "(").replace("]", ")"))

    cursor_local.execute(offices_query)
    result = cursor_local.fetchall()
    column_names = [desc[0] for desc in cursor_local.description]
    df_stage = pd.DataFrame(result, columns=column_names)

    if df_stage.empty:
        return

    # Step 2: Fetch existing listing_id, rank combinations
    listing_rank_tuples = tuple(
        df_stage[["listing_id", "rank"]]
        .drop_duplicates()
        .itertuples(index=False, name=None)
    )

    # Avoid empty tuple issue in SQL
    if not listing_rank_tuples:
        return

    # Create query string for filtering existing rows
    query_existing = """
        SELECT listing_id, rank 
        FROM public.listing_real_estate_office_rel 
        WHERE (listing_id, rank) IN %s
    """
    cursor_stage.execute(query_existing, (listing_rank_tuples,))
    existing_pairs = set(cursor_stage.fetchall())

    # Step 3: Filter out existing rows from df_stage
    df_stage_filtered = df_stage[
        ~df_stage.set_index(["listing_id", "rank"]).index.isin(existing_pairs)
    ]

    if df_stage_filtered.empty:
        return

    # Step 4: Insert filtered records
    insert_query = (
        """INSERT INTO public.listing_real_estate_office_rel ({}) VALUES %s""".format(
            ",".join(df_stage_filtered.columns)
        )
    )
    extras.execute_values(cursor_stage, insert_query, df_stage_filtered.values.tolist())


def sync_schools(sync_ids, cursor_local, cursor_stage, db_connection_local):
    schools = """
    select s.source_id as source_id, 
     s.source_last_update_date,
     s.source_creation_date,
     s.school_category as mls_school_type,
     s.school_category as school_type,
     s.school_name as name,
     s.school_district as district,
     t.target_listing_id as listing_id, 
     s.y_creation_date as y_creation_date, 
     s.y_creation_date as y_last_update_date,
     t.batch_id as batch_id
    from stage.DIRECT_idx_school s 
    join stage.etl_direct_idx_insert_listings t

    on s.source_listing_id=t.source_listing_id
    where 
    s.source_id in  {} 
     and 
    s.school_name is not null and t.target_listing_id is not null;
        """.format(str(sync_ids).replace("[", "(").replace("]", ")"))
    cursor_local.execute(schools)
    result = cursor_local.fetchall()
    column_names = [desc[0] for desc in cursor_local.description]
    df = pd.DataFrame(result, columns=column_names)
    cols = ",".join(list(df.columns))
    insert_query = """INSERT INTO public.listing_school ({}) VALUES %s
                                """.format(cols)
    extras.execute_values(cursor_stage, insert_query, result)


def sync_listing_school_district(
    sync_ids, cursor_local, cursor_stage, db_connection_local
):
    school_district_query = """
    select DISTINCT ON (s.source_listing_id)
    	s.source_id as source_id
    	, t.target_listing_id as listing_id
    	, s.batch_id as batch_id
    	, s.school_district as district
    	, t.y_creation_date as y_creation_date
    	, t.y_creation_date as y_last_update_date
    	, s.source_creation_date as source_creation_date
    	, s.source_last_update_date

    from stage.DIRECT_idx_school s 
    join stage.etl_direct_idx_insert_listings t

    on s.source_listing_id=t.source_listing_id
    and s.batch_id = t.batch_id
    where  s.source_id in {}  and s.school_district is not null and t.target_listing_id is not null order by s.source_listing_id;
    """.format(str(sync_ids).replace("[", "(").replace("]", ")"))
    cursor_local.execute(school_district_query)
    result = cursor_local.fetchall()
    column_names = [desc[0] for desc in cursor_local.description]
    df = pd.DataFrame(result, columns=column_names)
    cols = ",".join(list(df.columns))
    insert_query = """INSERT INTO public.listing_school_district ({}) VALUES %s
                                """.format(cols)
    extras.execute_values(cursor_stage, insert_query, result)


def sync_address(sync_ids, cursor_local, cursor_stage, db_connection_stage):
    address_query = """
     select 
       t.target_listing_id as listing_id,
       s.city,
       s.county,
       s.full_street_address,
       t.y_creation_date as y_creation_date,
       t.y_creation_date as y_last_update_date,
       s.source_creation_date,                   
       s.source_last_update_date,
       s.zoning, 
       s.mls_latitude,
       s.mls_longitude,
       s.postal_code,
       s.unit_number, 
       s.state_or_province,
       s.parcel_number as parcel_id,
       s.batch_id as batch_id,
       s.country,                            
       UPPER(s.community_name) as community_name,
       s.region,
       s.zone,
       t.source_id as source_id,
       UPPER(s.subdivision_name) as subdivision_name,
       UPPER(s.district_name) as district_name,
       UPPER(s.mls_area_name) as mls_area_name,
       UPPER(s.custom_area_name_1) as custom_area_name_1,
       UPPER(s.custom_area_name_2) as custom_area_name_2,
       UPPER(s.custom_area_name_3) as custom_area_name_3,
       UPPER(s.sub_district_name) as sub_district_name,

       UPPER(TRIM(BOTH ' ' FROM CONCAT(
               REGEXP_REPLACE(REGEXP_REPLACE(s.full_street_address, '[aeiouAEIOU ]', '','g'), '[^a-zA-Z\d\s:]', '','g'),
               REGEXP_REPLACE(REGEXP_REPLACE(s.city, '[aeiouAEIOU ]', '','g'), '[^a-zA-Z\d\s:]', '','g'),
               REGEXP_REPLACE(REGEXP_REPLACE(s.postal_code, '[aeiouAEIOU ]', '','g'), '[^a-zA-Z\d\s:]', '','g')
           ))) AS address_token
        from stage.direct_idx_address s
            join stage.etl_direct_idx_insert_listings t
              on s.source_listing_id=t.source_listing_id
                   and s.source_id = t.source_id
    where s.source_id in {} and t.target_listing_id is not null
    order by t.target_listing_id
     """.format(str(sync_ids).replace("[", "(").replace("]", ")"))
    cursor_local.execute(address_query)
    result = cursor_local.fetchall()
    column_names = [desc[0] for desc in cursor_local.description]
    df = pd.DataFrame(result, columns=column_names)
    cols = ",".join(list(df.columns))
    insert_query = """INSERT INTO public.listing_address ({}) VALUES %s
                                    """.format(cols)

    try:
        extras.execute_values(cursor_stage, insert_query, result)
        db_connection_stage.commit()

    except Exception as e:

        log_msg = {
            "Status": "Exception in sync_address",
            "Error": str(e),
            "Error At line": traceback.format_exc(),
        }
        raise Exception(log_msg)

        db_connection_stage.rollback()


def insert_real_estate_office(
    sync_ids,
    batch_ids,
    cursor_local,
    cursor_stage,
    db_connection_local,
    db_connection_stage,
):
    query = """
    select  
    distinct  s.source_office_id
    , s.office_id
    , s.office_name
    , s.corporate_name
    , s.main_office_id
    , s.phone_number
    , s.fax
    , s.full_street_address
    , s.city
    , s.state_province
    , s.country
    , s.office_email
    , s.website
    , s.unit_number
    , s.office_mls_id
    , s.office_mui
    , s.batch_id
    , s.source_id 
    , s.source_creation_date
    , s.source_last_update_date
    , s.y_creation_date
    , s.y_last_update_date
    
    from stage.DIRECT_idx_office s
    
    join stage.etl_direct_idx_insert_listings t
    
    on t.source_listing_id = s.source_listing_id
    and s.batch_id = t.batch_id 
    and s.source_id = t.source_id
    
    left join public.real_estate_office o 
    
    on o.source_office_id = s.source_office_id
    and o.source_id in {0} and o.source_id = s.source_id 
    and coalesce(s.office_id, 'Dummy') = coalesce(o.office_id, 'Dummy')
    
    where o.source_office_id is null 
    and t.target_listing_id is not null
    and s.batch_id in {1}
    and s.source_id in {0}
    and  s.source_office_id is not null;
    """.format(
        str(sync_ids).replace("[", "(").replace("]", ")"),
        str(batch_ids).replace("[", "(").replace("]", ")"),
    )

    cursor_local.execute(query)
    result = cursor_local.fetchall()
    column_names = [desc[0] for desc in cursor_local.description]
    df = pd.DataFrame(result, columns=column_names)
    cols = ",".join(list(df.columns))

    insert_query = """
                    INSERT INTO public.real_estate_office ({}) VALUES %s
                    """.format(cols)
    query_execution_and_commit(insert_query, cursor_stage, db_connection_stage, result)
    query_execution_and_commit(insert_query, cursor_local, db_connection_local, result)


def insert_real_estate_participant(
    sync_ids,
    batch_ids,
    cursor_local,
    cursor_stage,
    db_connection_local,
    db_connection_stage,
):
    query = """
    select  
    distinct s.source_participant_id
    ,s.participant_id 
    ,s.first_name  
    ,s.last_name  
    ,s.full_name  
    ,s.participant_role
    ,s.primary_contact_phone
    ,s.office_phone   
    ,s.email   
    ,s.website_url
    ,s.agent_mls_id
    ,s.agent_mui
    ,s.batch_id
    ,s.source_id 
    ,s.agent_license
    ,s.source_creation_date
    ,s.y_creation_date
    ,s.y_last_update_date
    ,s.source_last_update_date
    
    from stage.DIRECT_idx_agent s
    
    join stage.etl_direct_idx_insert_listings t
    
    on t.source_listing_id=s.source_listing_id 
    and s.batch_id = t.batch_id
    and s.source_id = t.source_id
    
    left join public.real_estate_participant o 
    
    on o.source_participant_id = s.source_participant_id
    and o.source_id in {0} and s.source_id = o.source_id
    and coalesce(s.participant_id, 'Dummy') = coalesce(o.participant_id, 'Dummy')
    
    where o.source_participant_id is null 
    and t.target_listing_id is not null
    and s.batch_id in {1}
    and s.source_id in {0}
    and s.source_participant_id is not null;
    """.format(
        str(sync_ids).replace("[", "(").replace("]", ")"),
        str(batch_ids).replace("[", "(").replace("]", ")"),
    )

    cursor_local.execute(query)
    result = cursor_local.fetchall()
    column_names = [desc[0] for desc in cursor_local.description]
    df = pd.DataFrame(result, columns=column_names)
    cols = ",".join(list(df.columns))

    insert_query = """
                    INSERT INTO public.real_estate_participant ({}) VALUES %s
                    """.format(cols)
    query_execution_and_commit(insert_query, cursor_stage, db_connection_stage, result)
    query_execution_and_commit(insert_query, cursor_local, db_connection_local, result)


def area_normalize_subdivision_insert(
    sync_ids, cursor_local, cursor_stage, db_connection_local
):
    deletion = """delete from stage.area_normalize where source_id in {}""".format(
        str(sync_ids).replace("[", "(").replace("]", ")")
    )
    cursor_local.execute(deletion)
    db_connection_local.commit()

    area_subdivision = """
    select 
    a.subdivision_name as orignal_subdivision,
    regexp_replace(UPPER(a.subdivision_name), area_name_exp , am.area_name)  as mapped_subdivision,
    cast(case when (replace( regexp_replace(UPPER(a.subdivision_name), area_name_exp , '*****'),UPPER(a.subdivision_name),'!!!!!')) like '%*****%' then '9' else '0' end as int) as flag_address,

    a.source_listing_id,
    a.source_id,
    am.area_name_exp,
    am.area_name,
    am.area_type,

    a.y_creation_date,	
    a.y_last_update_date,	
    a.source_creation_date,	
    a.source_last_update_date,
    t.target_listing_id as listing_id


    from stage.direct_idx_address a
    join
    stage.area_mapping am
     on a.source_id=am.source_id
    join stage.etl_direct_idx_insert_listings t
    on a.source_listing_id=t.source_listing_id
    and a.source_id = t.source_id

    and am.area_type~*'subdivision'

    where a.source_id in {}
    order by t.target_listing_id""".format(
        str(sync_ids).replace("[", "(").replace("]", ")")
    )
    cursor_local.execute(area_subdivision)
    result = cursor_local.fetchall()
    column_names = [desc[0] for desc in cursor_local.description]
    df = pd.DataFrame(result, columns=column_names)
    df["mapped_subdivision"] = df["mapped_subdivision"].str.strip()
    df_sorted = df.sort_values(
        by=["flag_address", "listing_id"], ascending=[True, True]
    )
    df_unique = df_sorted.drop_duplicates(subset=["flag_address", "listing_id"])
    df_unique["flag_address"] = df_unique["flag_address"].astype(int)
    filtered_df = df_unique[df_unique["flag_address"] == 9]
    list_of_tuples = filtered_df.to_records(index=False).tolist()
    cols = ",".join(list(filtered_df.columns))
    insert_query = """INSERT INTO stage.area_normalize ({}) VALUES %s
                                    """.format(cols)
    extras.execute_values(cursor_local, insert_query, list_of_tuples)


def area_normalize_community_insert(
    sync_ids, cursor_local, cursor_stage, db_connection_local
):
    area_community = """
    select
    a.community_name as orignal_community,
    regexp_replace(UPPER(a.community_name), area_name_exp , am.area_name) as mapped_community,
    cast(case when (replace( regexp_replace(UPPER(a.community_name), area_name_exp , '*****'),UPPER(a.community_name),'!!!!!')) like '%*****%' then '9' else '0' end as int) as flag_address,
    a.source_listing_id,
    a.source_id,
    am.area_name_exp,
    am.area_name,
    am.area_type,

    a.y_creation_date,	
    a.y_last_update_date,	
    a.source_creation_date,	
    a.source_last_update_date,
    t.target_listing_id as listing_id

    from stage.direct_idx_address a
    join
    stage.area_mapping am
     on a.source_id=am.source_id

    join stage.etl_direct_idx_insert_listings t
    on a.source_listing_id=t.source_listing_id
    and a.source_id = t.source_id

    and am.area_type~*'community'
    where a.source_id in {}
    order by t.target_listing_id""".format(
        str(sync_ids).replace("[", "(").replace("]", ")")
    )
    cursor_local.execute(area_community)
    result = cursor_local.fetchall()
    column_names = [desc[0] for desc in cursor_local.description]
    df = pd.DataFrame(result, columns=column_names)
    df["mapped_community"] = df["mapped_community"].str.strip()
    df_sorted = df.sort_values(
        by=["flag_address", "listing_id"], ascending=[True, True]
    )
    df_unique = df_sorted.drop_duplicates(subset=["flag_address", "listing_id"])
    df_unique["flag_address"] = df_unique["flag_address"].astype(int)
    filtered_df = df_unique[df_unique["flag_address"] == 9]
    list_of_tuples = filtered_df.to_records(index=False).tolist()
    cols = ",".join(list(filtered_df.columns))
    insert_query = """INSERT INTO stage.area_normalize ({}) VALUES %s
                                        """.format(cols)
    extras.execute_values(cursor_local, insert_query, list_of_tuples)


def area_normalize_subdivision_update(
    sync_ids, cursor_local, cursor_stage, db_connection_local
):

    area_subdivision = """
    select 
    a.subdivision_name as orignal_subdivision,
    regexp_replace(UPPER(a.subdivision_name), area_name_exp , am.area_name)  as mapped_subdivision,
    cast(case when (replace( regexp_replace(UPPER(a.subdivision_name), area_name_exp , '*****'),UPPER(a.subdivision_name),'!!!!!')) like '%*****%' then '9' else '0' end as int) as flag_address,

    a.source_listing_id,
    a.source_id,
    am.area_name_exp,
    am.area_name,
    am.area_type,

    a.y_creation_date,	
    a.y_last_update_date,	
    a.source_creation_date,	
    a.source_last_update_date,
    t.target_listing_id as listing_id


    from stage.direct_idx_address a
    join
    stage.area_mapping am
     on a.source_id=am.source_id
    join stage.etl_direct_idx_update_listings t
    on a.source_listing_id=t.source_listing_id
    and a.source_id = t.source_id

    and am.area_type~*'subdivision'

    where a.source_id in {} and t.target_listing_id is not null
    order by t.target_listing_id""".format(
        str(sync_ids).replace("[", "(").replace("]", ")")
    )
    cursor_local.execute(area_subdivision)
    result = cursor_local.fetchall()
    column_names = [desc[0] for desc in cursor_local.description]
    df = pd.DataFrame(result, columns=column_names)
    df["mapped_subdivision"] = df["mapped_subdivision"].str.strip()
    df_sorted = df.sort_values(
        by=["flag_address", "listing_id"], ascending=[True, True]
    )
    df_unique = df_sorted.drop_duplicates(subset=["flag_address", "listing_id"])
    df_unique["flag_address"] = df_unique["flag_address"].astype(int)
    filtered_df = df_unique[df_unique["flag_address"] == 9]
    list_of_tuples = filtered_df.to_records(index=False).tolist()
    cols = ",".join(list(filtered_df.columns))
    insert_query = """INSERT INTO stage.area_normalize ({}) VALUES %s
                                    """.format(cols)
    extras.execute_values(cursor_local, insert_query, list_of_tuples)


def area_normalize_community_update(
    sync_ids, cursor_local, cursor_stage, db_connection_local
):
    area_community = """
    select
    a.community_name as orignal_community,
    regexp_replace(UPPER(a.community_name), area_name_exp , am.area_name) as mapped_community,
    cast(case when (replace( regexp_replace(UPPER(a.community_name), area_name_exp , '*****'),UPPER(a.community_name),'!!!!!')) like '%*****%' then '9' else '0' end as int) as flag_address,
    a.source_listing_id,
    a.source_id,
    am.area_name_exp,
    am.area_name,
    am.area_type,

    a.y_creation_date,	
    a.y_last_update_date,	
    a.source_creation_date,	
    a.source_last_update_date,
    t.target_listing_id as listing_id

    from stage.direct_idx_address a
    join
    stage.area_mapping am
     on a.source_id=am.source_id

    join stage.etl_direct_idx_update_listings t
    on a.source_listing_id=t.source_listing_id
    and a.source_id = t.source_id

    and am.area_type~*'community'
    where a.source_id in {} and t.target_listing_id is not null
    order by t.target_listing_id""".format(
        str(sync_ids).replace("[", "(").replace("]", ")")
    )
    cursor_local.execute(area_community)
    result = cursor_local.fetchall()
    column_names = [desc[0] for desc in cursor_local.description]
    df = pd.DataFrame(result, columns=column_names)
    df["mapped_community"] = df["mapped_community"].str.strip()
    df_sorted = df.sort_values(
        by=["flag_address", "listing_id"], ascending=[True, True]
    )
    df_unique = df_sorted.drop_duplicates(subset=["flag_address", "listing_id"])
    df_unique["flag_address"] = df_unique["flag_address"].astype(int)
    filtered_df = df_unique[df_unique["flag_address"] == 9]
    list_of_tuples = filtered_df.to_records(index=False).tolist()
    cols = ",".join(list(filtered_df.columns))
    insert_query = """INSERT INTO stage.area_normalize ({}) VALUES %s
                                        """.format(cols)
    extras.execute_values(cursor_local, insert_query, list_of_tuples)


def area_normalize_update_listing_address_one(
    sync_ids, cursor_local, cursor_stage, db_connection_local
):
    address_update = """
    SELECT
    a.source_id,
    a.source_listing_id,
    REPLACE(MAX(a.mapped_subdivision),'''','''''') AS mapped_subdivision,
    REPLACE(MAX(a.mapped_community), '''','''''') AS mapped_community
    FROM
        stage.area_normalize a
    JOIN
        stage.etl_direct_idx_update_listings e ON a.source_id = e.source_id
        and 
        a.source_listing_id = e.source_listing_id 
    WHERE
        a.source_id in {} and e.target_listing_id is not null
    GROUP BY
        a.source_id,
        a.source_listing_id""".format(str(sync_ids).replace("[", "(").replace("]", ")"))
    cursor_local.execute(address_update)
    result = cursor_local.fetchall()
    for i in result:
        update_query = """update stage.direct_idx_address set subdivision_name = '{}', community_name = '{}'
                      where source_id = {} and source_listing_id = '{}'""".format(
            i[2], i[3], i[0], i[1]
        )
        cursor_local.execute(update_query)
    db_connection_local.commit()


def area_normalize_update_listing_address_subdivision(
    sync_ids, cursor_local, cursor_stage, db_connection_local
):
    address_update = """
    SELECT
    a.source_id,
    a.source_listing_id,
    REPLACE(MAX(a.mapped_subdivision),'''','''''') AS mapped_subdivision
    FROM
        stage.area_normalize a
    JOIN
        stage.etl_direct_idx_update_listings e ON a.source_id = e.source_id
        and 
        a.source_listing_id = e.source_listing_id 
    WHERE
        a.source_id in {} and e.target_listing_id is not null and a.mapped_subdivision is not null
    GROUP BY
        a.source_id,
        a.source_listing_id""".format(str(sync_ids).replace("[", "(").replace("]", ")"))
    cursor_local.execute(address_update)
    result = cursor_local.fetchall()
    for i in result:
        update_query = """update stage.direct_idx_address set subdivision_name = '{}'
                      where source_id = {} and source_listing_id = '{}'""".format(
            i[2], i[0], i[1]
        )
        cursor_local.execute(update_query)
    db_connection_local.commit()


def area_normalize_update_listing_address_community(
    sync_ids, cursor_local, cursor_stage, db_connection_local
):
    address_update = """
    SELECT
    a.source_id,
    a.source_listing_id,
    REPLACE(MAX(a.mapped_community), '''','''''') AS mapped_community
    FROM
        stage.area_normalize a
    JOIN
        stage.etl_direct_idx_update_listings e ON a.source_id = e.source_id
        and 
        a.source_listing_id = e.source_listing_id 
    WHERE
        a.source_id in {} and e.target_listing_id is not null and a.mapped_community is not null
    GROUP BY
        a.source_id,
        a.source_listing_id""".format(str(sync_ids).replace("[", "(").replace("]", ")"))
    cursor_local.execute(address_update)
    result = cursor_local.fetchall()
    for i in result:
        update_query = """update stage.direct_idx_address set community_name = '{}'
                      where source_id = {} and source_listing_id = '{}'""".format(
            i[2], i[0], i[1]
        )
        cursor_local.execute(update_query)
    db_connection_local.commit()


def area_normalize_insert_listing_address_one(
    sync_ids, cursor_local, cursor_stage, db_connection_local
):
    address_update = """
    SELECT
    a.source_id,
    a.source_listing_id,
    REPLACE(MAX(a.mapped_subdivision),'''','''''') AS mapped_subdivision,
    REPLACE(MAX(a.mapped_community), '''','''''') AS mapped_community
    FROM
        stage.area_normalize a
    JOIN
        stage.etl_direct_idx_insert_listings e ON a.source_id = e.source_id
        and 
        a.source_listing_id = e.source_listing_id 
    WHERE
        a.source_id in {} and e.target_listing_id is not null
    GROUP BY
        a.source_id,
        a.source_listing_id""".format(str(sync_ids).replace("[", "(").replace("]", ")"))
    cursor_local.execute(address_update)
    result = cursor_local.fetchall()
    for i in result:
        update_query = """update stage.direct_idx_address set subdivision_name = '{}', community_name = '{}'
                      where source_id = {} and source_listing_id = '{}'""".format(
            i[2], i[3], i[0], i[1]
        )
        cursor_local.execute(update_query)
    db_connection_local.commit()


def area_normalize_insert_listing_address_subdivision(
    sync_ids, cursor_local, cursor_stage, db_connection_local
):
    address_update = """
    SELECT
    a.source_id,
    a.source_listing_id,
    REPLACE(MAX(a.mapped_subdivision),'''','''''') AS mapped_subdivision

    FROM
        stage.area_normalize a
    JOIN
        stage.etl_direct_idx_insert_listings e ON a.source_id = e.source_id
        and 
        a.source_listing_id = e.source_listing_id 
    WHERE
        a.source_id in {} and e.target_listing_id is not null and a.mapped_subdivision is not null
    GROUP BY
        a.source_id,
        a.source_listing_id""".format(str(sync_ids).replace("[", "(").replace("]", ")"))
    cursor_local.execute(address_update)
    result = cursor_local.fetchall()
    for i in result:
        update_query = """update stage.direct_idx_address set subdivision_name = '{}'
                      where source_id = {} and source_listing_id = '{}'""".format(
            i[2], i[0], i[1]
        )
        cursor_local.execute(update_query)
    db_connection_local.commit()


def area_normalize_insert_listing_address_community(
    sync_ids, cursor_local, cursor_stage, db_connection_local
):
    address_update = """
    SELECT
    a.source_id,
    a.source_listing_id,
    REPLACE(MAX(a.mapped_community), '''','''''') AS mapped_community
    FROM
        stage.area_normalize a
    JOIN
        stage.etl_direct_idx_insert_listings e ON a.source_id = e.source_id
        and 
        a.source_listing_id = e.source_listing_id 
    WHERE
        a.source_id in {} and e.target_listing_id is not null and a.mapped_community is not null
    GROUP BY
        a.source_id,
        a.source_listing_id""".format(str(sync_ids).replace("[", "(").replace("]", ")"))
    cursor_local.execute(address_update)
    result = cursor_local.fetchall()
    for i in result:
        update_query = """update stage.direct_idx_address set community_name = '{}'
                      where source_id = {} and source_listing_id = '{}'""".format(
            i[2], i[0], i[1]
        )
        cursor_local.execute(update_query)
    db_connection_local.commit()


def update_load_date(
    sync_ids, batch_ids, cursor_stage, db_connection_local, db_connection_stage
):
    update_load_date_query = """
    UPDATE listing SET load_flag=false WHERE load_flag is true AND batch_id IN {1} AND source_id IN {0}
    """.format(
        str(sync_ids).replace("[", "(").replace("]", ")"),
        str(batch_ids).replace("[", "(").replace("]", ")"),
    )
    try:
        cursor_stage.execute(update_load_date_query)
        db_connection_stage.commit()
    except Exception as e:
        log_msg = {"Error":str(e), "Error At line": traceback.format_exc()}
        raise Exception(log_msg)


def update_listing_status(
    sync_ids, cursor_local, cursor_stage, db_connection_local, db_connection_stage
):
    select_listing_status_for_update = """
    select  
        l.target_listing_id as listing_id,
        s.batch_id as batch_id, 
        s.source_id as source_id, 
		ls.id as listing_status_id, 
	    CASE WHEN ls.display_flag is false AND upper(ls.ylopo_status) != 'SOLD' THEN 'INACTIVE'
	WHEN upper(ls.ylopo_status) = 'SOLD' THEN 'SOLD'
	ELSE 'ACTIVE' END  AS source_status

    from stage.DIRECT_idx_listing s
    join listing_status ls
    on s.listing_status=ls.status
    and s.source_id=ls.source_id
    inner join 
        stage.etl_direct_idx_update_listings l
        on l.source_listing_id = s.source_listing_id
        and l.source_id = s.source_id

    where (ls.load_flag is true or ls.display_flag is true)
    and s.source_id IN {};
    """.format(str(sync_ids).replace("[", "(").replace("]", ")"))
    cursor_local.execute(select_listing_status_for_update)
    result = cursor_local.fetchall()
    column_names = [desc[0] for desc in cursor_local.description]
    df = pd.DataFrame(result, columns=column_names)
    update_data = [
        (row["source_status"], row["listing_status_id"], row["listing_id"])
        for index, row in df.iterrows()
    ]
    update_query = f"""
            UPDATE public.listing
           SET source_status = data.source_status::source_status_type, listing_status_id = data.listing_status_id FROM (VALUES %s) as 
           data (source_status,listing_status_id, listing_id)
           WHERE id = data.listing_id
           """
    try:

        extras.execute_values(cursor_stage, update_query, update_data)
        db_connection_stage.commit()
    except Exception as e:
        log_msg = {
            "Status": "Exception in update_listing_status ",
            "Error": str(e),
            "Error At line": traceback.format_exc(),
        }
        raise Exception(log_msg)


def update_listing_price(
    sync_ids, cursor_local, cursor_stage, db_connection_local, db_connection_stage
):
    price = """
    SELECT 
        S.price as price
    	,l.price AS prior_price
    	,s.y_creation_date as price_update_date
    	,l.listing_id
        FROM stage.listing_lookup l
        	JOIN 
        		stage.etl_direct_idx_update_listings A
        		ON
        		l.listing_id=A.target_listing_id
        	JOIN
        		stage.DIRECT_idx_listing S
        		ON
        		A.source_listing_id= S.source_listing_id
    			AND A.source_id=S.source_id
        	WHERE
        	cast(S.price as numeric(18,2)) <>l.price
        and A.source_id in {}""".format(
        str(sync_ids).replace("[", "(").replace("]", ")")
    )
    cursor_local.execute(price)
    price_data = cursor_local.fetchall()
    column_names = [desc[0] for desc in cursor_local.description]
    price_df = pd.DataFrame(price_data, columns=column_names)
    listing_ids = list(price_df["listing_id"])
    update_data = [
        (
            row["price"],
            row["prior_price"],
            (row["price_update_date"]),
            row["listing_id"],
        )
        for index, row in price_df.iterrows()
    ]

    update_query = f"""
        UPDATE public.listing
       SET price = data.price, prior_price = data.prior_prior_price, price_update_date = data.price_update_date FROM (VALUES %s) as 
       data (price,prior_prior_price, price_update_date, listing_id)
       WHERE id = data.listing_id
       """
    try:

        extras.execute_values(cursor_stage, update_query, update_data)
        db_connection_stage.commit()
        return listing_ids
    except Exception as e:
        log_msg = {
            "Status": "Exception in update_listing_price ",
            "Error": str(e),
            "Error At line": traceback.format_exc(),
        }
        raise Exception(log_msg)


def update_address_insert(sync_ids, cursor_local, cursor_stage, db_connection_stage):

    # # Return early if source_id 859 is not in sync_ids
    # if 859 not in sync_ids:
    #     return

    address_query = """
     select 
       t.target_listing_id as listing_id,
       s.city,
       s.county,
       s.full_street_address,
       s.y_creation_date as y_creation_date,
       s.y_creation_date as y_last_update_date,
       s.source_creation_date,                   
       s.source_last_update_date,
       s.zoning, 
       s.mls_latitude,
       s.mls_longitude,
       s.postal_code,
       s.unit_number, 
       s.state_or_province,
       s.parcel_number as parcel_id,
       s.batch_id as batch_id,
       s.country,                            
       UPPER(s.community_name) as community_name,
       s.region,
       s.zone,
       t.source_id as source_id,
       UPPER(s.subdivision_name) as subdivision_name,
       UPPER(s.district_name) as district_name,
       UPPER(s.mls_area_name) as mls_area_name,
       UPPER(s.custom_area_name_1) as custom_area_name_1,
       UPPER(s.custom_area_name_2) as custom_area_name_2,
       UPPER(s.custom_area_name_3) as custom_area_name_3,
       UPPER(s.sub_district_name) as sub_district_name,
       UPPER(TRIM(BOTH ' ' FROM CONCAT(
               REGEXP_REPLACE(REGEXP_REPLACE(s.full_street_address, '[aeiouAEIOU ]', '','g'), '[^a-zA-Z\d\s:]', '','g'),
               REGEXP_REPLACE(REGEXP_REPLACE(s.city, '[aeiouAEIOU ]', '','g'), '[^a-zA-Z\d\s:]', '','g'),
               REGEXP_REPLACE(REGEXP_REPLACE(s.postal_code, '[aeiouAEIOU ]', '','g'), '[^a-zA-Z\d\s:]', '','g')
           ))) AS address_token
        from stage.direct_idx_address s
            join stage.etl_direct_idx_update_listings t
              on s.source_listing_id=t.source_listing_id
                   and s.source_id = t.source_id
    where s.source_id in {} and t.target_listing_id is not null
    order by t.target_listing_id
     """.format(str(sync_ids).replace("[", "(").replace("]", ")"))
    cursor_local.execute(address_query)
    result = cursor_local.fetchall()
    column_names = [desc[0] for desc in cursor_local.description]
    df_stage = pd.DataFrame(result, columns=column_names)

    if df_stage.empty:
        return

    # Step 2: Fetch existing listing_id values
    listing_ids = tuple(df_stage["listing_id"].drop_duplicates())

    # Avoid empty tuple issue in SQL
    if not listing_ids:
        return

    # Create query string for filtering existing rows
    query_existing = """
        SELECT listing_id 
        FROM public.listing_address
        WHERE listing_id IN %s
    """
    cursor_stage.execute(query_existing, (listing_ids,))
    existing_listing_ids = set(row[0] for row in cursor_stage.fetchall())

    # Step 3: Filter out existing listing_ids from df_stage
    df_stage_filtered = df_stage[~df_stage["listing_id"].isin(existing_listing_ids)]

    if df_stage_filtered.empty:
        return

    # Step 4: Insert filtered records
    insert_query = """INSERT INTO public.listing_address ({}) VALUES %s""".format(
        ",".join(df_stage_filtered.columns)
    )
    extras.execute_values(cursor_stage, insert_query, df_stage_filtered.values.tolist())


def update_address_status(
    sync_ids, cursor_local, cursor_stage, db_connection_local, db_connection_stage
):
    listing_id_list_query = """SELECT
    target_listing_id as listing_id
    FROM
    stage.etl_direct_idx_insert_listings
    WHERE
    SOURCE_ID
    IN {}""".format(str(sync_ids).replace("[", "(").replace("]", ")"))
    cursor_local.execute(listing_id_list_query)
    listing_id_result = cursor_local.fetchall()
    listing_ids = [int(id[0]) for id in listing_id_result if id[0] is not None]

    update_address_status_for_insert_listing = """ UPDATE
    listing_address
    a
    set
    source_status = l.source_status
    FROM
    listing
    l
    WHERE
    a.listing_id = l.id
    AND
    a.listing_id in {}""".format(
        str(listing_ids).replace("[", "(").replace("]", ")")
        if len(listing_ids) != 0
        else f"(0)"
    )
    cursor_stage.execute(update_address_status_for_insert_listing)
    db_connection_stage.commit()

    update_address_status_standard_for_insert_listing = """ UPDATE
    listing_address_standard
    a
    set
    source_status = l.source_status
    FROM
    listing
    l
    WHERE
    a.listing_id = l.id
    AND
    a.listing_id in {}""".format(
        str(listing_ids).replace("[", "(").replace("]", ")")
        if len(listing_ids) != 0
        else f"(0)"
    )
    cursor_stage.execute(update_address_status_standard_for_insert_listing)
    db_connection_stage.commit()

    listing_ids_query = """SELECT
    target_listing_id as listing_id
    FROM
    stage.etl_direct_idx_update_listings
    WHERE
    SOURCE_ID
    IN {}""".format(str(sync_ids).replace("[", "(").replace("]", ")"))
    cursor_local.execute(listing_ids_query)
    listing_id_result = cursor_local.fetchall()
    listing_ids = [int(id[0]) for id in listing_id_result if id[0] is not None]

    update_address_status_for_update_listing = """UPDATE
    listing_address
    a
    set
    source_status = l.source_status
    FROM
    listing
    l
    WHERE
    a.listing_id = l.id
    AND
    a.listing_id in {}""".format(
        str(listing_ids).replace("[", "(").replace("]", ")")
        if len(listing_ids) != 0
        else f"(0)"
    )
    cursor_stage.execute(update_address_status_for_update_listing)
    db_connection_stage.commit()

    update_address_status_standard_for_update_listing = """UPDATE
    listing_address_standard
    a
    set
    source_status = l.source_status
    FROM
    listing
    l
    WHERE
    a.listing_id = l.id
    AND
    a.listing_id in {}""".format(
        str(listing_ids).replace("[", "(").replace("]", ")")
        if len(listing_ids) != 0
        else f"(0)"
    )
    cursor_stage.execute(update_address_status_standard_for_update_listing)
    db_connection_stage.commit()


def sync_Listing_Property_Type_Search(sync_ids, cursor_local, cursor_stage):
    query = """
        select distinct on (t.target_listing_id)
        t.target_listing_id as listing_id
        , s.source_last_update_date 
        , s.source_id
        , s.batch_id as batch_id
        , s.y_creation_date
        , s.y_last_update_date
        ,ilpt.is_condo
        ,ilpt.is_foreclosure
        ,ilpt.is_land
        ,ilpt.is_manufactured_home
        ,ilpt.is_mobile_home
        ,ilpt.is_multifamily
        ,ilpt.is_rental
        ,ilpt.is_short_sale
        ,ilpt.is_single_family_home
        ,ilpt.is_townhouse
        ,ilpt.is_income_property
        ,ilpt.is_commercial_property
        ,ilpt.is_farm_ranch
        ,ilpt.is_other
        ,ilpt.is_coop
        ,ilpt.is_condop
        ,ilpt.is_condo_hotel

        from stage.direct_idx_listing s 
        join stage.etl_direct_idx_insert_listings t
        on s.source_listing_id=t.source_listing_id
        and s.source_id = t.source_id
        inner join 
        public.listing_property_type lpt
        on 
        s.source_id = lpt.source_id and
        --s.property_type = lpt.property_type
        (s.property_type = lpt.property_type or (s.property_type is null and lpt.property_type is null))

        inner join 
        public.listing_property_sub_type lpst
        on 
        s.source_id = lpst.source_id and
        --s.property_sub_type = lpst.property_sub_type
        (s.property_sub_type = lpst.property_sub_type or (s.property_sub_type is null and lpst.property_sub_type is null))

        inner join idx_config.listing_property_type ilpt on
        s.source_id = ilpt.source_id and
        lpt.id = ilpt.property_type_id  and
        lpst.id = ilpt.property_sub_type_id

        where s.source_id in {} and t.target_listing_id is not null ;
""".format(str(sync_ids).replace("[", "(").replace("]", ")"))
    cursor_local.execute(query)

    result = cursor_local.fetchall()
    column_names = [desc[0] for desc in cursor_local.description]
    df = pd.DataFrame(result, columns=column_names)
    cols = ",".join(list(df.columns))
    insert_query = """INSERT INTO public.listing_property_type_search ({}) VALUES %s
                                """.format(cols)
    extras.execute_values(cursor_stage, insert_query, result)


def update_insert_Listing_Property_Type_Search(sync_ids, cursor_local, cursor_stage):
    query = """
        select 
        t.target_listing_id as listing_id
        , s.source_last_update_date 
        , s.source_id
        , s.batch_id as batch_id
        , s.y_creation_date
        , s.y_last_update_date
        ,ilpt.is_condo
        ,ilpt.is_foreclosure
        ,ilpt.is_land
        ,ilpt.is_manufactured_home
        ,ilpt.is_mobile_home
        ,ilpt.is_multifamily
        ,ilpt.is_rental
        ,ilpt.is_short_sale
        ,ilpt.is_single_family_home
        ,ilpt.is_townhouse
        ,ilpt.is_income_property
        ,ilpt.is_commercial_property
        ,ilpt.is_farm_ranch
        ,ilpt.is_other
        ,ilpt.is_coop
        ,ilpt.is_condop
        ,ilpt.is_condo_hotel

        from stage.direct_idx_listing s 
        join stage.etl_direct_idx_update_listings t
        on s.source_listing_id=t.source_listing_id
        and s.source_id = t.source_id
        inner join 
        public.listing_property_type lpt
        on 
        s.source_id = lpt.source_id and
        --s.property_type = lpt.property_type
        (s.property_type = lpt.property_type or (s.property_type is null and lpt.property_type is null))

        inner join 
        public.listing_property_sub_type lpst
        on 
        s.source_id = lpst.source_id and
        --s.property_sub_type = lpst.property_sub_type
        (s.property_sub_type = lpst.property_sub_type or (s.property_sub_type is null and lpst.property_sub_type is null))

        inner join idx_config.listing_property_type ilpt on
        s.source_id = ilpt.source_id and
        lpt.id = ilpt.property_type_id  and
        lpst.id = ilpt.property_sub_type_id

        where s.source_id in {} and t.target_listing_id is not null ;
    """.format(str(sync_ids).replace("[", "(").replace("]", ")"))
    cursor_local.execute(query)

    result = cursor_local.fetchall()
    column_names = [desc[0] for desc in cursor_local.description]
    df = pd.DataFrame(result, columns=column_names)

    if df.empty:
        return
    # Step 2: Fetch existing listing_id values
    listing_ids = df["listing_id"].to_list()

    # Create query string for filtering existing rows
    query = """ select listing_id 
    from public.listing_property_type_search 
    where listing_id in %s """

    cursor_stage.execute(query, (tuple(listing_ids),))
    existing_listing_ids = set(row[0] for row in cursor_stage.fetchall())

    # Step 3: Filter out existing listing_ids from df
    df_stage_filtered = df[~df["listing_id"].isin(existing_listing_ids)]

    if df_stage_filtered.empty:
        return

    # Step 4: Insert filtered records
    cols = ",".join(list(df_stage_filtered.columns))
    insert_query = """INSERT INTO public.listing_property_type_search ({}) VALUES %s
                                """.format(cols)
    extras.execute_values(cursor_stage, insert_query, df_stage_filtered.values.tolist())


def sync_listing_marketing_info(
    sync_ids,
    batch_ids,
    cursor_local,
    cursor_stage,
    db_connection_local,
    db_connection_stage,
):
    mi = """
    select 
    t.target_listing_id as listing_id,
    s.y_creation_date,
    s.y_last_update_date,
    S.batch_id
    , s.permit_address_on_internet
    , s.vow_address_display
    , s.vow_automated_valuation_display
    , s.vow_consumer_comment
    , s.source_creation_date, 
    s.source_last_update_date 
    
    from stage.direct_idx_listing s 
    join stage.etl_direct_idx_insert_listings t
    on s.source_listing_id=t.source_listing_id
    and s.batch_id = t.batch_id 
    
    
    where (s.permit_address_on_internet is not null or s.vow_address_display is not null or s.vow_automated_valuation_display is not null or s.vow_consumer_comment is not null) 
    and t.target_listing_id is not null
    and s.batch_id in {1}
    and s.source_id in {0};
    """.format(
        str(sync_ids).replace("[", "(").replace("]", ")"),
        str(batch_ids).replace("[", "(").replace("]", ")"),
    )
    cursor_local.execute(mi)
    result = cursor_local.fetchall()
    if result:
        column_names = [desc[0] for desc in cursor_local.description]
        df = pd.DataFrame(result, columns=column_names)
        cols = ",".join(list(df.columns))
        insert_query = """INSERT INTO public.listing_marketing_info ({}) VALUES %s
                                    """.format(cols)
        extras.execute_values(cursor_stage, insert_query, result)
        db_connection_stage.commit()


def attributes_cleansing(df):
    if len(df) > 0:
        pattern = r"[,\{\}/\$\]\\]"
        df = df.applymap(
            lambda x: None if isinstance(x, str) and x.lower() == "nan" else x
        )
        df.replace({np.nan: None}, inplace=True)
        df = df.applymap(lambda x: "" if x == "NONE" else x)
        df = df.replace(r",,", ",", regex=True)
        df = df.replace(to_replace=pattern, value="", regex=True)
        df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        df = df.drop_duplicates(subset="listing_id")
    return df


def convert_to_pg_array(val):
    """Convert Python lists to PostgreSQL array format, handling stringified lists."""
    if isinstance(val, str) and val.startswith("{") and val.endswith("}"):
        try:
            val = ast.literal_eval(
                val.replace("{", "[").replace("}", "]")
            )  # Convert '{value}' to ['value']
        except (SyntaxError, ValueError):
            return None  # Handle cases where conversion fails
    if isinstance(val, list):
        return "{" + ",".join(map(str, val)) + "}"
    return val


def sync_listing_attribute(sync_ids, cursor_local, cursor_stage, db_connection_local):
    for table_name, query in attribute_query_dict.items():
        query = query.format(str(sync_ids).replace("[", "(").replace("]", ")"))

        cursor_local.execute(query)
        result = cursor_local.fetchall()
        column_names = [desc[0] for desc in cursor_local.description]

        df = pd.DataFrame(result, columns=column_names)
        df = attributes_cleansing(df)

        # Convert NaN to None and properly format array values
        updated_data = [
            tuple(
                (
                    None
                    if isinstance(val, float) and pd.isna(val)
                    else convert_to_pg_array(val)
                )
                for val in tup
            )
            for tup in df.values
        ]

        cols = ",".join(df.columns)
        insert_query = f"INSERT INTO public.{table_name} ({cols}) VALUES %s"

        extras.execute_values(cursor_stage, insert_query, updated_data)


def query_execution_and_commit(query, cursor, connection, result):
    extras.execute_values(cursor, query, result)
    connection.commit()


def Load_Lisitng_Category_Lookup(
    sync_ids,
    batch_ids,
    cursor_local,
    cursor_stage,
    db_connection_local,
    db_connection_stage,
):
    lcl = """
    select 
     distinct listing_category as category,
     CASE  WHEN listing_category = 'Rental' OR listing_category = 'Lease' OR listing_category = 'Sub-Lease' THEN  'Rent'
     ELSE 'Purchase' end as display_category,
     l.batch_id,
     l.source_id
    
    from stage.DIRECT_idx_listing l
    left join public.listing_category lc
    	on 
    	(l.listing_category = lc.category   or  l.listing_category is null and lc.category is null) 
    	and lc.source_id in {0}
    	and l.source_id in {0}
    
    where lc.id is null and l.batch_id in {1}
    """.format(
        str(sync_ids).replace("[", "(").replace("]", ")"),
        str(batch_ids).replace("[", "(").replace("]", ")"),
    )

    cursor_local.execute(lcl)
    result = cursor_local.fetchall()
    column_names = [desc[0] for desc in cursor_local.description]
    df = pd.DataFrame(result, columns=column_names)
    cols = ",".join(list(df.columns))
    insert_query = """
                    INSERT INTO public.listing_category ({}) VALUES %s
                    """.format(cols)
    query_execution_and_commit(insert_query, cursor_stage, db_connection_stage, result)

    rds_insertion = """ select * from public.listing_category where source_id in {0} and batch_id in {1};""".format(
        str(sync_ids).replace("[", "(").replace("]", ")"),
        str(batch_ids).replace("[", "(").replace("]", ")"),
    )
    cursor_stage.execute(rds_insertion)
    result_2 = cursor_stage.fetchall()
    if result_2:
        column_names = [desc[0] for desc in cursor_stage.description]
        df_2 = pd.DataFrame(result_2, columns=column_names)
        cols_2 = ",".join(list(df_2.columns))
        insert_rds = """
                        INSERT INTO public.listing_category ({}) VALUES %s ON CONFLICT (id) DO NOTHING;
                        """.format(cols_2)
        query_execution_and_commit(
            insert_rds, cursor_local, db_connection_local, result_2
        )


def Load_Listing_Status(
    sync_ids,
    batch_ids,
    cursor_local,
    cursor_stage,
    db_connection_local,
    db_connection_stage,
):
    ls = """
    select distinct c.source_id, c.listing_status as status, c.batch_id, c.listing_status as display_status

    from stage.DIRECT_idx_listing  c
    left  join  public.listing_status p
    on
    (c.listing_status = p.status or c.listing_status is null and p.status is null)  
    and p.source_id in {0}
    and c.source_id in {0}
    join stage.ylopo_status_mapping ysm
    on c.listing_status=ysm.status

    where p.id is null and c.batch_id in {1}
    
    """.format(
        str(sync_ids).replace("[", "(").replace("]", ")"),
        str(batch_ids).replace("[", "(").replace("]", ")"),
    )

    cursor_local.execute(ls)
    result = cursor_local.fetchall()
    column_names = [desc[0] for desc in cursor_local.description]
    df = pd.DataFrame(result, columns=column_names)
    cols = ",".join(list(df.columns))
    insert_query = """
                    INSERT INTO public.listing_status ({}) VALUES %s
                    """.format(cols)
    query_execution_and_commit(insert_query, cursor_stage, db_connection_stage, result)

    rds_insertion = """ select * from public.listing_status where source_id in {0} and batch_id in {1};""".format(
        str(sync_ids).replace("[", "(").replace("]", ")"),
        str(batch_ids).replace("[", "(").replace("]", ")"),
    )
    cursor_stage.execute(rds_insertion)
    result_2 = cursor_stage.fetchall()
    if result_2:
        column_names = [desc[0] for desc in cursor_stage.description]
        df_2 = pd.DataFrame(result_2, columns=column_names)
        cols_2 = ",".join(list(df_2.columns))
        insert_rds = """
                        INSERT INTO public.listing_status ({}) VALUES %s;
                        """.format(cols_2)
        query_execution_and_commit(
            insert_rds, cursor_local, db_connection_local, result_2
        )


def Load_Listing_Property_Type(
    sync_ids,
    batch_ids,
    cursor_local,
    cursor_stage,
    db_connection_local,
    db_connection_stage,
):
    lpt = """
    select distinct c.source_id, c.property_type, c.batch_id, c.y_creation_date,
    CASE 
    WHEN c.property_type = 'Commercial/Business' THEN 'Commercial'
    WHEN c.property_type = 'Lots' THEN 'Lots And Land'
    ELSE c.property_type
    END AS display_property_type

    from stage.DIRECT_idx_listing  c
          left  join  public.listing_property_type p
           on
            (c.property_type = p.property_type  or (c.property_type is null and p.property_type is null))
             and p.source_id in {0}
             and c.source_id in {0}
    where p.id is null and c.batch_id in {1}
    
    """.format(
        str(sync_ids).replace("[", "(").replace("]", ")"),
        str(batch_ids).replace("[", "(").replace("]", ")"),
    )

    cursor_local.execute(lpt)
    result = cursor_local.fetchall()
    column_names = [desc[0] for desc in cursor_local.description]
    df = pd.DataFrame(result, columns=column_names)
    cols = ",".join(list(df.columns))

    insert_query = """
                    INSERT INTO public.listing_property_type ({}) VALUES %s
                    """.format(cols)
    query_execution_and_commit(insert_query, cursor_stage, db_connection_stage, result)

    rds_insertion = """ select * from public.listing_property_type where source_id in {0} and batch_id in {1};""".format(
        str(sync_ids).replace("[", "(").replace("]", ")"),
        str(batch_ids).replace("[", "(").replace("]", ")"),
    )
    cursor_stage.execute(rds_insertion)
    result_2 = cursor_stage.fetchall()
    if result_2:

        column_names = [desc[0] for desc in cursor_stage.description]
        df_2 = pd.DataFrame(result_2, columns=column_names)
        cols_2 = ",".join(list(df_2.columns))
        insert_rds = """
                        INSERT INTO public.listing_property_type ({}) VALUES %s ON CONFLICT (id) DO NOTHING;
                        """.format(cols_2)
        query_execution_and_commit(
            insert_rds, cursor_local, db_connection_local, result_2
        )


def load_listing_property_sub_type(
    sync_ids,
    batch_ids,
    cursor_local,
    cursor_stage,
    db_connection_local,
    db_connection_stage,
):
    llpst = """
    select distinct c.source_id,c.property_sub_type , c.batch_id , c.y_creation_date,
    CASE 
            WHEN c.property_sub_type IN ('Attached', 'Timeshare', 'Villa/Townhome (Common Wall)') THEN 'Townhouse'
            WHEN c.property_sub_type IN ('Commercial/Industrial', 'Commercial Sale') THEN 'Commercial'
            WHEN c.property_sub_type IN ('Condominium', 'Condo') THEN 'Condominium'
            WHEN c.property_sub_type IN ('Farm', 'Farm/Forest') THEN 'Farm'
            WHEN c.property_sub_type = 'Land' THEN 'Land'
            WHEN c.property_sub_type = 'Floating Home' THEN 'Other'
            WHEN c.property_sub_type IN ('Manufactured Home in Park', 'Manufactured Home on Real Property') THEN 'Single Family Detached'
            WHEN c.property_sub_type IN ('Multifamily', 'MultiFamily') THEN 'MultiFamily'
            WHEN c.property_sub_type = 'Partially Owned' THEN 'Single Family Attached'
            WHEN c.property_sub_type = 'Planned Unit Development' THEN 'Other'
            WHEN c.property_sub_type = 'Recreation Only' THEN 'Other'
            WHEN c.property_sub_type IN ('Residential/Recreational', 'Single Family Residence', 'Single Family Detached') THEN 'Single Family Detached'
            ELSE ''
        END AS display_property_sub_type

        from stage.DIRECT_idx_listing  c
              left  join  public.listing_property_sub_type p
               on
                 (c.property_sub_type = p.property_sub_type  or (c.property_sub_type is null and p.property_sub_type is null))
                 and p.source_id in {0}
                 and c.source_id in {0}
        where p.id is null and c.batch_id in {1}
            
    """.format(
        str(sync_ids).replace("[", "(").replace("]", ")"),
        str(batch_ids).replace("[", "(").replace("]", ")"),
    )

    cursor_local.execute(llpst)
    result = cursor_local.fetchall()
    column_names = [desc[0] for desc in cursor_local.description]
    df = pd.DataFrame(result, columns=column_names)
    cols = ",".join(list(df.columns))

    insert_query = """
                    INSERT INTO public.listing_property_sub_type ({}) VALUES %s
                    """.format(cols)
    query_execution_and_commit(insert_query, cursor_stage, db_connection_stage, result)

    rds_insertion = """ select * from public.listing_property_sub_type where source_id in {0} and batch_id in {1};""".format(
        str(sync_ids).replace("[", "(").replace("]", ")"),
        str(batch_ids).replace("[", "(").replace("]", ")"),
    )
    cursor_stage.execute(rds_insertion)
    result_2 = cursor_stage.fetchall()
    if result_2:
        column_names = [desc[0] for desc in cursor_stage.description]
        df_2 = pd.DataFrame(result_2, columns=column_names)
        cols_2 = ",".join(list(df_2.columns))
        insert_rds = """
                        INSERT INTO public.listing_property_sub_type ({}) VALUES %s ON CONFLICT (id) DO NOTHING;
                        """.format(cols_2)
        query_execution_and_commit(
            insert_rds, cursor_local, db_connection_local, result_2
        )


def load_mls_board(
    sync_ids,
    batch_ids,
    cursor_local,
    cursor_stage,
    db_connection_local,
    db_connection_stage,
):
    lmb = """
    select distinct c.source_id,
    c.mls as mls_source_id,
    c.batch_id,
    true as active_flag,
    current_timestamp as active_date,
    current_timestamp as creation_date,
    current_timestamp as last_update_date,
     'All' as type,
     'All' as city,
     'All' as state,
     'All' as country

    from stage.direct_idx_listing  c
          left  join  public.mls_board p
           on
             (c.mls = p.mls_source_id  or c.mls is null and p.mls_source_id is null)
             and p.source_id in {0}
             and c.source_id in {0}
    where p.id is null and c.batch_id in {1}
        
    """.format(
        str(sync_ids).replace("[", "(").replace("]", ")"),
        str(batch_ids).replace("[", "(").replace("]", ")"),
    )

    cursor_local.execute(lmb)
    result = cursor_local.fetchall()
    column_names = [desc[0] for desc in cursor_local.description]
    df = pd.DataFrame(result, columns=column_names)
    cols = ",".join(list(df.columns))

    insert_query = """
                    INSERT INTO public.mls_board ({}) VALUES %s
                    """.format(cols)
    query_execution_and_commit(insert_query, cursor_stage, db_connection_stage, result)

    rds_insertion = """ select * from public.mls_board where source_id in {0} and batch_id in {1};""".format(
        str(sync_ids).replace("[", "(").replace("]", ")"),
        str(batch_ids).replace("[", "(").replace("]", ")"),
    )
    cursor_stage.execute(rds_insertion)
    result_2 = cursor_stage.fetchall()
    if result_2:
        column_names = [desc[0] for desc in cursor_stage.description]
        df_2 = pd.DataFrame(result_2, columns=column_names)
        cols_2 = ",".join(list(df_2.columns))

        insert_rds = """
                        INSERT INTO public.mls_board ({}) VALUES %s ;
                        """.format(cols_2)
        query_execution_and_commit(
            insert_rds, cursor_local, db_connection_local, result_2
        )


def load_config_listing_property_type(
    sync_ids,
    batch_ids,
    cursor_local,
    cursor_stage,
    db_connection_local,
    db_connection_stage,
):
    lclpt = """
    select distinct c.source_id, 
    c.property_type, 
    c.property_sub_type, 
    c.y_creation_date,
    c.y_creation_date as y_last_update_date,
    lpt.id as property_type_id,
    lpst.id as property_sub_type_id
    from stage.DIRECT_idx_listing  c
    left  join  idx_config.listing_property_type p
        on    (c.property_type = p.property_type  or (c.property_type is null and p.property_type is null))
        and (c.property_sub_type = p.property_sub_type  or (c.property_sub_type is null and p.property_sub_type is null))
        and p.source_id in {0}
        and c.source_id in {0}
    join public.listing_property_type lpt
    	on c.source_id=lpt.source_id
    	and c.source_id in {0}
    	and lpt.source_id in {0}
    	and (c.property_type=lpt.property_type or (c.property_type is null and lpt.property_type is null))
    join public.listing_property_sub_type lpst
        on c.source_id=lpst.source_id
        and c.source_id in {0}
    	and lpst.source_id in {0}
    	and (c.property_sub_type=lpst.property_sub_type or (c.property_sub_type is null and lpst.property_sub_type is null))
    where p.id is null and c.batch_id in {1}
    """.format(
        str(sync_ids).replace("[", "(").replace("]", ")"),
        str(batch_ids).replace("[", "(").replace("]", ")"),
    )

    cursor_local.execute(lclpt)
    result = cursor_local.fetchall()
    if result:
        column_names = [desc[0] for desc in cursor_local.description]
        df = pd.DataFrame(result, columns=column_names)
        cols = ",".join(list(df.columns))

        insert_query = """
                        INSERT INTO idx_config.listing_property_type ({}) VALUES %s ON CONFLICT (source_id, property_type_id, property_sub_type_id ) DO NOTHING;
                        """.format(cols)

        query_execution_and_commit(
            insert_query, cursor_stage, db_connection_stage, result
        )

    rds_insertion = """ select * from idx_config.listing_property_type where source_id in {0} ;""".format(
        str(sync_ids).replace("[", "(").replace("]", ")")
    )

    cursor_stage.execute(rds_insertion)
    result_2 = cursor_stage.fetchall()
    if result_2:
        rds_delete = "Delete from idx_config.listing_property_type where source_id in {0}".format(
            str(sync_ids).replace("[", "(").replace("]", ")")
        )
        cursor_local.execute(rds_delete)
        db_connection_local.commit()

        column_names = [desc[0] for desc in cursor_stage.description]
        df_2 = pd.DataFrame(result_2, columns=column_names)
        cols_2 = ",".join(list(df_2.columns))

        insert_rds = """
                        INSERT INTO idx_config.listing_property_type ({}) VALUES %s  ON CONFLICT (source_id, property_type_id, property_sub_type_id) DO NOTHING;
                        """.format(cols_2)

        query_execution_and_commit(
            insert_rds, cursor_local, db_connection_local, result_2
        )


def load_listing_school_type(
    sync_ids,
    batch_ids,
    cursor_local,
    cursor_stage,
    db_connection_local,
    db_connection_stage,
):
    llst = """
    select distinct ls.school_category as mls_school_type,
    lst2.ylopo_school_type,
    case when lst2.active is true then true else false end as active,
    CURRENT_DATE as y_creation_date,
    CURRENT_DATE as y_last_update_date
    from stage.direct_idx_school ls
    left join idx_config.listing_school_type lst
    on ls.school_category=lst.mls_school_type
    left join idx_config.listing_school_type lst2
    on ls.school_category = lst2.mls_school_type
    where lst.id is null 
    and ls.school_category is not null 
    and ls.source_id in {0}
    """.format(str(sync_ids).replace("[", "(").replace("]", ")"))

    cursor_local.execute(llst)
    result = cursor_local.fetchall()
    if result:
        column_names = [desc[0] for desc in cursor_local.description]
        df = pd.DataFrame(result, columns=column_names)
        cols = ",".join(list(df.columns))

        insert_query = """
                        INSERT INTO idx_config.listing_school_type ({}) VALUES %s ON CONFLICT (id) DO NOTHING;
                        """.format(cols)
        query_execution_and_commit(
            insert_query, cursor_stage, db_connection_stage, result
        )

        query_execution_and_commit(
            insert_query, cursor_local, db_connection_local, result
        )


def temp_listing_rds(
    sync_ids,
    batch_ids,
    cursor_local,
    cursor_stage,
    db_connection_local,
    db_connection_stage,
):

    batch_id = str(batch_ids).replace("[", "(").replace("]", ")")
    source_id = str(sync_ids).replace("[", "(").replace("]", ")")

    # Build and execute a SQL query to retrieve update listing_ids
    update_listing_ids = "select target_listing_id  from stage.etl_direct_idx_update_listings where source_id in {0} and batch_id in {1}".format(
        source_id, batch_id
    )
    cursor_local.execute(update_listing_ids)
    update_listings = cursor_local.fetchall()
    update_listings = [i[0] for i in update_listings]
    log_msg = {"Status": "Got Lisiting from update Lisiting table"}
    logger.info(log_msg)

    # Build and execute a SQL query to retrieve delete listing_ids
    delete_listing_ids = "select target_listing_id  from stage.etl_direct_idx_delete_listings where source_id in {0}".format(
        source_id
    )
    cursor_stage.execute(delete_listing_ids)
    delete_listings = cursor_stage.fetchall()
    delete_listings = [i[0] for i in delete_listings]
    log_msg = {"Status": "Got Lisiting from delete Lisiting table"}
    logger.info(log_msg)

    # Merging the update_listings and delete_listings into a single list
    target_listing_ids = update_listings + delete_listings

    # Exit the function early if target_listing_ids is empty
    if not target_listing_ids:
        logger.info(
            {"Status": "No listings found, exiting the temp_listing_rds function"}
        )
        return

    target_listing_ids = str(target_listing_ids).replace("[", "(").replace("]", ")")
    log_msg = {"Status": "Merged update and delete listings into one list"}
    logger.info(log_msg)

    # RDS Temp listing insertion
    prev_deletion_listing = (
        """ delete from public.listing where source_id in {0}""".format(source_id)
    )
    cursor_local.execute(prev_deletion_listing)
    db_connection_local.commit()
    new_insertion_listing = """ select * from public.listing where source_id in {1} and id in {0};""".format(
        target_listing_ids, source_id
    )
    cursor_stage.execute(new_insertion_listing)
    result = cursor_stage.fetchall()
    column_names = [desc[0] for desc in cursor_stage.description]
    df = pd.DataFrame(result, columns=column_names)
    cols = ",".join(list(df.columns))
    insert_query = """
                    INSERT INTO public.listing ({0}) VALUES %s
                    """.format(cols)
    query_execution_and_commit(insert_query, cursor_local, db_connection_local, result)

    # Area Mappings RDS Sync
    prev_deletion_area_mapping = (
        """ delete from stage.area_mapping where source_id in {0}""".format(source_id)
    )
    cursor_local.execute(prev_deletion_area_mapping)
    db_connection_local.commit()
    new_insertion_area_mapping = (
        """ select * from stage.area_mapping where source_id in {0};""".format(
            source_id
        )
    )
    cursor_stage.execute(new_insertion_area_mapping)
    result = cursor_stage.fetchall()
    column_names = [desc[0] for desc in cursor_stage.description]
    df = pd.DataFrame(result, columns=column_names)
    cols = ",".join(list(df.columns))
    insert_query = """
                    INSERT INTO stage.area_mapping ({0}) VALUES %s
                    """.format(cols)
    query_execution_and_commit(insert_query, cursor_local, db_connection_local, result)

    # RDS Temp listing_photo insertion
    prev_deletion_photo = (
        """ delete from public.listing_photo where listing_id in {0};""".format(
            target_listing_ids
        )
    )
    cursor_local.execute(prev_deletion_photo)
    db_connection_local.commit()
    new_insertion_photo = (
        """ select * from public.listing_photo where listing_id in {0};""".format(
            target_listing_ids
        )
    )
    cursor_stage.execute(new_insertion_photo)
    result = cursor_stage.fetchall()
    column_names = [desc[0] for desc in cursor_stage.description]
    df = pd.DataFrame(result, columns=column_names)

    cols = ",".join(list(df.columns))
    insert_query = """
                    INSERT INTO public.listing_photo ({0}) VALUES %s
                    """.format(cols)

    query_execution_and_commit(insert_query, cursor_local, db_connection_local, result)

    # RDS Temp listing_openhouse insertion
    prev_deletion_openhouse = (
        """ delete from public.listing_openhouse where listing_id in {0};""".format(
            target_listing_ids
        )
    )
    cursor_local.execute(prev_deletion_openhouse)
    db_connection_local.commit()
    new_insertion_openhouse = (
        """ select * from public.listing_openhouse where listing_id in {0};""".format(
            target_listing_ids
        )
    )
    cursor_stage.execute(new_insertion_openhouse)
    result = cursor_stage.fetchall()
    column_names = [desc[0] for desc in cursor_stage.description]
    df = pd.DataFrame(result, columns=column_names)

    cols = ",".join(list(df.columns))
    insert_query = """
                    INSERT INTO public.listing_openhouse ({0}) VALUES %s
                    """.format(cols)
    query_execution_and_commit(insert_query, cursor_local, db_connection_local, result)


def update_listing(
    sync_ids,
    batch_ids,
    cursor_local,
    cursor_stage,
    db_connection_local,
    db_connection_stage,
):
    ul = """
    select DISTINCT ON (s.source_listing_id)
        s.source_id as source_id,
        l.target_listing_id,
        s.y_creation_date as y_last_update_date,
        s.batch_id as batch_id, 
        s.architecture_style,
        s.bathrooms,
        s.bedrooms,
        s.disclose_address::boolean,
        s.disclose_days_on_market::boolean,
        s.full_bathrooms,
        s.half_bathrooms,
        s.idx_contact_info,
        s.is_new_construction::boolean,
        s.lead_routing_email,
        s.listing_title,
        s.listing_url,
        s.living_area_sq_ft,
        s.media_modification_timestamp,
        s.mls_number,
        s.modification_timestamp,
        s.num_floors,
        s.num_parking_spaces,
        s.on_market_date,
        s.one_quarter_bathrooms,
        s.partial_bathrooms,
        s.photo_count,
        s.price,
        s.price_max,
        s.price_min,
        s.price_type,
        s.provider_category,
        s.provider_name,
        s.provider_url,
        s.room_count,
        s.source_last_update_date,
        s.source_listing_id,
        s.three_quarter_bathrooms,
        s.year_built,
        ls.id as listing_status_id, 
        --case when mr.expression is null then null else REGEXP_REPLACE(s.mls_number,mr.expression,'','g') end as mls_number_normalized,
        --CASE WHEN s.source_id = 996 THEN REGEXP_REPLACE(s.mls_number, '^R', 'RX-', 'g') WHEN s.source_id = 861 then REGEXP_REPLACE(REGEXP_REPLACE(s.mls_number, '^PWB', ''), '^(..)', '\\1-') WHEN mr.expression IS NULL THEN NULL   ELSE REGEXP_REPLACE(s.mls_number, mr.expression, '', 'g') END AS mls_number_normalized, archived at 22-01-2026
        CASE WHEN s.source_id = 996 THEN REGEXP_REPLACE(s.mls_number, '^R', 'RX-', 'g') WHEN s.source_id = 861 then REGEXP_REPLACE(REGEXP_REPLACE(s.mls_number, '^PWB', ''), '^(..)', '\\1-') WHEN mr.expression IS NULL THEN NULL   ELSE REGEXP_REPLACE(s.mls_number, mr.expression, coalesce(mr.expression_replace_with,''), 'g') END AS mls_number_normalized,
        lpst.id as Property_sub_type_id,
        lpt.id as property_type_id,
        lc.id as listing_category_id,
        mb.id as  mls_board_id,
        (CASE WHEN ls.ylopo_status = 'SOLD' THEN s.sold_date ELSE NULL END) as sold_date,
        (CASE WHEN ls.ylopo_status = 'SOLD' THEN s.sold_price ELSE NULL END) as sold_price,

    NULLIF(CASE
        WHEN lot_size IS NOT NULL AND lot_size_unit IS NOT NULL THEN lot_size
        WHEN lot_size_acres >= 1 THEN lot_size_acres
        WHEN lot_size_sqft IS NULL AND lot_size_acres IS NOT NULL AND lot_size_acres < 1 AND lot_size_acres > 0 THEN lot_size_acres
        WHEN lot_size_sqft IS NOT NULL AND lot_size_acres < 1 THEN lot_size_sqft
		 WHEN lot_size_sqft > 0 AND lot_size_acres is null THEN lot_size_sqft
        ELSE NULL
    END, 0.0) AS lot_size,
    
    CASE
        WHEN lot_size IS NOT NULL AND lot_size_unit IS NOT NULL THEN lot_size_unit
        WHEN lot_size_acres >= 1 THEN 'acres'
        WHEN lot_size_sqft IS NULL AND lot_size_acres IS NOT NULL AND lot_size_acres < 1 AND lot_size_acres > 0 THEN 'acres'
        WHEN lot_size_sqft IS NOT NULL AND lot_size_acres < 1 THEN 'sqft'
		WHEN lot_size_sqft > 0 AND lot_size_acres is null THEN 'sqft'
        ELSE NULL
    END AS lot_size_units,
	
    NULLIF(CASE
        WHEN lot_size_sqft IS NOT NULL THEN NULLIF(NULLIF(round(lot_size_sqft::numeric,2),'0.00'),'0')
        WHEN (lot_size_sqft IS NULL AND lot_size_acres IS NOT NULL AND lot_size_acres != 0) THEN NULLIF(NULLIF(round(lot_size_acres::numeric * 43560,2),'0.00'),'0')
        WHEN (lot_size IS NOT NULL AND lot_size != 0) THEN
            CASE
                WHEN lot_size_unit = 'sqft' THEN NULLIF(NULLIF(round(lot_size::numeric,2),'0.00'),'0')
				WHEN lot_size_unit = 'acres' THEN NULLIF(NULLIF(round(lot_size::numeric * 43560,2),'0.00'),'0')
                ELSE NULL
            END
        ELSE NULL
    END, 0.0) AS lot_size_sqft,
    
    NULLIF(CASE
        WHEN lot_size_acres IS NOT NULL THEN lot_size_acres
        WHEN (lot_size_acres IS NULL AND lot_size_sqft IS NOT NULL AND lot_size_sqft != 0) THEN round(lot_size_sqft::numeric / 43560,2)
        WHEN (lot_size IS NOT NULL AND lot_size != 0) THEN
            CASE
                WHEN lot_size_unit = 'acres' THEN lot_size
				WHEN lot_size_unit = 'sqft' THEN round(lot_size::numeric / 43560,2)
                ELSE NULL
            END
        ELSE NULL
    END, 0.0) AS lot_size_acres,
    s.disclose_map::boolean,
    s.disclose_price::boolean,
    s.idx_contact_info_office,
    s.source_mls_url

    from stage.DIRECT_idx_listing s
	    join listing_status ls
    on s.listing_status=ls.status
    and s.source_id=ls.source_id
    inner join 
        stage.etl_direct_idx_update_listings l
        on s.source_listing_id = l.source_listing_id
        and l.source_id = s.source_id
		and s.batch_id = l.batch_id


    inner JOIN
            public.listing_property_sub_type lpst ON
            s.source_id = lpst.source_id
            AND coalesce(s.property_sub_type, '') = coalesce(lpst.property_sub_type,'')
    inner JOIN
            public.listing_property_type lpt ON
            s.source_id = lpt.source_id
            AND coalesce(s.property_type, '') = coalesce(lpt.property_type,'')

    inner JOIN
            public.listing_category lc ON
            s.source_id = lc.source_id
            and (s.listing_category = lc.category or (s.listing_category is null and lc.category is null))

    inner JOIN
            public.mls_board mb ON
            s.source_id = mb.source_id
            AND s.mls = mb.mls_source_id
    left join 
        idx_config.listing_mls_number_regex mr
        on s.source_id = mr.source_Id

    where s.source_id in {0}
	and s.batch_id in {1}

    order by s.source_listing_id;

    """.format(
        str(sync_ids).replace("[", "(").replace("]", ")"),
        str(batch_ids).replace("[", "(").replace("]", ")"),
    )
    cursor_local.execute(ul)
    result = cursor_local.fetchall()
    update_query = f"""
    UPDATE public.listing AS pl
    SET y_last_update_date = data.y_last_update_date::timestamp,
    batch_id = data.batch_id,
    architecture_style = data.architecture_style,
    bathrooms = data.bathrooms::numeric,
    bedrooms = data.bedrooms::int,
    disclose_address = data.disclose_address,
    disclose_days_on_market = data.disclose_days_on_market::boolean,
    full_bathrooms = data.full_bathrooms::int,
    half_bathrooms = data.half_bathrooms::int,
    idx_contact_info = data.idx_contact_info,
    is_new_construction = data.is_new_construction::boolean,
    lead_routing_email = data.lead_routing_email,
    listing_title = data.listing_title,
    listing_url = data.listing_url,
    living_area_sq_ft = data.living_area_sq_ft::int,
    media_modification_timestamp = data.media_modification_timestamp::timestamp,
    mls_number = data.mls_number,
    modification_timestamp = data.modification_timestamp,
    num_floors = data.num_floors::numeric,
    num_parking_spaces = data.num_parking_spaces::int,
    on_market_date = data.on_market_date::date,
    one_quarter_bathrooms = data.one_quarter_bathrooms::int,
    partial_bathrooms = data.partial_bathrooms::int,
    photo_count = data.photo_count::int,
    price = data.price::numeric,
    price_max = data.price_max::numeric,
    price_min = data.price_min::numeric,
    price_type = data.price_type,
    provider_category = data.provider_category,
    provider_name = data.provider_name,
    provider_url = data.provider_url,
    room_count = data.room_count::int,
    source_last_update_date = data.source_last_update_date,
    source_listing_id = data.source_listing_id,
    three_quarter_bathrooms = data.three_quarter_bathrooms::int,
    year_built = data.year_built::int,
    listing_status_id = data.listing_status_id,
    mls_number_normalized = data.mls_number_normalized,
    Property_sub_type_id = data.Property_sub_type_id,
    property_type_id = data.property_type_id,
    listing_category_id = data.listing_category_id,
    mls_board_id = data.mls_board_id,
    sold_date = data.sold_date::date,
    sold_price = data.sold_price::numeric,
    lot_size = data.lot_size::numeric,
    lot_size_units = data.lot_size_units,
    lot_size_sqft = data.lot_size_sqft::numeric,
    lot_size_acres = data.lot_size_acres::numeric,
    disclose_map = data.disclose_map::boolean,
    disclose_price = data.disclose_price::boolean,
    idx_contact_info_office = data.idx_contact_info_office::text,
    source_mls_url = data.source_mls_url::text
    
    FROM (VALUES %s) AS data (source_id, target_listing_id,y_last_update_date,batch_id,architecture_style,bathrooms,bedrooms,disclose_address,disclose_days_on_market,full_bathrooms,half_bathrooms,idx_contact_info,is_new_construction,lead_routing_email,
                             listing_title,listing_url,living_area_sq_ft,media_modification_timestamp,mls_number,modification_timestamp,num_floors,num_parking_spaces,on_market_date,one_quarter_bathrooms,partial_bathrooms,photo_count,
                             price,price_max,price_min,price_type,provider_category,provider_name,provider_url,room_count,source_last_update_date,source_listing_id,three_quarter_bathrooms,year_built,listing_status_id,mls_number_normalized,
                             Property_sub_type_id,property_type_id,listing_category_id,mls_board_id,sold_date,sold_price,lot_size,lot_size_units,lot_size_sqft,lot_size_acres,disclose_map,disclose_price,idx_contact_info_office,source_mls_url)
    
    
    WHERE pl.id = data.target_listing_id
    and pl.source_id = data.source_id
    """
    extras.execute_values(cursor_stage, update_query, result)
    db_connection_stage.commit()


def update_description(
    sync_ids,
    batch_ids,
    cursor_local,
    cursor_stage,
    db_connection_local,
    db_connection_stage,
):

    del_ids = """
    select target_listing_id from stage.etl_direct_idx_update_listings where batch_id in {1} and source_id in {0};
    """.format(
        str(sync_ids).replace("[", "(").replace("]", ")"),
        str(batch_ids).replace("[", "(").replace("]", ")"),
    )
    cursor_local.execute(del_ids)
    del_ids = cursor_local.fetchall()
    del_ids = [t[0] for t in del_ids]

    if del_ids:
        del_description = (
            """ delete from public.listing_description where listing_id in {}""".format(
                str(del_ids).replace("[", "(").replace("]", ")")
            )
        )
        cursor_stage.execute(del_description)
        db_connection_stage.commit()

    ud = """
    select
    s.key_name,
    s.key_value,
    s.source_creation_date,
    s.source_last_update_date,
    s.batch_id as batch_id,
    s.y_creation_date as y_creation_date,
    s.y_creation_date as y_last_update_date,
    t.target_listing_id as listing_id   
    
    from stage.DIRECT_idx_description s 
    join stage.etl_direct_idx_update_listings t
    
    on s.source_listing_id=t.source_listing_id
    and s.batch_id = t.batch_id
    
    where s.batch_id in {1}
    and s.source_id in {0}""".format(
        str(sync_ids).replace("[", "(").replace("]", ")"),
        str(batch_ids).replace("[", "(").replace("]", ")"),
    )

    cursor_local.execute(ud)
    result = cursor_local.fetchall()
    column_names = [desc[0] for desc in cursor_local.description]
    df = pd.DataFrame(result, columns=column_names)
    df["key_value"] = df["key_value"].apply(decode_url)
    cols = ",".join(list(df.columns))
    insert_query = """
        INSERT INTO public.listing_description ({}) VALUES %s
        """.format(cols)
    extras.execute_values(cursor_stage, insert_query, result)
    db_connection_stage.commit()


def update_photo(
    sync_ids,
    batch_ids,
    cursor_local,
    cursor_stage,
    db_connection_local,
    db_connection_stage,
):
    del_inner = """ select target_listing_id from stage.etl_direct_idx_update_listings t join stage.direct_idx_listing s on t.source_listing_id = s.source_listing_id
    and s.batch_id = t.batch_id where s.source_id in {0} and s.batch_id in {1} and (s.media_modification_timestamp > t.media_modification_timestamp or t.media_modification_timestamp is null or s.media_modification_timestamp is null)""".format(
        str(sync_ids).replace("[", "(").replace("]", ")"),
        str(batch_ids).replace("[", "(").replace("]", ")"),
    )
    cursor_local.execute(del_inner)
    del_ids = cursor_local.fetchall()
    del_ids = [t[0] for t in del_ids]

    if del_ids:
        del_photos = """delete from listing_photo where listing_id in {};""".format(
            str(del_ids).replace("[", "(").replace("]", ")")
        )
        cursor_stage.execute(del_photos)
        db_connection_stage.commit()

        load_photos = """
        SELECT
            s.source_creation_date,
            s.source_last_update_date,
            s.media_modification_timestamp,
            s.media_url,
            t.target_listing_id AS listing_id,
            s.y_creation_date AS y_creation_date,
            s.batch_id AS batch_id,
            s.y_creation_date AS y_last_update_date
        FROM
            stage.DIRECT_idx_photo s
        JOIN
            stage.etl_direct_idx_update_listings t
        ON
            s.source_listing_id = t.source_listing_id and
            s.source_id=t.source_id
        WHERE
            s.source_id in {0} and t.target_listing_id is not null
            and t.target_listing_id in {1}
        ORDER BY
            s.source_listing_id,s.id; 
            """.format(
            str(sync_ids).replace("[", "(").replace("]", ")"),
            str(del_ids).replace("[", "(").replace("]", ")"),
            str(batch_ids).replace("[", "(").replace("]", ")"),
        )

        cursor_local.execute(load_photos)
        result = cursor_local.fetchall()
        column_names = [desc[0] for desc in cursor_local.description]
        df = pd.DataFrame(result, columns=column_names)
        cols = ",".join(list(df.columns))
        insert_query = """
            INSERT INTO public.listing_photo ({}) VALUES %s
            """.format(cols)
        extras.execute_values(cursor_stage, insert_query, result)
        db_connection_stage.commit()


def update_mlsgrid_photo(
    sync_ids,
    batch_ids,
    cursor_local,
    cursor_stage,
    db_connection_local,
    db_connection_stage,
):
    mlsgrid_etl_photos_temp_history_query = """INSERT INTO stage.etl_action_mlsgrid_photos_history (source_id,batch_id,listing_id,source_listing_id,source_creation_date,source_last_update_date,y_creation_date,y_last_update_date,media_modification_timestamp,media_url,photo_order,photo_id,url_flag)
                            SELECT source_id,batch_id,listing_id,source_listing_id,source_creation_date,source_last_update_date,y_creation_date,y_last_update_date,media_modification_timestamp,media_url,photo_order,photo_id,url_flag  FROM stage.etl_action_mlsgrid_photos
                            WHERE source_id in {0} and batch_id in {1} and url_flag ~* 'D'
							ORDER BY source_listing_id,photo_order;""".format(
        str(sync_ids).replace("[", "(").replace("]", ")"),
        str(batch_ids).replace("[", "(").replace("]", ")"),
    )
    cursor_local.execute(mlsgrid_etl_photos_temp_history_query)
    db_connection_local.commit()

    del_inner = """ select photo_id 
                    from stage.etl_action_mlsgrid_photos s 
                    where s.source_id in {0} and s.batch_id in {1} 
                    and photo_id is not null and url_flag ~* 'D';""".format(
        str(sync_ids).replace("[", "(").replace("]", ")"),
        str(batch_ids).replace("[", "(").replace("]", ")"),
    )
    cursor_local.execute(del_inner)
    del_ids = cursor_local.fetchall()
    del_ids = [t[0] for t in del_ids]

    if del_ids:
        del_photos = """delete from listing_photo where id in {};""".format(
            str(del_ids).replace("[", "(").replace("]", ")")
        )
        cursor_stage.execute(del_photos)
        db_connection_stage.commit()

    load_photos = """
    SELECT
        s.source_creation_date,
        s.source_last_update_date,
        s.media_modification_timestamp,
        s.media_url,
        s.listing_id,
        s.y_creation_date,
        s.batch_id,
        s.y_last_update_date
    FROM
        stage.etl_action_mlsgrid_photos s
    JOIN
        stage.etl_direct_idx_update_listings t
    ON
        s.source_listing_id = t.source_listing_id and
        s.source_id=t.source_id
    WHERE
        s.source_id in {0} and s.url_flag ~* 'I'
    ORDER BY
        s.source_listing_id,s.id;
        """.format(
        str(sync_ids).replace("[", "(").replace("]", ")"),
        str(batch_ids).replace("[", "(").replace("]", ")"),
    )

    cursor_local.execute(load_photos)
    result = cursor_local.fetchall()
    column_names = [desc[0] for desc in cursor_local.description]
    df = pd.DataFrame(result, columns=column_names)
    cols = ",".join(list(df.columns))
    insert_query = """
        INSERT INTO public.listing_photo ({}) VALUES %s
        """.format(cols)
    extras.execute_values(cursor_stage, insert_query, result)
    db_connection_stage.commit()


def update_listing_prefetch_photo(
    sync_ids,
    batch_ids,
    cursor_local,
    cursor_stage,
    db_connection_local,
    db_connection_stage,
):

    first_query = """
    Select 
	photo_prefetch, 
	photo_prefetch_info,
    (photo_prefetch_info->>'photo_url') AS photo_url,
    (photo_prefetch_info->>'use_main_photo')::BOOLEAN AS use_main_photo,
    (photo_prefetch_info->>'prefetch_method') AS prefetch_method,
    (photo_prefetch_info->>'wildcard_support')::BOOLEAN AS wildcard_support

    from source where id in {0};
    """.format(str(sync_ids).replace("[", "(").replace("]", ")"))
    cursor_stage.execute(first_query)
    result_1 = cursor_stage.fetchall()
    photo_prefetch = result_1[0][0]
    prefetch_method = result_1[0][4]
    photo_url = result_1[0][2]
    use_main_photo = result_1[0][3]

    if photo_prefetch is False:
        pass
    else:
        if prefetch_method == "WILDCARD":
            is_wild_card = """
            select 
            h.batch_id,
    		h.source_id,
    		t.target_listing_id as listing_id,
    		h.y_creation_date as creation_date,
    		h.y_creation_date as last_update_date,
    		ROW_NUMBER() over(order by h.id) as process_seq,
    		CASE 
            WHEN photo_count = 1 THEN	
            replace(replace('{2}','[source_listing_id]', h.source_listing_id::text),'[photo_num]', '1') 
            WHEN photo_count > 1 THEN
    		replace(replace('{2}','[source_listing_id]', h.source_listing_id::text),'[photo_num]', '*') 
            end as media_url,
    		{3} as main_photo

            from (
                select
                batch_id, source_id, source_listing_id, target_listing_id
                from stage.etl_direct_idx_update_listings 
                where source_id in {0}
                union
                select
                batch_id, source_id, source_listing_id, target_listing_id 
                from stage.etl_direct_idx_insert_listings ediil 
                where source_id in {0} and target_listing_id is not null
            ) t

            join stage.direct_idx_listing h
            on h.batch_id = t.batch_id 
            and h.source_listing_id = t.source_listing_id
            where  h.source_id in {0}  and h.batch_id in {1} 
            and h.photo_count>0 and h.photo_count is not null;
            """.format(
                str(sync_ids).replace("[", "(").replace("]", ")"),
                str(batch_ids).replace("[", "(").replace("]", ")"),
                photo_url,
                use_main_photo,
            )

            cursor_local.execute(is_wild_card)
            result = cursor_local.fetchall()
            column_names = [desc[0] for desc in cursor_local.description]
            df = pd.DataFrame(result, columns=column_names)
            cols = ",".join(list(df.columns))
            insert_query = """
                INSERT INTO public.listing_photo_prefetch ({}) VALUES %s
                """.format(cols)
            extras.execute_values(cursor_stage, insert_query, result)
            insert_query_stage = """
                INSERT INTO stage.listing_photo_prefetcher_archived ({}) VALUES %s
                """.format(cols)
            # we are disabling this for temporary for now untill we get the permenetly deleting permission from the Solution team
            # if 258 in sync_ids:
            #     extras.execute_values(cursor_stage, insert_query_stage, result)
            db_connection_stage.commit()
        else:
            # For non-wildcard support, we will fetch the first 3 photos for each listing
            no_wild_card = """
            select
                s.batch_id
            , s.source_id
            , s.listing_photo_id
            , s.listing_id
            , s.media_url
            , s.creation_date
            , s.last_update_date
            , s.process_seq
            from ( select 
                t.batch_id 
                , t.source_id
                , s.id as listing_photo_id
                , t.id as listing_id
                , s.media_url
                , s.y_creation_date as creation_date
                , s.y_creation_date as last_update_date
                , ROW_NUMBER() over(order by s.id) as process_seq
                , ROW_NUMBER() over(partition by t.id order by t.id) as photo_seq
            from public.listing t
            join public.listing_photo s 
            on s.listing_id=t.id
            and s.batch_id = t.batch_id
            where t.batch_id in {1} and t.source_id in {0} 
            ) as s
            where s.media_url is not null and s.photo_seq <= 3;	            
            """.format(
                str(sync_ids).replace("[", "(").replace("]", ")"),
                str(batch_ids).replace("[", "(").replace("]", ")"),
            )

            cursor_stage.execute(no_wild_card)
            result = cursor_stage.fetchall()
            column_names = [desc[0] for desc in cursor_stage.description]
            df = pd.DataFrame(result, columns=column_names)
            cols = ",".join(list(df.columns))
            insert_query = """
                INSERT INTO public.listing_photo_prefetch ({}) VALUES %s
                """.format(cols)
            extras.execute_values(cursor_stage, insert_query, result)
            db_connection_stage.commit()


def update_school(
    sync_ids,
    batch_ids,
    cursor_local,
    cursor_stage,
    db_connection_local,
    db_connection_stage,
):
    del_inner = """ select target_listing_id from stage.etl_direct_idx_update_listings 
                where batch_id in {1} and source_id in {0}""".format(
        str(sync_ids).replace("[", "(").replace("]", ")"),
        str(batch_ids).replace("[", "(").replace("]", ")"),
    )
    cursor_local.execute(del_inner)
    del_ids = cursor_local.fetchall()
    del_ids = [t[0] for t in del_ids]

    if del_ids:
        del_schools = """delete from listing_school where listing_id in {};""".format(
            str(del_ids).replace("[", "(").replace("]", ")")
        )
        cursor_stage.execute(del_schools)
        db_connection_stage.commit()

    load_schools = """ select 
                S.y_last_update_date,
                S.y_creation_date,
                S.source_last_update_date,
                S.source_id,
                S.source_creation_date,
                S.school_category as school_type,
                S.school_name as name,
                S.school_category as mls_school_type,
                S.school_district as district,
                S.batch_id,
                 t.target_listing_id AS listing_id
                
                
                from stage.DIRECT_idx_school s 
                join stage.etl_direct_idx_update_listings t
                
                on s.source_listing_id=t.source_listing_id
                and s.batch_id = t.batch_id
                where s.batch_id in {1}
                and s.source_id in {0}
                and s.school_name is not null;""".format(
        str(sync_ids).replace("[", "(").replace("]", ")"),
        str(batch_ids).replace("[", "(").replace("]", ")"),
    )

    cursor_local.execute(load_schools)
    result = cursor_local.fetchall()
    column_names = [desc[0] for desc in cursor_local.description]
    df = pd.DataFrame(result, columns=column_names)
    cols = ",".join(list(df.columns))
    insert_query = """
        INSERT INTO public.listing_school ({}) VALUES %s
        """.format(cols)
    extras.execute_values(cursor_stage, insert_query, result)
    db_connection_stage.commit()


def update_school_district(
    sync_ids,
    batch_ids,
    cursor_local,
    cursor_stage,
    db_connection_local,
    db_connection_stage,
):
    del_inner = """ select target_listing_id from stage.etl_direct_idx_update_listings 
                where batch_id in {1} and source_id in {0}""".format(
        str(sync_ids).replace("[", "(").replace("]", ")"),
        str(batch_ids).replace("[", "(").replace("]", ")"),
    )
    cursor_local.execute(del_inner)
    del_ids = cursor_local.fetchall()
    del_ids = [t[0] for t in del_ids]

    if del_ids:
        del_schools = (
            """delete from listing_school_district where listing_id in {};""".format(
                str(del_ids).replace("[", "(").replace("]", ")")
            )
        )
        cursor_stage.execute(del_schools)
        db_connection_stage.commit()

    school_district_query = """
    select DISTINCT ON (s.source_listing_id)
    	s.source_id as source_id
    	, t.target_listing_id as listing_id
    	, s.batch_id as batch_id
    	, s.school_district as district
    	, s.y_creation_date as y_creation_date
    	, s.y_creation_date as y_last_update_date
    	, s.source_creation_date as source_creation_date
    	, s.source_last_update_date

    from stage.DIRECT_idx_school s 
    join stage.etl_direct_idx_update_listings t

    on s.source_listing_id=t.source_listing_id
    and s.batch_id = t.batch_id
    where  s.source_id in {}  and s.school_district is not null and t.target_listing_id is not null order by s.source_listing_id;
    """.format(str(sync_ids).replace("[", "(").replace("]", ")"))
    cursor_local.execute(school_district_query)
    result = cursor_local.fetchall()
    column_names = [desc[0] for desc in cursor_local.description]
    df = pd.DataFrame(result, columns=column_names)
    cols = ",".join(list(df.columns))
    insert_query = """INSERT INTO public.listing_school_district ({}) VALUES %s
                                """.format(cols)
    extras.execute_values(cursor_stage, insert_query, result)


def update_attributes(
    sync_ids,
    batch_ids,
    cursor_local,
    cursor_stage,
    db_connection_local,
    db_connection_stage,
):
    del_query = """ select target_listing_id from stage.etl_direct_idx_update_listings where batch_id in {1} and source_id in {0}""".format(
        str(sync_ids).replace("[", "(").replace("]", ")"),
        str(batch_ids).replace("[", "(").replace("]", ")"),
    )
    cursor_local.execute(del_query)
    del_ids = cursor_local.fetchall()
    del_ids = [t[0] for t in del_ids]

    for table_name, query in attribute_query_dict.items():
        query = query.replace(
            "etl_direct_idx_insert_listings", "etl_direct_idx_update_listings"
        ).replace("t.y_creation_date", "s.y_creation_date")
        query = query.format(str(sync_ids).replace("[", "(").replace("]", ")"))
        if del_ids:
            attribute_deletion = """ delete from {0} where listing_id in {1}""".format(
                table_name, str(del_ids).replace("[", "(").replace("]", ")")
            )
            cursor_stage.execute(attribute_deletion)
            db_connection_stage.commit()

        cursor_local.execute(query)
        result = cursor_local.fetchall()
        column_names = [desc[0] for desc in cursor_local.description]
        df = pd.DataFrame(result, columns=column_names)
        df = attributes_cleansing(df)
        records = [tuple(row) for row in df.values]
        updated_data = [
            tuple(
                None if isinstance(val, float) and pd.isna(val) else val for val in tup
            )
            for tup in records
        ]
        cols = ",".join(list(df.columns))
        insert_query = """INSERT INTO public.{} ({}) VALUES %s
                                                      """.format(table_name, cols)

        extras.execute_values(cursor_stage, insert_query, updated_data)
        db_connection_stage.commit()


def update_address(
    sync_ids,
    batch_ids,
    cursor_local,
    cursor_stage,
    db_connection_local,
    db_connection_stage,
):
    address_updation = """
    select 
    t.target_listing_id,
    s.city,
    s.county,
    s.full_street_address,
    s.y_creation_date as y_last_update_date,                  
    s.source_last_update_date,
    s.zoning, 
    s.mls_latitude,
    s.mls_longitude,
    s.postal_code,
    s.unit_number, 
    s.state_or_province,
    s.parcel_number as parcel_id,
    s.batch_id as batch_id,
    s.country,                            
    UPPER(s.community_name) as community_name,
    s.region,
    s.zone,
    UPPER(s.subdivision_name) as subdivision_name,
    UPPER(s.district_name) as district_name,
    UPPER(s.mls_area_name) as mls_area_name,
    UPPER(s.custom_area_name_1) as custom_area_name_1,
    UPPER(s.custom_area_name_2) as custom_area_name_2,
    UPPER(s.custom_area_name_3) as custom_area_name_3,
    UPPER(s.sub_district_name) as sub_district_name,
    UPPER(TRIM(BOTH ' ' FROM CONCAT(REGEXP_REPLACE(REGEXP_REPLACE(s.full_street_address, '[aeiouAEIOU ]', '','g'), '[^a-zA-Z\d\s:]', '','g'),REGEXP_REPLACE(REGEXP_REPLACE(s.city, '[aeiouAEIOU ]', '','g'), '[^a-zA-Z\d\s:]', '','g'),REGEXP_REPLACE(REGEXP_REPLACE(s.postal_code, '[aeiouAEIOU ]', '','g'), '[^a-zA-Z\d\s:]', '','g')))) AS address_token
        
	from stage.direct_idx_address s
        join stage.etl_direct_idx_update_listings t
              on s.source_listing_id=t.source_listing_id
                   and s.source_id = t.source_id
    where s.source_id in {0} and s.batch_id in {1}
    order by t.target_listing_id;
    """.format(
        str(sync_ids).replace("[", "(").replace("]", ")"),
        str(batch_ids).replace("[", "(").replace("]", ")"),
    )
    cursor_local.execute(address_updation)
    result = cursor_local.fetchall()

    update_query = f"""
    UPDATE public.listing_address AS pl
    SET 
	city = data.city, county = data.county, full_street_address = data.full_street_address, y_last_update_date = data.y_last_update_date::timestamp, source_last_update_date = data.source_last_update_date::date,
	zoning = data.zoning, mls_latitude = data.mls_latitude, mls_longitude = data.mls_longitude, postal_code = data.postal_code, unit_number = data.unit_number,
	state_or_province = data.state_or_province, parcel_id = data.parcel_id, batch_id = data.batch_id,country = data.country, community_name = data.community_name,
	region = data.region, zone = data.zone,subdivision_name = data.subdivision_name,district_name = data.district_name, mls_area_name = data.mls_area_name,
	custom_area_name_1 = data.custom_area_name_1, custom_area_name_2 = data.custom_area_name_2, custom_area_name_3 = data.custom_area_name_3, sub_district_name = data.sub_district_name,
	address_token = data.address_token
	
    
    FROM (VALUES %s) AS data (target_listing_id,city,county,full_street_address,y_last_update_date,source_last_update_date,zoning,mls_latitude,mls_longitude,
                              postal_code,unit_number,state_or_province,parcel_id,batch_id,country,community_name,region,zone,subdivision_name,
                              district_name,mls_area_name,custom_area_name_1,custom_area_name_2,custom_area_name_3,sub_district_name,address_token)
    
    
    WHERE pl.listing_id = data.target_listing_id
    """
    extras.execute_values(cursor_stage, update_query, result)
    db_connection_stage.commit()


def update_address_standard(
    sync_ids,
    batch_ids,
    cursor_local,
    cursor_stage,
    db_connection_local,
    db_connection_stage,
):
    uas = """
    select
    la.community_name,
    la.subdivision_name,
    la.listing_id,
    la.source_id 
    from listing_address la
    join listing_address_standard las
    on la.source_id = las.source_id
    and la.listing_id = las.listing_id
    where la.source_id in {0}
    and la.batch_id in {1};
    """.format(
        str(sync_ids).replace("[", "(").replace("]", ")"),
        str(batch_ids).replace("[", "(").replace("]", ")"),
    )
    cursor_stage.execute(uas)
    result = cursor_stage.fetchall()

    update_query = f"""
    UPDATE public.listing_address_standard AS pl
    SET community_name = data.community_name,
        subdivision_name = data.subdivision_name

    FROM (VALUES %s) AS data (community_name,subdivision_name,listing_id,source_id)
    
    
    WHERE pl.listing_id = data.listing_id
    AND pl.source_id = data.source_id
    """
    extras.execute_values(cursor_stage, update_query, result)
    db_connection_stage.commit()


def update_openhouse(
    sync_ids,
    batch_ids,
    cursor_local,
    cursor_stage,
    db_connection_local,
    db_connection_stage,
):
    oh = """
	select DISTINCT ON (s.source_listing_id, s.date, s.start_time)
	s.batch_id,
	s.source_date,
	case when s.y_creation_date>='2023-11-14 17:00:00' and s.source_Id not in (839,847,540,825,857) then concat(s.start_time - INTERVAL '1 HOURS',' - ', s.end_time - INTERVAL '1 HOURS') else s.source_time end as source_time,
	s.date,
	case when s.y_creation_date>='2023-11-14 17:00:00' and s.source_Id not in (839,847,540,825,857) then s.start_time - INTERVAL '1 HOURS' else s.start_time end,
	case when s.y_creation_date>='2023-11-14 17:00:00' and s.source_Id not in (839,847,540,825,857) then s.end_time - INTERVAL '1 HOURS' else s.end_time end,
	s.source_last_update_date,
	s.y_last_update_date,
	s.virtual_tour_url,
	s.ylopo_action,
	t.target_listing_id,
	s.openhouse_type

	from stage.DIRECT_idx_openhouse s 
	join stage.etl_direct_idx_update_listings t
	on s.source_listing_id=t.source_listing_id
	and s.batch_id = t.batch_id
	where s.source_date is not null
	and s.batch_id in {1}
	and s.source_id in {0}
	ORDER BY s.source_listing_id, s.date DESC, s.start_time DESC;
    """.format(
        str(sync_ids).replace("[", "(").replace("]", ")"),
        str(batch_ids).replace("[", "(").replace("]", ")"),
    )
    cursor_local.execute(oh)
    result = cursor_local.fetchall()

    update_query = f"""
    UPDATE public.listing_openhouse AS pl
    SET 
	batch_id=data.batch_id,
	source_date=data.source_date,
	source_time=data.source_time,
	date=data.date,
	start_time=data.start_time,
	end_time=data.end_time,
	source_last_update_date=data.source_last_update_date,
	y_last_update_date=data.y_last_update_date::timestamp,
	virtual_tour_url=data.virtual_tour_url,
	ylopo_action=data.ylopo_action,
	openhouse_type=data.openhouse_type
    
    FROM (VALUES %s) AS data (batch_id,source_date,source_time,date,start_time,end_time,source_last_update_date,y_last_update_date,virtual_tour_url,ylopo_action,target_listing_id,openhouse_type)	
    
    
    WHERE pl.listing_id = data.target_listing_id
	and pl.date=data.date
	and pl.start_time=data.start_time
	and pl.openhouse_type=data.openhouse_type
    """
    extras.execute_values(cursor_stage, update_query, result)
    db_connection_stage.commit()


def update_openhouse_new(
    sync_ids,
    batch_ids,
    cursor_local,
    cursor_stage,
    db_connection_local,
    db_connection_stage,
):
    ohn = """
	SELECT DISTINCT ON (o.source_listing_id, o.date, o.start_time)
	l.batch_id,
	o.source_date,
	case when o.y_creation_date>='2023-11-14 17:00:00' and o.source_Id not in (839,847,540,825,857) then concat(o.start_time - INTERVAL '1 HOURS',' - ', o.end_time - INTERVAL '1 HOURS') else o.source_time end as source_time,
	o.date,
	case when o.y_creation_date>='2023-11-14 17:00:00' and o.source_Id not in (839,847,540,825,857) then o.start_time - INTERVAL '1 HOURS' else o.start_time end,
	case when o.y_creation_date>='2023-11-14 17:00:00' and o.source_Id not in (839,847,540,825,857) then o.end_time - INTERVAL '1 HOURS' else o.end_time end,
	case when o.source_last_update_date is null then l.source_last_update_date else o.source_last_update_date end source_last_update_date,
	o.y_last_update_date,
	l.id as listing_id,
	o.contact_name,
	o.contact_phone,
	upper(coalesce(o.openhouse_type,'IN-PERSON')) as openhouse_type,
	o.virtual_tour_url,
	o.ylopo_action
	
	
	from listing l
	join stage.direct_idx_openhouse o on 
	l.source_listing_id=o.source_listing_id
	and l.source_id=o.source_id
	where o.source_date is not null
	and o.source_id in {0}
	and l.source_id in {0}
	ORDER BY o.source_listing_id, o.date DESC, o.start_time DESC;


    """.format(str(sync_ids).replace("[", "(").replace("]", ")"))
    cursor_local.execute(ohn)
    result = cursor_local.fetchall()

    update_query = f"""
    UPDATE public.listing_openhouse AS pl
    SET 
	batch_id=data.batch_id,
	source_date=data.source_date,
	source_time=data.source_time,
	date=data.date,
	start_time=data.start_time::time,
	end_time=data.end_time,
	source_last_update_date=data.source_last_update_date,
	y_last_update_date=data.y_last_update_date::timestamp,
	contact_name=data.contact_name,
	contact_phone=data.contact_phone,
	openhouse_type=data.openhouse_type,
	virtual_tour_url=data.virtual_tour_url,
	ylopo_action=data.ylopo_action
    
    FROM (VALUES %s) AS data (batch_id,source_date,source_time,date,start_time,end_time,source_last_update_date,y_last_update_date,listing_id,contact_name,contact_phone,openhouse_type,virtual_tour_url,ylopo_action)	
    
    
    WHERE pl.listing_id = data.listing_id
	and pl.date=data.date
	and pl.start_time=data.start_time
	and pl.openhouse_type=data.openhouse_type
    """
    extras.execute_values(cursor_stage, update_query, result)
    db_connection_stage.commit()


def openhouse_rds_to_homelistings(
    sync_ids, cursor_local, db_connection_local, cursor_stage, db_connection_stage
):
    try:
        query = (
            """Delete from stage.direct_idx_openhouse where source_id in {0}""".format(
                str(sync_ids).replace("[", "(").replace("]", ")")
            )
        )
        cursor_stage.execute(query)
        db_connection_stage.commit()

        query = """select * from stage.direct_idx_openhouse s where s.source_Id in {0}""".format(
            str(sync_ids).replace("[", "(").replace("]", ")")
        )
        cursor_local.execute(query)
        result = cursor_local.fetchall()
        column_names = [desc[0] for desc in cursor_local.description]
        df = pd.DataFrame(result, columns=column_names)

        column_to_remove = (
            "id"  # Replace with the actual column name you want to remove
        )
        if column_to_remove in df.columns:
            df = df.drop(columns=[column_to_remove])

        cols = ",".join(list(df.columns))
        data_values = [tuple(row) for row in df.values]
        insert_query = (
            """INSERT INTO stage.direct_idx_openhouse ({}) VALUES %s""".format(cols)
        )
        extras.execute_values(cursor_stage, insert_query, data_values)
        db_connection_stage.commit()

        query_delete = """Delete from listing_Openhouse where date < current_date and id in (
                select O.id from stage.direct_idx_Openhouse s 
                join listing_p_active l  on s.source_id = l.source_id and s.source_Listing_Id=l.source_listing_Id
                join listing_Openhouse o on l.id=o.listing_Id
                where s.source_id = {0} and l.source_id = {0}
        )""".format(
            str(sync_ids).replace("[", "(").replace("]", ")")
        )
        cursor_stage.execute(query_delete)
        db_connection_stage.commit()
    except Exception as e:
        logger.error(f"Error in data_download: {e}")
        raise


def openhouse_all_1(sync_ids, cursor_stage, db_connection_stage):
    open_house_2 = """
    select DISTINCT ON (s.source_listing_id, s.date, s.start_time)
    t.id as listing_id
    ,t.y_creation_date as y_creation_date
    ,t.y_last_update_date as y_last_update_date
    ,upper(coalesce(s.openhouse_type,'IN-PERSON')) as openhouse_type
    ,s.source_creation_date
    ,s.source_last_update_date
    ,s.source_date
    ,s.source_time
    ,s.contact_name
    ,s.contact_phone
    ,s.date
    ,s.start_time
    ,s.end_time
    ,s.virtual_tour_url
    ,s.ylopo_action
    ,t.batch_id as batch_id     
    from stage.DIRECT_idx_openhouse s 
    join public.listing t   
    on s.source_listing_id=t.source_listing_id and s.source_id=t.source_id
    where s.source_date is not null
    and s.source_id in {0} and t.source_id in {0}
    ORDER BY s.source_listing_id, s.date DESC, s.start_time DESC;
    """.format(str(sync_ids).replace("[", "(").replace("]", ")"))
    cursor_stage.execute(open_house_2)
    result = cursor_stage.fetchall()
    column_names = [desc[0] for desc in cursor_stage.description]
    df = pd.DataFrame(result, columns=column_names)
    df = df.drop_duplicates(
        subset=["listing_id", "date", "start_time", "end_time"], keep="first"
    )
    cols = ",".join(list(df.columns))
    insert_query = """INSERT INTO public.listing_openhouse ({}) VALUES %s""".format(
        cols
    )
    extras.execute_values(cursor_stage, insert_query, result)
    db_connection_stage.commit()


def openhouse_all(sync_ids, cursor_stage, db_connection_stage):

    id = str(sync_ids).replace("[", "").replace("]", "")
    open_house_2 = """
    SELECT DISTINCT ON (s.source_listing_id, s.date, s.start_time)
        t.id AS listing_id,
        t.y_creation_date AS y_creation_date,
        t.y_last_update_date AS y_last_update_date,
        UPPER(COALESCE(s.openhouse_type, 'IN-PERSON')) AS openhouse_type,
        s.source_creation_date,
        s.source_last_update_date,
        s.source_date,
        s.source_time,
        s.contact_name,
        s.contact_phone,
        s.date,
        s.start_time,
        s.end_time,
        s.virtual_tour_url,
        s.ylopo_action,
        s.batch_id AS batch_id, 
        s.is_inactive
    FROM stage.DIRECT_idx_openhouse s
    JOIN public.listing t
        ON s.source_listing_id = t.source_listing_id
        AND s.source_id = t.source_id
    WHERE s.source_date IS NOT NULL
      AND s.source_id IN {0}
      AND t.source_id IN {0}
    ORDER BY s.source_listing_id, s.date DESC, s.start_time DESC;
    """.format(str(sync_ids).replace("[", "(").replace("]", ")"))

    cursor_stage.execute(open_house_2)
    result = cursor_stage.fetchall()
    column_names = [desc[0] for desc in cursor_stage.description]
    df = pd.DataFrame(result, columns=column_names)
    temp_table = f"temp_openhouse_{id}"

    # Step 1: Create a temporary table
    cursor_stage.execute(f"""
    CREATE TEMP TABLE {temp_table} AS 
    SELECT * FROM public.listing_openhouse LIMIT 0;
    """)
    extras.execute_values(
        cursor_stage,
        f"INSERT INTO {temp_table} ({','.join(column_names)}) VALUES %s",
        result,
    )

    # Step 2: Delete existing Openhouse records from homelistings
    cursor_stage.execute(f"""
    DELETE FROM public.listing_openhouse lo
    USING {temp_table} t
    WHERE lo.listing_id = t.listing_id;
    """)

    # Step 2: Update existing rows
    # update_query = f"""
    # UPDATE public.listing_openhouse AS t
    # SET
    #     y_creation_date = s.y_creation_date,
    #     y_last_update_date = s.y_last_update_date,
    #     source_creation_date = s.source_creation_date,
    #     source_last_update_date = s.source_last_update_date,
    #     source_date = s.source_date,
    #     source_time = s.source_time,
    #     contact_name = s.contact_name,
    #     contact_phone = s.contact_phone,
    #     start_time = s.start_time,
    #     end_time = s.end_time,
    #     virtual_tour_url = s.virtual_tour_url,
    #     ylopo_action = s.ylopo_action,
    #     batch_id = s.batch_id
    # FROM {temp_table} AS s
    # WHERE s.listing_id = t.listing_id
    #   AND s.date = t.date and s.start_time = t.start_time
    #   AND s.openhouse_type = t.openhouse_type;
    # """
    # cursor_stage.execute(update_query)

    # Step 3: Insert non-matching rows with explicit columns
    insert_query = f"""
    INSERT INTO public.listing_openhouse ({','.join(column_names)})
    SELECT DISTINCT ON (s.listing_id, s.date, s.start_time, s.end_time) {','.join([f's.{col}' for col in column_names])}  -- Explicitly use s.
    FROM {temp_table} AS s
    LEFT JOIN public.listing_openhouse t
    ON t.listing_id = s.listing_id
       AND t.date = s.date
       AND t.start_time = s.start_time
       AND t.end_time = s.end_time
       AND t.openhouse_type = s.openhouse_type
    WHERE t.listing_id IS NULL and coalesce(s.is_inactive, false) = false
    ORDER BY s.listing_id, s.date, s.start_time, s.end_time DESC;
    """
    cursor_stage.execute(insert_query)
    db_connection_stage.commit()


def duplicate_openhouse_removal(sync_ids, cursor_stage, db_connection_stage):
    data_deletion = """DELETE FROM listing_openhouse
                        WHERE id IN (
                        SELECT a.id FROM
                        (
                        SELECT o.id,
                            row_number() OVER (PARTITION BY o.listing_id, o.date, o.start_time, o.end_time ORDER BY o.id DESC) AS rn
                        FROM
                        listing_openhouse o
                        JOIN listing_p_active l
                        on o.listing_id = l.id
                        WHERE l.source_id in {}
                        ) a
                        WHERE a.rn > 1)""".format(
        str(sync_ids).replace("[", "(").replace("]", ")")
    )
    cursor_stage.execute(data_deletion)
    db_connection_stage.commit()


def upsert_openhouse(sync_ids, cursor_local, cursor_stage, db_connection_local):
    oh_latest = """
    select DISTINCT ON (s.source_listing_id, s.date, s.start_time)
    t.target_listing_id as listing_id
    ,s.y_creation_date as y_creation_date
    ,s.y_last_update_date as y_last_update_date
    ,upper(coalesce(s.openhouse_type,'IN-PERSON')) as openhouse_type
    ,s.source_creation_date
    ,s.source_last_update_date
    ,s.source_date
    ,s.source_time
    ,s.contact_name
    ,s.contact_phone
    ,s.date
    ,s.start_time
    ,s.end_time
    ,s.virtual_tour_url
    ,s.ylopo_action
    ,t.batch_id as batch_id     
    from stage.DIRECT_idx_openhouse s 
    join stage.etl_direct_idx_update_listings t   
    on s.source_listing_id=t.source_listing_id
    where s.source_date is not null and t.target_listing_id is not null
    and s.source_id in {}
    ORDER BY s.source_listing_id, s.date DESC, s.start_time DESC;
    """.format(str(sync_ids).replace("[", "(").replace("]", ")"))
    cursor_local.execute(oh_latest)
    result = cursor_local.fetchall()
    column_names = [desc[0] for desc in cursor_local.description]
    df = pd.DataFrame(result, columns=column_names)
    cols = ",".join(list(df.columns))
    insert_query = """INSERT INTO public.listing_openhouse ({}) VALUES %s
                        """.format(cols)
    extras.execute_values(cursor_stage, insert_query, result)


def update_listing_property_type_search(
    sync_ids,
    batch_ids,
    cursor_local,
    cursor_stage,
    db_connection_local,
    db_connection_stage,
):
    ulpts = """
    select 
    t.target_listing_id
    ,s.source_last_update_date 
    ,s.source_id
    ,s.batch_id
    ,s.y_last_update_date
    ,ilpt.is_condo
    ,ilpt.is_foreclosure
    ,ilpt.is_land
    ,ilpt.is_manufactured_home
    ,ilpt.is_mobile_home
    ,ilpt.is_multifamily
    ,ilpt.is_rental
    ,ilpt.is_short_sale
    ,ilpt.is_single_family_home
    ,ilpt.is_townhouse
    ,ilpt.is_income_property
    ,ilpt.is_commercial_property
    ,ilpt.is_farm_ranch
    ,ilpt.is_other
    ,ilpt.is_coop
    ,ilpt.is_condop
    ,ilpt.is_condo_hotel

    from stage.direct_idx_listing s 
    join stage.etl_direct_idx_update_listings t
    on s.source_listing_id=t.source_listing_id
    and s.source_id = t.source_id
    inner join 
    public.listing_property_type lpt
    on 
    s.source_id = lpt.source_id and
    --s.property_type = lpt.property_type
    (s.property_type = lpt.property_type or (s.property_type is null and lpt.property_type is null))

    inner join 
    public.listing_property_sub_type lpst
    on 
    s.source_id = lpst.source_id and
    --s.property_sub_type = lpst.property_sub_type
    (s.property_sub_type = lpst.property_sub_type or (s.property_sub_type is null and lpst.property_sub_type is null))

    inner join idx_config.listing_property_type ilpt on
    s.source_id = ilpt.source_id and
    lpt.id = ilpt.property_type_id  and
    lpst.id = ilpt.property_sub_type_id

    where s.source_id in {0} and s.batch_id in {1}


    """.format(
        str(sync_ids).replace("[", "(").replace("]", ")"),
        str(batch_ids).replace("[", "(").replace("]", ")"),
    )
    cursor_local.execute(ulpts)
    result = cursor_local.fetchall()

    update_query = f"""
    UPDATE public.listing_property_type_search AS pl
    SET 
	source_last_update_date = data.source_last_update_date::date,
	batch_id = data.batch_id,
	y_last_update_date = data.y_last_update_date::timestamp,
	is_condo = data.is_condo::boolean,
	is_foreclosure = data.is_foreclosure::boolean,
	is_land = data.is_land::boolean,
	is_manufactured_home = data.is_manufactured_home::boolean,
	is_mobile_home = data.is_mobile_home::boolean,
	is_multifamily = data.is_multifamily::boolean,
	is_rental = data.is_rental::boolean,
	is_short_sale = data.is_short_sale::boolean,
	is_single_family_home = data.is_single_family_home::boolean,
	is_townhouse = data.is_townhouse::boolean,
	is_income_property = data.is_income_property::boolean,
	is_commercial_property = data.is_commercial_property::boolean,
	is_farm_ranch = data.is_farm_ranch::boolean,
	is_other = data.is_other::boolean
    
    FROM (VALUES %s) AS data (target_listing_id,source_last_update_date ,source_id,batch_id,y_last_update_date,is_condo,is_foreclosure,is_land,is_manufactured_home,is_mobile_home,is_multifamily,is_rental,is_short_sale,is_single_family_home,is_townhouse,is_income_property,is_commercial_property,is_farm_ranch,is_other)
    
    
    WHERE pl.listing_id = data.target_listing_id
	and pl.source_id = data.source_id

    """
    extras.execute_values(cursor_stage, update_query, result)
    db_connection_stage.commit()


def update_listing_property_type_search_against_metadata(
    sync_ids,
    batch_ids,
    cursor_local,
    cursor_stage,
    db_connection_local,
    db_connection_stage,
):

    ulp = """
	select 
	
	ps.listing_id,
	pt.is_condo,
	pt.is_foreclosure,	
	pt.is_land,
	pt.is_manufactured_home,	
	pt.is_mobile_home,	
	pt.is_multifamily,	
	pt.is_rental,	
	pt.is_short_sale,	
	pt.is_single_family_home,	
	pt.is_townhouse	,
	pt.is_income_property,	
	pt.is_commercial_property,
	pt.is_farm_ranch,	
	pt.is_other,
	l.y_last_update_date,
	l.source_id
    ,pt.is_coop
    ,pt.is_condop
    ,pt.is_condo_hotel

	from listing_property_type_search ps
	join listing l
		on ps.listing_id = l.id
		and ps.source_id = l.source_id
	join idx_config.listing_property_type pt
		on pt.property_type_id = l.property_type_id
		and pt.property_sub_type_id = l.property_sub_type_id 
		and pt.source_id = l.source_id
	where l.source_id in {} and pt.y_last_update_date > ps.y_last_update_date;


    """.format(str(sync_ids).replace("[", "(").replace("]", ")"))
    cursor_stage.execute(ulp)
    result = cursor_stage.fetchall()

    update_query = f"""
    UPDATE public.listing_property_type_search AS pl
    SET 
	is_condo=data.is_condo::boolean,
	is_foreclosure=data.is_foreclosure::boolean,
	is_land=data.is_land::boolean,
	is_manufactured_home=data.is_manufactured_home::boolean,
	is_mobile_home=data.is_mobile_home::boolean,
	is_multifamily=data.is_multifamily::boolean,
	is_rental=data.is_rental::boolean,	
	is_short_sale=data.is_short_sale::boolean,	
	is_single_family_home=data.is_single_family_home::boolean,	
	is_townhouse=data.is_townhouse::boolean,
	is_income_property=data.is_income_property::boolean,
	is_commercial_property=data.is_commercial_property::boolean,
	is_farm_ranch=data.is_farm_ranch::boolean,
	is_other=data.is_other::boolean,
	y_last_update_date=data.y_last_update_date::timestamp
		
    FROM (VALUES %s) AS data (listing_id,is_condo,is_foreclosure,	is_land,is_manufactured_home,	is_mobile_home,	is_multifamily,	is_rental,	is_short_sale,	is_single_family_home,	is_townhouse	,is_income_property,	is_commercial_property,is_farm_ranch,	is_other,y_last_update_date,source_id)
    
    
    WHERE pl.listing_id = data.listing_id
	and pl.source_id=data.source_id

    """
    extras.execute_values(cursor_stage, update_query, result)
    db_connection_stage.commit()


def listing_change(
    sync_ids,
    batch_ids,
    cursor_local,
    cursor_stage,
    db_connection_local,
    db_connection_stage,
):
    ls = """
    SELECT ls.ylopo_status, a.change_type,a.new_value,a.old_value,a.listing_id,a.batch_id 
    FROM (
    select 'STATUS_CHANGE' as change_type, s.listing_status as new_value, ls.status as old_value, l.id as listing_id  , s.batch_id,l.listing_status_id , s.source_id, s.listing_status as stg_status
    from stage.direct_idx_listing s
    join listing l 
    on s.source_listing_id = l.source_listing_id
    and s.source_id = l.source_id
    join stage.etl_direct_idx_update_listings e
    on e.target_listing_id = l.id
    and e.source_id = l.source_id
    join listing_status ls 

	on l.listing_status_id=ls.id
    where s.source_id in {0} and trim(s.listing_status) != trim(ls.status)

    UNION ALL

    select 'PRICE_CHANGE' as change_type, s.price::text as new_value, l.price::text as old_value, l.id as listing_id  , s.batch_id,l.listing_status_id , s.source_id, s.listing_status as stg_status
    from stage.direct_idx_listing s
    join listing l 
    on s.source_listing_id = l.source_listing_id
    and s.source_id = l.source_id
    join stage.etl_direct_idx_update_listings e
    on e.target_listing_id = l.id
    and e.source_id = l.source_id
    where s.source_id in {0} and s.price != l.price and case when s.price - l.price < 0 then (-1)*(s.price - l.price) else s.price - l.price end >= 1000

    UNION ALL

    select 'PHOTOS_ADDED' as change_type, s.photo_count::text as new_value, l.photo_count::text as old_value, l.id as listing_id  , s.batch_id,l.listing_status_id , s.source_id, s.listing_status as stg_status
    from stage.direct_idx_listing s
    join listing l 
    on s.source_listing_id = l.source_listing_id
    and s.source_id = l.source_id
    join stage.etl_direct_idx_update_listings e
    on e.target_listing_id = l.id
    and e.source_id = l.source_id
    where s.source_id in {0} and coalesce(s.photo_count,0) > coalesce(l.photo_count,0)

    UNION ALL

    select 'OPEN_HOUSE_ADDED' as change_type, new_value::text as new_value, old_value::text as old_value,  l.listing_id  , s.batch_id,listing_status_id , s.source_id, s.stg_status
    from 
    (
    	select o.source_id, o.source_listing_id,l.batch_id, concat(date, ' ', o.start_time)::timeStamp as new_value , coalesce(l.listing_status, ls.status) as stg_status
    	from stage.direct_idx_openhouse o
    	left join stage.direct_idx_listing l
    	on o.source_id = l.source_id
    	and o.source_listing_id = l.source_listing_id
    	left join listing ll
    	on o.source_id = ll.source_id
    	and o.source_listing_id = ll.source_listing_id
    	left join listing_status ls
    	on ll.listing_status_id = ls.id
    	where o.source_id in {0}
    )s
    join 

    (select l.source_id, l.source_listing_id, o.listing_id, max(concat(date, ' ', start_time)::timestamp) as old_value,l.listing_status_id
    	from listing l 
    	join listing_openhouse o 
    	on l.id = o.listing_id 
    	join stage.etl_direct_idx_update_listings e
    	on e.target_listing_id = l.id
    	and e.source_id = l.source_id
    	where l.source_id in {0} group by 1,2,3,5
    ) l
	on s.source_listing_id = l.source_listing_id
    and s.source_id = l.source_id
    left join 
    (
    select listing_id, concat(date, ' ', start_time)::timestamp as oph_date_time from listing l join listing_openhouse o on l.id = o.listing_id where source_id in {0}
    ) z
	on z.listing_id = l.listing_id
	and z.oph_date_time = s.new_value
    where s.source_id in {0}  and z.listing_id is null and  s.new_value != l.old_value
    
    order by 4  ) a
    join listing_status ls
    on a.stg_status=ls.status
    and a.source_Id=ls.source_id;
    """.format(str(sync_ids).replace("[", "(").replace("]", ")"))

    cursor_local.execute(ls)
    result = cursor_local.fetchall()
    column_names = [desc[0] for desc in cursor_local.description]
    df = pd.DataFrame(result, columns=column_names)
    cols = ",".join(list(df.columns))

    insert_query = """
                    INSERT INTO public.listing_change ({}) VALUES %s
                    """.format(cols)
    query_execution_and_commit(insert_query, cursor_stage, db_connection_stage, result)


def update_insert_normalize_community_update(
    sync_ids,
    batch_ids,
    cursor_local,
    cursor_stage,
    db_connection_local,
    db_connection_stage,
):
    query = """
    select
    a.community_name as orignal_community,
    regexp_replace(UPPER(a.community_name), area_name_exp , am.area_name) as mapped_community,
    cast(case when (replace( regexp_replace(UPPER(a.community_name), area_name_exp , '*****'),UPPER(a.community_name),'!!!!!')) like '%*****%' then '9' else '0' end as int) as flag_address,
    a.source_listing_id,
    a.source_id,
    am.area_name_exp,
    am.area_name,
    am.area_type,

    a.y_creation_date,	
    a.y_last_update_date,	
    a.source_creation_date,	
    a.source_last_update_date,
    t.target_listing_id as listing_id

    from stage.direct_idx_address a
    join
    stage.area_mapping am
     on a.source_id=am.source_id

    join stage.etl_direct_idx_update_listings t
    on a.source_listing_id=t.source_listing_id
    and a.source_id = t.source_id

    and am.area_type~*'community'
    where a.source_id in {0} and a.batch_id in {1}
    order by t.target_listing_id
    """.format(
        str(sync_ids).replace("[", "(").replace("]", ")"),
        str(batch_ids).replace("[", "(").replace("]", ")"),
    )

    cursor_local.execute(query)
    result = cursor_local.fetchall()
    column_names = [desc[0] for desc in cursor_local.description]
    df = pd.DataFrame(result, columns=column_names)
    cols = ",".join(list(df.columns))

    insert_query = """
                    INSERT INTO stage.area_normalize ({}) VALUES %s
                    """.format(cols)
    query_execution_and_commit(insert_query, cursor_local, db_connection_local, result)


def update_insert_normalize_subdivision_update(
    sync_ids,
    batch_ids,
    cursor_local,
    cursor_stage,
    db_connection_local,
    db_connection_stage,
):
    query = """
    select
    a.community_name as orignal_community,
    regexp_replace(UPPER(a.community_name), area_name_exp , am.area_name) as mapped_community,
    cast(case when (replace( regexp_replace(UPPER(a.community_name), area_name_exp , '*****'),UPPER(a.community_name),'!!!!!')) like '%*****%' then '9' else '0' end as int) as flag_address,
    a.source_listing_id,
    a.source_id,
    am.area_name_exp,
    am.area_name,
    am.area_type,

    a.y_creation_date,	
    a.y_last_update_date,	
    a.source_creation_date,	
    a.source_last_update_date,
    t.target_listing_id as listing_id

    from stage.direct_idx_address a
    join
    stage.area_mapping am
     on a.source_id=am.source_id

    join stage.etl_direct_idx_update_listings t
    on a.source_listing_id=t.source_listing_id
    and a.source_id = t.source_id

    and am.area_type~*'subdivision'
    where a.source_id in {0} and a.batch_id in {1}
    order by t.target_listing_id
    """.format(
        str(sync_ids).replace("[", "(").replace("]", ")"),
        str(batch_ids).replace("[", "(").replace("]", ")"),
    )

    cursor_local.execute(query)
    result = cursor_local.fetchall()
    column_names = [desc[0] for desc in cursor_local.description]
    df = pd.DataFrame(result, columns=column_names)
    cols = ",".join(list(df.columns))

    insert_query = """
                    INSERT INTO stage.area_normalize ({}) VALUES %s
                    """.format(cols)
    query_execution_and_commit(insert_query, cursor_local, db_connection_local, result)


def update_listing_real_estate_office_rel(
    sync_ids,
    batch_ids,
    cursor_local,
    cursor_stage,
    db_connection_local,
    db_connection_stage,
):
    ul = """
	select 
	distinct on (t.target_listing_id,s.rank,s.source_office_id)
    t.target_listing_id as listing_id,
	s.rank,
	s.source_last_update_date,
	s.source_office_id as office_id,
	s.batch_id, 
	s.y_creation_date as y_last_update_date
	
	
	from stage.DIRECT_idx_office s
	
	join stage.etl_direct_idx_update_listings t
	
	on t.source_listing_id=s.source_listing_id 
	and s.batch_id = t.batch_id
    and s.source_id =  t.source_id
	
	where s.batch_id in {1}  
	and s.source_id in {0}
	and s.source_office_id is not null;
	""".format(
        str(sync_ids).replace("[", "(").replace("]", ")"),
        str(batch_ids).replace("[", "(").replace("]", ")"),
    )
    cursor_local.execute(ul)
    result = cursor_local.fetchall()

    update_query = f"""
    UPDATE public.listing_real_estate_office_rel AS pl
    SET 
	source_last_update_date = data.source_last_update_date::timestamp,
	office_id = data.office_id,
	batch_id = data.batch_id, 
	y_last_update_date = data.y_last_update_date::timestamp
    
    FROM (VALUES %s) AS data  (listing_id,rank,source_last_update_date,office_id,batch_id, y_last_update_date)
    
    
    WHERE pl.listing_id = data.listing_id
	and pl.rank = data.rank
    """
    extras.execute_values(cursor_stage, update_query, result)
    db_connection_stage.commit()


def update_listing_participant_rel(
    sync_ids,
    batch_ids,
    cursor_local,
    cursor_stage,
    db_connection_local,
    db_connection_stage,
):
    ul = """
	select  
	distinct on (t.target_listing_id,s. rank,s.source_participant_id)
    t.target_listing_id as listing_id,
	s. rank,
	s.source_last_update_date,
	s.batch_id, 
	s.y_last_update_date,
	s.source_participant_id as participant_id,
	s.source_office_id as source_real_estate_office_id
	from stage.DIRECT_idx_agent s
	join stage.etl_direct_idx_update_listings t
	on t.source_listing_id=s.source_listing_id 
	and s.batch_id = t.batch_id
    and s.source_id =  t.source_id
	
	where s.batch_id in {1}
	and s.source_id in {0} 
	and s.source_participant_id is not null; 

    """.format(
        str(sync_ids).replace("[", "(").replace("]", ")"),
        str(batch_ids).replace("[", "(").replace("]", ")"),
    )
    cursor_local.execute(ul)
    result = cursor_local.fetchall()

    update_query = f"""
    UPDATE public.listing_participant_rel AS pl
    SET 
	source_last_update_date = data.source_last_update_date::date,
	batch_id = data.batch_id, 
	y_last_update_date = data.y_last_update_date::timestamp,
	participant_id = data.participant_id,
	source_real_estate_office_id = data.source_real_estate_office_id
    
    FROM (VALUES %s) AS data  (listing_id,rank,source_last_update_date,batch_id, y_last_update_date,participant_id,source_real_estate_office_id)
    
    
    WHERE pl.listing_id = data.listing_id
	and pl.rank = data.rank
    """
    extras.execute_values(cursor_stage, update_query, result)
    db_connection_stage.commit()


def update_listing_value(
    sync_ids,
    batch_ids,
    cursor_local,
    cursor_stage,
    db_connection_local,
    db_connection_stage,
):

    ul = """
	select 
	
	t.target_listing_id as listing_id, 
	s.key_name,
	s.source_creation_date,
	s.source_last_update_date,
	s.batch_id,
	s.y_creation_date,
	s.y_last_update_date,
	s.key_value	
	
	from stage.DIRECT_idx_value s 
	join stage.etl_direct_idx_update_listings t
	
	on s.source_listing_id=t.source_listing_id
	and s.batch_id = t.batch_id
	
	where s.batch_id in {1}
	and s.source_id in {0}

    """.format(
        str(sync_ids).replace("[", "(").replace("]", ")"),
        str(batch_ids).replace("[", "(").replace("]", ")"),
    )
    cursor_local.execute(ul)
    result = cursor_local.fetchall()
    column_names = [desc[0] for desc in cursor_local.description]
    df = pd.DataFrame(result, columns=column_names)
    cols = ",".join(list(df.columns))

    insert_query = """
                    INSERT INTO public.listing_value ({}) VALUES %s
                    """.format(cols)
    query_execution_and_commit(insert_query, cursor_stage, db_connection_stage, result)


def update_listing_marketing_info(
    sync_ids,
    batch_ids,
    cursor_local,
    cursor_stage,
    db_connection_local,
    db_connection_stage,
):
    lmi = """
	select 
	t.target_listing_id as listing_id,
	s.y_last_update_date,
	s.batch_id,
	s.permit_address_on_internet,
	s.vow_address_display,
	s.vow_automated_valuation_display,
	s.vow_consumer_comment,
	s.source_last_update_date 
	
	from stage.direct_idx_listing s 
	join stage.etl_direct_idx_update_listings t
	
	on s.source_listing_id=t.source_listing_id
	and s.batch_id = t.batch_id
	
	
	where (s.permit_address_on_internet is not null or s.vow_address_display is not null or s.vow_automated_valuation_display is not null or s.vow_consumer_comment is not null) 
	and s.batch_id in {1}
	and s.source_id in {0} ;


    """.format(
        str(sync_ids).replace("[", "(").replace("]", ")"),
        str(batch_ids).replace("[", "(").replace("]", ")"),
    )
    cursor_local.execute(lmi)
    result = cursor_local.fetchall()

    update_query = f"""
    UPDATE public.listing_marketing_info AS pl
    SET 
	y_last_update_date = data.y_last_update_date::timestamp,
	batch_id = data.batch_id,
	permit_address_on_internet = data.permit_address_on_internet::boolean,
	vow_address_display = data.vow_address_display::boolean,
	vow_automated_valuation_display = data.vow_automated_valuation_display::boolean,
	vow_consumer_comment = data.vow_consumer_comment::boolean,
	source_last_update_date = data.source_last_update_date::date 
		
    FROM (VALUES %s) AS data (listing_id,y_last_update_date,batch_id,permit_address_on_internet,vow_address_display,vow_automated_valuation_display,vow_consumer_comment,source_last_update_date)
    WHERE pl.listing_id = data.listing_id
    """
    extras.execute_values(cursor_stage, update_query, result)
    db_connection_stage.commit()


def etl_upsert_real_estate_participants(
    sync_ids,
    batch_ids,
    cursor_local,
    cursor_stage,
    db_connection_local,
    db_connection_stage,
):
    try:
        # --------------------------------------------------
        # Step 1: Fetch participants from local DB
        # --------------------------------------------------
        query = """
        SELECT DISTINCT
            s.source_participant_id,
            s.participant_id,
            s.first_name,
            s.last_name,
            s.full_name,
            s.participant_role,
            s.primary_contact_phone,
            s.office_phone,
            s.email,
            s.website_url,
            s.agent_mls_id,
            s.agent_mui,
            s.source_creation_date,
            s.source_last_update_date,
            s.y_creation_date,
            s.y_last_update_date,
            s.batch_id,
            s.source_id,
            s.agent_license
        FROM stage.DIRECT_idx_agent s
        JOIN stage.etl_direct_idx_update_listings t ON s.source_id in {0}  AND t.source_id in {0} AND s.batch_id in {1}
                                                    AND t.source_listing_id = s.source_listing_id
                                                    AND s.source_id = t.source_id AND s.batch_id = t.batch_id
        """.format(
            str(sync_ids).replace("[", "(").replace("]", ")"),
            str(batch_ids).replace("[", "(").replace("]", ")"),
        )
        cursor_local.execute(query)
        result = cursor_local.fetchall()
        column_names = [desc[0] for desc in cursor_local.description]

        if not result:
            logger.info("No participants found in local DB.")
            return

        df = pd.DataFrame(result, columns=column_names)

        # --------------------------------------------------
        # Step 2: Generate row_hash (excluding date/batch fields)
        # --------------------------------------------------

        # def row_hash(row):
        #     columns_to_hash = [
        #         "participant_id",
        #         "first_name",
        #         "last_name",
        #         "full_name",
        #         "participant_role",
        #         "primary_contact_phone",
        #         "office_phone",
        #         "email",
        #         "website_url",
        #         "agent_mls_id",
        #         "agent_mui",
        #         "agent_license",
        #     ]
        #     hash_input = "||".join(
        #         [
        #             str(row[col]) if row[col] is not None else ""
        #             for col in columns_to_hash
        #         ]
        #     )
        #     return hashlib.md5(hash_input.encode("utf-8")).hexdigest()

        # df["row_hash"] = df.apply(row_hash, axis=1)

        # --------------------------------------------------
        # Step 3: Create temp table
        # --------------------------------------------------

        cursor_stage.execute("DROP TABLE IF EXISTS real_estate_participant_temp")
        create_temp_table = """
        CREATE TEMP TABLE real_estate_participant_temp (
            source_participant_id TEXT,
            participant_id TEXT,
            first_name TEXT,
            last_name TEXT,
            full_name TEXT,
            participant_role TEXT,
            primary_contact_phone TEXT,
            office_phone TEXT,
            email TEXT,
            website_url TEXT,
            agent_mls_id TEXT,
            agent_mui TEXT,
            source_creation_date TIMESTAMP,
            source_last_update_date TIMESTAMP,
            y_creation_date TIMESTAMP,
            y_last_update_date TIMESTAMP,
            batch_id BIGINT,
            source_id BIGINT,
            agent_license TEXT
        )
        """
        cursor_stage.execute(create_temp_table)
        db_connection_stage.commit()

        # --------------------------------------------------
        # Step 4: Bulk insert into temp table
        # ------------------------------------------------

        cols = ",".join(df.columns)
        insert_query = f"INSERT INTO real_estate_participant_temp ({cols}) VALUES %s"
        execute_values(cursor_stage, insert_query, df.values.tolist())
        db_connection_stage.commit()
        print(f"{len(df)} participants inserted into real_estate_participant_temp")

        # --------------------------------------------------
        # Step 5: Create indexes for performance
        # --------------------------------------------------

        cursor_stage.execute("""
        CREATE INDEX idx_participant_temp_source
        ON real_estate_participant_temp (source_participant_id, source_id);
        """)

        cursor_stage.execute("""
        CREATE INDEX idx_participant_temp_batch
        ON real_estate_participant_temp (batch_id);
        """)
        db_connection_stage.commit()

        # --------------------------------------------------
        # Step 6: Insert new records
        # --------------------------------------------------

        insert_new_query = """
        INSERT INTO real_estate_participant (
            source_participant_id, participant_id, first_name, last_name,
            full_name, participant_role, primary_contact_phone, office_phone,
            email, website_url, agent_mls_id, agent_mui,
            source_creation_date, source_last_update_date,
            y_creation_date, y_last_update_date,
            batch_id, source_id, agent_license
        )
        SELECT tmp.source_participant_id, tmp.participant_id, tmp.first_name, tmp.last_name,
               tmp.full_name, tmp.participant_role, tmp.primary_contact_phone, tmp.office_phone,
               tmp.email, tmp.website_url, tmp.agent_mls_id, tmp.agent_mui,
               tmp.source_creation_date, tmp.source_last_update_date,
               tmp.y_creation_date, tmp.y_last_update_date,
               tmp.batch_id, tmp.source_id, tmp.agent_license
        FROM real_estate_participant_temp tmp
        LEFT JOIN real_estate_participant rl
          ON rl.source_id in {0}
            AND tmp.source_participant_id = rl.source_participant_id
            AND tmp.source_id = rl.source_id 
        WHERE rl.source_participant_id IS NULL
        """.format(
            str(sync_ids).replace("[", "(").replace("]", ")"),
            str(batch_ids).replace("[", "(").replace("]", ")"),
        )
        cursor_stage.execute(insert_new_query)
        db_connection_stage.commit()
        print("Inserted new participants into real_estate_participant")

        # --------------------------------------------------
        # Step 7: Update only changed participants using hash (exclude date/batch columns)
        # --------------------------------------------------
        update_query = """
        UPDATE real_estate_participant rl
        SET
            participant_id = tmp.participant_id,
            first_name = tmp.first_name,
            last_name = tmp.last_name,
            full_name = tmp.full_name,
            participant_role = tmp.participant_role,
            primary_contact_phone = tmp.primary_contact_phone,
            office_phone = tmp.office_phone,
            email = tmp.email,
            website_url = tmp.website_url,
            agent_mls_id = tmp.agent_mls_id,
            agent_mui = tmp.agent_mui,
            source_creation_date = tmp.source_creation_date,
            source_last_update_date = tmp.source_last_update_date,
            
            y_last_update_date = tmp.y_last_update_date,
            batch_id = tmp.batch_id,
            agent_license = tmp.agent_license
        FROM real_estate_participant_temp tmp
        WHERE rl.source_id in {0}
          AND rl.source_participant_id = tmp.source_participant_id
          AND rl.source_id = tmp.source_id
          
        """.format(
            str(sync_ids).replace("[", "(").replace("]", ")"),
            str(batch_ids).replace("[", "(").replace("]", ")"),
        )
        cursor_stage.execute(update_query)
        db_connection_stage.commit()
        print("Updated only changed participants using hash")
        logger.info("Changed participants updated successfully")

        logger.info("ETL process completed successfully")

    except Exception as e:
        logger.error(
            f"ETL process failed for source_id={sync_ids}: {str(e)}", exc_info=True
        )

        # Rollback in case of failure
        try:
            db_connection_stage.rollback()
            logger.info("Transaction rolled back successfully")
        except Exception as rollback_error:
            logger.error(f"Rollback failed: {rollback_error}")

        raise  # Re-raise exception if you want upstream handling


def etl_Upsert_real_estate_offices(
    sync_ids,
    batch_ids,
    cursor_local,
    cursor_stage,
    db_connection_local,
    db_connection_stage,
):
    try:

        # --------------------------------------------------
        # Step 1: Fetch participants from local DB
        # --------------------------------------------------
        query = """
        SELECT DISTINCT
            s.source_office_id,
            s.office_id,
            s.office_name,
            s.corporate_name,
            s.main_office_id,
            s.phone_number,
            s.fax,
            s.full_street_address,
            s.city,
            s.state_province,
            s.country,
            s.office_email,
            s.website,
            s.unit_number,
            s.office_mls_id,
            s.office_mui,
            s.source_creation_date,
            s.source_last_update_date,
            s.y_creation_date,
            s.y_last_update_date,
            s.batch_id,
            s.source_id
        FROM stage.DIRECT_idx_office s
        JOIN stage.etl_direct_idx_update_listings t     ON  s.source_id in {0}  AND t.source_id in {0} AND s.batch_id in {1} 
                                                        AND S.source_listing_id = T.source_listing_id
                                                        AND s.batch_id = t.batch_id
                                                        AND s.source_id = t.source_id
        """.format(
            str(sync_ids).replace("[", "(").replace("]", ")"),
            str(batch_ids).replace("[", "(").replace("]", ")"),
        )
        cursor_local.execute(query)
        result = cursor_local.fetchall()
        column_names = [desc[0] for desc in cursor_local.description]

        if not result:
            print("No office records found in local DB.")
            return

        df = pd.DataFrame(result, columns=column_names)

        # --------------------------------------------------
        # Step 2: Generate row_hash (excluding date/batch fields)
        # --------------------------------------------------
        # def row_hash(row):
        #     columns_to_hash = [
        #         "office_id",
        #         "office_name",
        #         "corporate_name",
        #         "main_office_id",
        #         "phone_number",
        #         "fax",
        #         "full_street_address",
        #         "city",
        #         "state_province",
        #         "country",
        #         "office_email",
        #         "website",
        #         "unit_number",
        #         "office_mls_id",
        #         "office_mui",
        #     ]
        #     hash_input = "||".join(
        #         [
        #             str(row[col]) if row[col] is not None else ""
        #             for col in columns_to_hash
        #         ]
        #     )

        #     return hashlib.md5(hash_input.encode("utf-8")).hexdigest()

        # df["row_hash"] = df.apply(row_hash, axis=1)

        # --------------------------------------------------
        # Step 3: Create temp table
        # --------------------------------------------------

        cursor_stage.execute("DROP TABLE IF EXISTS real_estate_office_temp")
        create_temp_table = """
        CREATE TEMP TABLE real_estate_office_temp (
            source_office_id TEXT,
            office_id TEXT,
            office_name TEXT,
            corporate_name TEXT,
            main_office_id TEXT,
            phone_number TEXT,
            fax TEXT,
            full_street_address TEXT,
            city TEXT,
            state_province TEXT,
            country TEXT,
            office_email TEXT,
            website TEXT,
            unit_number TEXT,
            office_mls_id TEXT,
            office_mui TEXT,
            source_creation_date TIMESTAMP,
            source_last_update_date TIMESTAMP,
            y_creation_date TIMESTAMP,
            y_last_update_date TIMESTAMP,
            batch_id BIGINT,
            source_id BIGINT
        )
        """
        cursor_stage.execute(create_temp_table)
        db_connection_stage.commit()

        # --------------------------------------------------
        # Step 4: Bulk insert into temp table
        # ------------------------------------------------
        cols = ",".join(df.columns)
        insert_query = f"INSERT INTO real_estate_office_temp ({cols}) VALUES %s"
        execute_values(cursor_stage, insert_query, df.values.tolist())
        db_connection_stage.commit()
        print(f"{len(df)} records inserted into real_estate_office_temp")

        # --------------------------------------------------
        # Step 5: Create indexes for performance
        # --------------------------------------------------
        cursor_stage.execute("""
        CREATE INDEX idx_real_estate_office_temp_source
        ON real_estate_office_temp (source_office_id, source_id);
        """)
        db_connection_stage.commit()

        # --------------------------------------------------
        # Step 6: Insert new records
        # --------------------------------------------------
        insert_new_query = """
        INSERT INTO real_estate_office (
            source_office_id, office_id, office_name, corporate_name,
            main_office_id, phone_number, fax, full_street_address,
            city, state_province, country, office_email, website,
            unit_number, office_mls_id, office_mui,
            source_creation_date, source_last_update_date,
            y_creation_date, y_last_update_date,
            batch_id, source_id
        )
        SELECT tmp.source_office_id, tmp.office_id, tmp.office_name, tmp.corporate_name,
               tmp.main_office_id, tmp.phone_number, tmp.fax, tmp.full_street_address,
               tmp.city, tmp.state_province, tmp.country, tmp.office_email, tmp.website,
               tmp.unit_number, tmp.office_mls_id, tmp.office_mui,
               tmp.source_creation_date, tmp.source_last_update_date,
               tmp.y_creation_date, tmp.y_last_update_date,
               tmp.batch_id, tmp.source_id
        FROM real_estate_office_temp tmp
        LEFT JOIN real_estate_office rl
            ON rl.source_id in {0}
            AND tmp.source_office_id = rl.source_office_id
            AND tmp.source_id = rl.source_id
        WHERE rl.source_office_id IS NULL
        """.format(
            str(sync_ids).replace("[", "(").replace("]", ")"),
            str(batch_ids).replace("[", "(").replace("]", ")"),
        )
        cursor_stage.execute(insert_new_query)
        db_connection_stage.commit()
        print("Inserted new offices into real_estate_office")

        # --------------------------------------------------
        # Step 7: Update only changed offices using hash (no main table modification)
        # --------------------------------------------------
        update_query = """
        UPDATE real_estate_office rl
        SET
            office_id = tmp.office_id,
            office_name = tmp.office_name,
            corporate_name = tmp.corporate_name,
            main_office_id = tmp.main_office_id,
            phone_number = tmp.phone_number,
            fax = tmp.fax,
            full_street_address = tmp.full_street_address,
            city = tmp.city,
            state_province = tmp.state_province,
            country = tmp.country,
            office_email = tmp.office_email,
            website = tmp.website,
            unit_number = tmp.unit_number,
            office_mls_id = tmp.office_mls_id,
            office_mui = tmp.office_mui,
            source_creation_date = tmp.source_creation_date,
            source_last_update_date = tmp.source_last_update_date,
            y_last_update_date = tmp.y_last_update_date,
            batch_id = tmp.batch_id
        FROM real_estate_office_temp tmp
        WHERE rl.source_id in {0}
          AND rl.source_office_id = tmp.source_office_id
          AND rl.source_id = tmp.source_id
          
        """.format(
            str(sync_ids).replace("[", "(").replace("]", ")"),
            str(batch_ids).replace("[", "(").replace("]", ")"),
        )
        cursor_stage.execute(update_query)
        db_connection_stage.commit()
        print("Updated only changed offices using hash from temp table")
    except Exception as e:
        logger.error(
            f"ETL process failed for source_id={sync_ids}: {str(e)}", exc_info=True
        )

        try:
            db_connection_stage.rollback()
            logger.info("Transaction rolled back successfully")
        except Exception as rollback_error:
            logger.error(f"Rollback failed: {rollback_error}")

        raise


def etl_upsert_real_estate_participants(
    sync_ids,
    batch_ids,
    cursor_local,
    cursor_stage,
    db_connection_local,
    db_connection_stage,
):
    try:
        # --------------------------------------------------
        # Step 1: Fetch participants from local DB
        # --------------------------------------------------
        query = """
        SELECT DISTINCT ON (s.source_participant_id, s.source_id)
            s.source_participant_id,
            s.participant_id,
            s.first_name,
            s.last_name,
            s.full_name,
            s.participant_role,
            s.primary_contact_phone,
            s.office_phone,
            s.email,
            s.website_url,
            s.agent_mls_id,
            s.agent_mui,
            s.source_creation_date,
            s.source_last_update_date,
            s.y_creation_date,
            s.y_last_update_date,
            s.batch_id,
            s.source_id,
            s.agent_license
        FROM stage.DIRECT_idx_agent s
        JOIN stage.etl_direct_idx_update_listings t ON s.source_id in {0}  AND t.source_id in {0} AND s.batch_id in {1}
                                                    AND t.source_listing_id = s.source_listing_id
                                                    AND s.source_id = t.source_id AND s.batch_id = t.batch_id
        """.format(
            str(sync_ids).replace("[", "(").replace("]", ")"),
            str(batch_ids).replace("[", "(").replace("]", ")"),
        )
        cursor_local.execute(query)
        result = cursor_local.fetchall()
        column_names = [desc[0] for desc in cursor_local.description]

        if not result:
            logger.info("No participants found in local DB.")
            return

        df = pd.DataFrame(result, columns=column_names)

        # --------------------------------------------------
        # Step 2: Generate row_hash (excluding date/batch fields)
        # --------------------------------------------------

        # def row_hash(row):
        #     columns_to_hash = [
        #         "participant_id",
        #         "first_name",
        #         "last_name",
        #         "full_name",
        #         "participant_role",
        #         "primary_contact_phone",
        #         "office_phone",
        #         "email",
        #         "website_url",
        #         "agent_mls_id",
        #         "agent_mui",
        #         "agent_license",
        #     ]
        #     hash_input = "||".join(
        #         [
        #             str(row[col]) if row[col] is not None else ""
        #             for col in columns_to_hash
        #         ]
        #     )
        #     return hashlib.md5(hash_input.encode("utf-8")).hexdigest()

        # df["row_hash"] = df.apply(row_hash, axis=1)

        # --------------------------------------------------
        # Step 3: Create temp table
        # --------------------------------------------------

        cursor_stage.execute("DROP TABLE IF EXISTS real_estate_participant_temp")
        create_temp_table = """
        CREATE TEMP TABLE real_estate_participant_temp (
            source_participant_id TEXT,
            participant_id TEXT,
            first_name TEXT,
            last_name TEXT,
            full_name TEXT,
            participant_role TEXT,
            primary_contact_phone TEXT,
            office_phone TEXT,
            email TEXT,
            website_url TEXT,
            agent_mls_id TEXT,
            agent_mui TEXT,
            source_creation_date TIMESTAMP,
            source_last_update_date TIMESTAMP,
            y_creation_date TIMESTAMP,
            y_last_update_date TIMESTAMP,
            batch_id BIGINT,
            source_id BIGINT,
            agent_license TEXT
        )
        """
        cursor_stage.execute(create_temp_table)
        db_connection_stage.commit()

        # --------------------------------------------------
        # Step 4: Bulk insert into temp table
        # ------------------------------------------------

        cols = ",".join(df.columns)
        insert_query = f"INSERT INTO real_estate_participant_temp ({cols}) VALUES %s"
        execute_values(cursor_stage, insert_query, df.values.tolist())
        db_connection_stage.commit()
        print(f"{len(df)} participants inserted into real_estate_participant_temp")

        # --------------------------------------------------
        # Step 5: Create indexes for performance
        # --------------------------------------------------

        cursor_stage.execute("""
        CREATE INDEX idx_participant_temp_source
        ON real_estate_participant_temp (source_participant_id, source_id);
        """)

        cursor_stage.execute("""
        CREATE INDEX idx_participant_temp_batch
        ON real_estate_participant_temp (batch_id);
        """)
        db_connection_stage.commit()

        # --------------------------------------------------
        # Step 6: Insert new records
        # --------------------------------------------------

        insert_new_query = """
        INSERT INTO real_estate_participant (
            source_participant_id, participant_id, first_name, last_name,
            full_name, participant_role, primary_contact_phone, office_phone,
            email, website_url, agent_mls_id, agent_mui,
            source_creation_date, source_last_update_date,
            y_creation_date, y_last_update_date,
            batch_id, source_id, agent_license
        )
        SELECT tmp.source_participant_id, tmp.participant_id, tmp.first_name, tmp.last_name,
               tmp.full_name, tmp.participant_role, tmp.primary_contact_phone, tmp.office_phone,
               tmp.email, tmp.website_url, tmp.agent_mls_id, tmp.agent_mui,
               tmp.source_creation_date, tmp.source_last_update_date,
               tmp.y_creation_date, tmp.y_last_update_date,
               tmp.batch_id, tmp.source_id, tmp.agent_license
        FROM real_estate_participant_temp tmp
        LEFT JOIN real_estate_participant rl
          ON rl.source_id in {0}
            AND tmp.source_participant_id = rl.source_participant_id
            AND tmp.source_id = rl.source_id 
        WHERE rl.source_participant_id IS NULL
        """.format(
            str(sync_ids).replace("[", "(").replace("]", ")"),
            str(batch_ids).replace("[", "(").replace("]", ")"),
        )
        cursor_stage.execute(insert_new_query)
        db_connection_stage.commit()
        print("Inserted new participants into real_estate_participant")

        # --------------------------------------------------
        # Step 7: Update only changed participants using hash (exclude date/batch columns)
        # --------------------------------------------------
        update_query = """
        UPDATE real_estate_participant rl
        SET
            participant_id = tmp.participant_id,
            first_name = tmp.first_name,
            last_name = tmp.last_name,
            full_name = tmp.full_name,
            participant_role = tmp.participant_role,
            primary_contact_phone = tmp.primary_contact_phone,
            office_phone = tmp.office_phone,
            email = tmp.email,
            website_url = tmp.website_url,
            agent_mls_id = tmp.agent_mls_id,
            agent_mui = tmp.agent_mui,
            source_creation_date = tmp.source_creation_date,
            source_last_update_date = tmp.source_last_update_date,
            y_creation_date = tmp.y_creation_date,
            y_last_update_date = tmp.y_last_update_date,
            batch_id = tmp.batch_id,
            agent_license = tmp.agent_license
        FROM real_estate_participant_temp tmp
        WHERE rl.source_id in {0}
          AND rl.source_participant_id = tmp.source_participant_id
          AND rl.source_id = tmp.source_id
          
        """.format(
            str(sync_ids).replace("[", "(").replace("]", ")"),
            str(batch_ids).replace("[", "(").replace("]", ")"),
        )
        cursor_stage.execute(update_query)
        db_connection_stage.commit()
        print("Updated only changed participants using hash")
        logger.info("Changed participants updated successfully")

        logger.info("ETL process completed successfully")

    except Exception as e:
        logger.error(
            f"ETL process failed for source_id={sync_ids}: {str(e)}", exc_info=True
        )

        # Rollback in case of failure
        try:
            db_connection_stage.rollback()
            logger.info("Transaction rolled back successfully")
        except Exception as rollback_error:
            logger.error(f"Rollback failed: {rollback_error}")

        raise  # Re-raise exception if you want upstream handling


def etl_Upsert_real_estate_offices(
    sync_ids,
    batch_ids,
    cursor_local,
    cursor_stage,
    db_connection_local,
    db_connection_stage,
):
    try:

        # --------------------------------------------------
        # Step 1: Fetch participants from local DB
        # --------------------------------------------------
        query = """
        SELECT DISTINCT ON (s.source_office_id, s.source_id)
            s.source_office_id,
            s.office_id,
            s.office_name,
            s.corporate_name,
            s.main_office_id,
            s.phone_number,
            s.fax,
            s.full_street_address,
            s.city,
            s.state_province,
            s.country,
            s.office_email,
            s.website,
            s.unit_number,
            s.office_mls_id,
            s.office_mui,
            s.source_creation_date,
            s.source_last_update_date,
            s.y_creation_date,
            s.y_last_update_date,
            s.batch_id,
            s.source_id
        FROM stage.DIRECT_idx_office s
        JOIN stage.etl_direct_idx_update_listings t     ON  s.source_id in {0}  AND t.source_id in {0} AND s.batch_id in {1} 
                                                        AND S.source_listing_id = T.source_listing_id
                                                        AND s.batch_id = t.batch_id
                                                        AND s.source_id = t.source_id
        """.format(
            str(sync_ids).replace("[", "(").replace("]", ")"),
            str(batch_ids).replace("[", "(").replace("]", ")"),
        )
        cursor_local.execute(query)
        result = cursor_local.fetchall()
        column_names = [desc[0] for desc in cursor_local.description]

        if not result:
            print("No office records found in local DB.")
            return

        df = pd.DataFrame(result, columns=column_names)

        # --------------------------------------------------
        # Step 2: Generate row_hash (excluding date/batch fields)
        # --------------------------------------------------
        # def row_hash(row):
        #     columns_to_hash = [
        #         "office_id",
        #         "office_name",
        #         "corporate_name",
        #         "main_office_id",
        #         "phone_number",
        #         "fax",
        #         "full_street_address",
        #         "city",
        #         "state_province",
        #         "country",
        #         "office_email",
        #         "website",
        #         "unit_number",
        #         "office_mls_id",
        #         "office_mui",
        #     ]
        #     hash_input = "||".join(
        #         [
        #             str(row[col]) if row[col] is not None else ""
        #             for col in columns_to_hash
        #         ]
        #     )

        #     return hashlib.md5(hash_input.encode("utf-8")).hexdigest()

        # df["row_hash"] = df.apply(row_hash, axis=1)

        # --------------------------------------------------
        # Step 3: Create temp table
        # --------------------------------------------------

        cursor_stage.execute("DROP TABLE IF EXISTS real_estate_office_temp")
        create_temp_table = """
        CREATE TEMP TABLE real_estate_office_temp (
            source_office_id TEXT,
            office_id TEXT,
            office_name TEXT,
            corporate_name TEXT,
            main_office_id TEXT,
            phone_number TEXT,
            fax TEXT,
            full_street_address TEXT,
            city TEXT,
            state_province TEXT,
            country TEXT,
            office_email TEXT,
            website TEXT,
            unit_number TEXT,
            office_mls_id TEXT,
            office_mui TEXT,
            source_creation_date TIMESTAMP,
            source_last_update_date TIMESTAMP,
            y_creation_date TIMESTAMP,
            y_last_update_date TIMESTAMP,
            batch_id BIGINT,
            source_id BIGINT
        )
        """
        cursor_stage.execute(create_temp_table)
        db_connection_stage.commit()

        # --------------------------------------------------
        # Step 4: Bulk insert into temp table
        # ------------------------------------------------
        cols = ",".join(df.columns)
        insert_query = f"INSERT INTO real_estate_office_temp ({cols}) VALUES %s"
        execute_values(cursor_stage, insert_query, df.values.tolist())
        db_connection_stage.commit()
        print(f"{len(df)} records inserted into real_estate_office_temp")

        # --------------------------------------------------
        # Step 5: Create indexes for performance
        # --------------------------------------------------
        cursor_stage.execute("""
        CREATE INDEX idx_real_estate_office_temp_source
        ON real_estate_office_temp (source_office_id, source_id);
        """)
        db_connection_stage.commit()

        # --------------------------------------------------
        # Step 6: Insert new records
        # --------------------------------------------------
        insert_new_query = """
        INSERT INTO real_estate_office (
            source_office_id, office_id, office_name, corporate_name,
            main_office_id, phone_number, fax, full_street_address,
            city, state_province, country, office_email, website,
            unit_number, office_mls_id, office_mui,
            source_creation_date, source_last_update_date,
            y_creation_date, y_last_update_date,
            batch_id, source_id
        )
        SELECT tmp.source_office_id, tmp.office_id, tmp.office_name, tmp.corporate_name,
               tmp.main_office_id, tmp.phone_number, tmp.fax, tmp.full_street_address,
               tmp.city, tmp.state_province, tmp.country, tmp.office_email, tmp.website,
               tmp.unit_number, tmp.office_mls_id, tmp.office_mui,
               tmp.source_creation_date, tmp.source_last_update_date,
               tmp.y_creation_date, tmp.y_last_update_date,
               tmp.batch_id, tmp.source_id
        FROM real_estate_office_temp tmp
        LEFT JOIN real_estate_office rl
            ON rl.source_id in {0}
            AND tmp.source_office_id = rl.source_office_id
            AND tmp.source_id = rl.source_id
        WHERE rl.source_office_id IS NULL
        """.format(
            str(sync_ids).replace("[", "(").replace("]", ")"),
            str(batch_ids).replace("[", "(").replace("]", ")"),
        )
        cursor_stage.execute(insert_new_query)
        db_connection_stage.commit()
        print("Inserted new offices into real_estate_office")

        # --------------------------------------------------
        # Step 7: Update only changed offices using hash (no main table modification)
        # --------------------------------------------------
        update_query = """
        UPDATE real_estate_office rl
        SET
            office_id = tmp.office_id,
            office_name = tmp.office_name,
            corporate_name = tmp.corporate_name,
            main_office_id = tmp.main_office_id,
            phone_number = tmp.phone_number,
            fax = tmp.fax,
            full_street_address = tmp.full_street_address,
            city = tmp.city,
            state_province = tmp.state_province,
            country = tmp.country,
            office_email = tmp.office_email,
            website = tmp.website,
            unit_number = tmp.unit_number,
            office_mls_id = tmp.office_mls_id,
            office_mui = tmp.office_mui,
            source_creation_date = tmp.source_creation_date,
            source_last_update_date = tmp.source_last_update_date,
            y_creation_date = tmp.y_creation_date,
            y_last_update_date = tmp.y_last_update_date,
            batch_id = tmp.batch_id
        FROM real_estate_office_temp tmp
        WHERE rl.source_id in {0}
          AND rl.source_office_id = tmp.source_office_id
          AND rl.source_id = tmp.source_id
          
        """.format(
            str(sync_ids).replace("[", "(").replace("]", ")"),
            str(batch_ids).replace("[", "(").replace("]", ")"),
        )
        cursor_stage.execute(update_query)
        db_connection_stage.commit()
        print("Updated only changed offices using hash from temp table")
    except Exception as e:
        logger.error(
            f"ETL process failed for source_id={sync_ids}: {str(e)}", exc_info=True
        )

        try:
            db_connection_stage.rollback()
            logger.info("Transaction rolled back successfully")
        except Exception as rollback_error:
            logger.error(f"Rollback failed: {rollback_error}")

        raise


def update_real_estate_office(
    sync_ids,
    batch_ids,
    cursor_local,
    cursor_stage,
    db_connection_local,
    db_connection_stage,
):

    query = """
    select  
    distinct  s.source_office_id
    , s.office_id
    , s.office_name
    , s.corporate_name
    , s.main_office_id
    , s.phone_number
    , s.fax
    , s.full_street_address
    , s.city
    , s.state_province
    , s.country
    , s.office_email
    , s.website
    , s.unit_number
    , s.office_mls_id
    , s.office_mui
    , S.source_creation_date
    , S.source_last_update_date
    , S.y_creation_date
    , S.y_last_update_date
    , s.batch_id
    , s.source_id 
    
    from stage.DIRECT_idx_office s
    
    join stage.etl_direct_idx_update_listings t
    
    on t.source_listing_id = s.source_listing_id
    and s.batch_id = t.batch_id 
    and s.source_id =  t.source_id
    
    left join public.real_estate_office o 
    
    on o.source_office_id = s.source_office_id
    and o.source_id in {0} and o.source_id = s.source_id
    and coalesce(s.office_id, 'Dummy') = coalesce(o.office_id, 'Dummy')
    
    where o.source_office_id is null 
    and s.batch_id in {1}
    and s.source_id in {0}
    and  s.source_office_id is not null;
    """.format(
        str(sync_ids).replace("[", "(").replace("]", ")"),
        str(batch_ids).replace("[", "(").replace("]", ")"),
    )

    cursor_local.execute(query)
    result = cursor_local.fetchall()
    column_names = [desc[0] for desc in cursor_local.description]
    df = pd.DataFrame(result, columns=column_names)
    cols = ",".join(list(df.columns))

    insert_query = """
                    INSERT INTO public.real_estate_office ({}) VALUES %s
                    """.format(cols)
    query_execution_and_commit(insert_query, cursor_stage, db_connection_stage, result)
    query_execution_and_commit(insert_query, cursor_local, db_connection_local, result)


def update_real_estate_participant(
    sync_ids,
    batch_ids,
    cursor_local,
    cursor_stage,
    db_connection_local,
    db_connection_stage,
):
    query = """
    select  
    distinct s.source_participant_id
    ,s.participant_id 
    ,s.first_name  
    ,s.last_name  
    ,s.full_name  
    ,s.participant_role
    ,s.primary_contact_phone
    ,s.office_phone   
    ,s.email   
    ,s.website_url
    ,s.agent_mls_id
    ,s.agent_mui
    ,s.source_creation_date
    ,s.source_last_update_date
    ,s.y_creation_date
    ,s.y_last_update_date
    ,s.batch_id
    ,s.source_id 
    , s.agent_license
    
    from stage.DIRECT_idx_agent s
    
    join stage.etl_direct_idx_update_listings t
    
    on t.source_listing_id=s.source_listing_id 
    and s.batch_id = t.batch_id
    and s.source_id =  t.source_id
    
    left join public.real_estate_participant o 
    
    on o.source_participant_id = s.source_participant_id
    and o.source_id in {0} and o.source_id = s.source_id
    and coalesce(s.participant_id, 'Dummy') = coalesce(o.participant_id, 'Dummy')
    
    where o.source_participant_id is null 
    and s.batch_id in {1}
    and s.source_id in {0}
    and s.source_participant_id is not null;
    """.format(
        str(sync_ids).replace("[", "(").replace("]", ")"),
        str(batch_ids).replace("[", "(").replace("]", ")"),
    )

    cursor_local.execute(query)
    result = cursor_local.fetchall()
    column_names = [desc[0] for desc in cursor_local.description]
    df = pd.DataFrame(result, columns=column_names)
    cols = ",".join(list(df.columns))

    insert_query = """
                    INSERT INTO public.real_estate_participant ({}) VALUES %s
                    """.format(cols)
    query_execution_and_commit(insert_query, cursor_stage, db_connection_stage, result)
    query_execution_and_commit(insert_query, cursor_local, db_connection_local, result)


def sync_broker(sync_ids, cursor_local, cursor_stage, db_connection_local):
    agents_query = """
    SELECT distinct 
    t.target_listing_id as listing_id,
    s.source_creation_date, 
    s.source_last_update_date, 
    t.batch_id,
    t.y_creation_date AS y_creation_date,
    t.y_creation_date AS y_last_update_date,
    s.brokerage_name,
    s.brokerage_phone,
    s.brokerage_email,
    s.brokerage_websiteurl
    	
	FROM stage.direct_idx_broker s  

    join stage.etl_direct_idx_insert_listings t
	on t.source_listing_id=s.source_listing_id 
	where 
     s.source_id in {0} 
     and t.target_listing_id is not null;    
""".format(str(sync_ids).replace("[", "(").replace("]", ")"))
    cursor_local.execute(agents_query)
    result = cursor_local.fetchall()
    column_names = [desc[0] for desc in cursor_local.description]
    df = pd.DataFrame(result, columns=column_names)
    cols = ",".join(list(df.columns))
    insert_query = """
                    INSERT INTO public.broker ({}) VALUES %s
                    """.format(cols)
    extras.execute_values(cursor_stage, insert_query, result)


def update_broker(
    sync_ids,
    batch_ids,
    cursor_local,
    cursor_stage,
    db_connection_local,
    db_connection_stage,
):
    ul = """
    SELECT distinct 
    t.target_listing_id as listing_id,
    s.source_creation_date, 
    s.source_last_update_date, 
    t.batch_id,
    s.y_creation_date AS y_creation_date,
    s.y_creation_date AS y_last_update_date,
    s.brokerage_name,
    s.brokerage_phone,
    s.brokerage_email,
    s.brokerage_websiteurl
    	
	FROM stage.direct_idx_broker s  

    join stage.etl_direct_idx_update_listings t
	on t.source_listing_id=s.source_listing_id 
	where 
     s.source_id in {0} 
     and t.target_listing_id is not null;

    """.format(
        str(sync_ids).replace("[", "(").replace("]", ")"),
        str(batch_ids).replace("[", "(").replace("]", ")"),
    )
    cursor_local.execute(ul)
    result = cursor_local.fetchall()

    update_query = f"""
    UPDATE public.broker AS br
    SET 
    source_creation_date = data.source_creation_date ,
    source_last_update_date = data.source_last_update_date ,
    batch_id = data.batch_id ,
    y_creation_date = data.y_creation_date ,
    y_last_update_date = data.y_last_update_date ,
    brokerage_name = data.brokerage_name ,
    brokerage_phone = data.brokerage_phone ,
    brokerage_email = data.brokerage_email ,
    brokerage_websiteurl = data.brokerage_websiteurl
    
    FROM (VALUES %s) AS data  (listing_id,source_creation_date,source_last_update_date, batch_id,y_creation_date,y_last_update_date,brokerage_name,brokerage_phone,brokerage_email,brokerage_websiteurl)
    
    WHERE br.listing_id = data.listing_id;
    """
    extras.execute_values(cursor_stage, update_query, result)
    db_connection_stage.commit()


def etl_delete_listings(sync_ids, batch_id, listing_cursor, listing_conn):

    source_id = str(sync_ids).replace("[", "").replace("]", "")
    batch_id = str(batch_id).replace("[", "").replace("]", "")

    lc = """select 'STATUS_CHANGE' as change_type, 
	e.listing_status as new_value, 
	ls.status as old_value, 
	l.id as listing_id,
	e.batch_id, 
	ls.ylopo_status
	from stage.etl_direct_idx_delete_listings e
	join listing l 
		on e.target_listing_id = l.id
			and e.source_id = l.source_id
	join listing_status ls 
		on ls.id = l.listing_status_id
	where e.source_id = {0} and e.batch_id = {1}  and trim(e.listing_status) != trim(ls.status)
    """.format(source_id, batch_id)

    listing_cursor.execute(lc)
    result = listing_cursor.fetchall()
    column_names = [desc[0] for desc in listing_cursor.description]
    df = pd.DataFrame(result, columns=column_names)
    cols = ",".join(list(df.columns))
    insert_query = """INSERT INTO public.listing_change ({}) VALUES %s""".format(cols)
    extras.execute_values(listing_cursor, insert_query, result)
    listing_conn.commit()

    update_1 = """update public.listing_change l set source_status='INACTIVE'
	from stage.etl_direct_idx_delete_listings t
	where l.listing_id = t.target_listing_id
	and t.source_status='INACTIVE'
	and t.source_id= {};
    """.format(source_id)
    listing_cursor.execute(update_1)
    listing_conn.commit()

    update_2 = """update public.listing_address_standard las set source_status='INACTIVE'
	where las.listing_address_id in (select l.id from listing_address l join stage.etl_direct_idx_delete_listings t
	on l.listing_id = t.target_listing_id
	where t.batch_id = {1}
	and t.source_status='INACTIVE'
	and t.source_id = {0});
    """.format(
        source_id, batch_id
    )
    listing_cursor.execute(update_2)
    listing_conn.commit()

    update_3 = """update public.listing_address l set source_status='INACTIVE', batch_id = {1}
	from stage.etl_direct_idx_delete_listings t
	where l.listing_id = t.target_listing_id
	and t.batch_id = {1}
	and t.source_status='INACTIVE'
	and t.source_id = {0};
    """.format(
        source_id, batch_id
    )
    listing_cursor.execute(update_3)
    listing_conn.commit()

    update_4 = """update public.listing l set source_status='INACTIVE'
	from stage.etl_direct_idx_delete_listings t
	where l.id = t.target_listing_id
	and t.source_status='INACTIVE'
	and t.source_id= {0};
    """.format(source_id)
    listing_cursor.execute(update_4)
    listing_conn.commit()

    update_5 = """update public.listing_change l set source_status='SOLD'
	from stage.etl_direct_idx_delete_listings t
	where l.listing_id = t.target_listing_id
	and t.source_status='SOLD'
	and t.source_id= {0};
    """.format(source_id)
    listing_cursor.execute(update_5)
    listing_conn.commit()

    update_6 = """update public.listing_address_standard las set source_status='SOLD'
	where las.listing_address_id in (select l.id from listing_address l join stage.etl_direct_idx_delete_listings t
	on l.listing_id = t.target_listing_id
	where t.batch_id = {1}
	and t.source_status='SOLD'
	and t.source_id = {0});
    """.format(source_id, batch_id)
    listing_cursor.execute(update_6)
    listing_conn.commit()

    update_7 = """update public.listing_address l set source_status='SOLD', batch_id = {1}
	from stage.etl_direct_idx_delete_listings t
	where l.listing_id = t.target_listing_id
	and t.batch_id = {1}
	and t.source_status='SOLD'
	and t.source_id = {0};
    """.format(
        source_id, batch_id
    )
    listing_cursor.execute(update_7)
    listing_conn.commit()

    update_8 = """update public.listing l set source_status='SOLD', sold_price = t.sold_price, sold_date = t.sold_date
	from stage.etl_direct_idx_delete_listings t
	where l.id = t.target_listing_id
	and t.source_status='SOLD'
	and t.source_id={0};
    """.format(
        source_id
    )
    listing_cursor.execute(update_8)
    listing_conn.commit()

    ul = """SELECT 
	current_timestamp AS inactive_date, 
	l.batch_id, 
	current_timestamp AS y_last_update_date, 
	L.id,
	t.source_id,
	case when t.source_last_update_date is null then current_timestamp else t.source_last_update_date end as source_last_update_date,
	ls.id as listing_status_id
	from listing L 
	join stage.etl_direct_idx_delete_listings t 
	on L.id=t.target_listing_id
	join listing_status ls
	on t.source_id = ls.source_id
	and t.listing_status = ls.status
	and t.source_id= {0}	
    
    """.format(source_id)
    listing_cursor.execute(ul)
    result = listing_cursor.fetchall()

    update_query = f"""UPDATE public.listing AS pl
    SET 
	inactive_date = data.inactive_date
	,batch_id = data.batch_id
	,y_last_update_date = data.y_last_update_date
	,listing_status_id = data.listing_status_id
	,source_last_update_date = data.source_last_update_date
    
    FROM (VALUES %s) AS data (inactive_date,batch_id ,y_last_update_date,id,source_id, source_last_update_date, listing_status_id)
    WHERE pl.id = data.id
    and pl.source_id = data.source_id
    """
    extras.execute_values(listing_cursor, update_query, result)
    listing_conn.commit()


def insert_address_missing(sync_ids, cursor_local, cursor_stage, db_connection_stage):

    address_query = """
     select 
       t.target_listing_id as listing_id,
       s.city,
       s.county,
       s.full_street_address,
       s.y_creation_date as y_creation_date,
       s.y_creation_date as y_last_update_date,
       s.source_creation_date,                   
       s.source_last_update_date,
       s.zoning, 
       s.mls_latitude,
       s.mls_longitude,
       s.postal_code,
       s.unit_number, 
       s.state_or_province,
       s.parcel_number as parcel_id,
       s.batch_id as batch_id,
       s.country,                            
       UPPER(s.community_name) as community_name,
       s.region,
       s.zone,
       t.source_id as source_id,
       UPPER(s.subdivision_name) as subdivision_name,
       s.district_name,
       s.mls_area_name,
       s.custom_area_name_1,
       s.custom_area_name_2,
       s.custom_area_name_3,
       s.sub_district_name,
       UPPER(TRIM(BOTH ' ' FROM CONCAT(
               REGEXP_REPLACE(REGEXP_REPLACE(s.full_street_address, '[aeiouAEIOU ]', '','g'), '[^a-zA-Z\d\s:]', '','g'),
               REGEXP_REPLACE(REGEXP_REPLACE(s.city, '[aeiouAEIOU ]', '','g'), '[^a-zA-Z\d\s:]', '','g'),
               REGEXP_REPLACE(REGEXP_REPLACE(s.postal_code, '[aeiouAEIOU ]', '','g'), '[^a-zA-Z\d\s:]', '','g')
           ))) AS address_token
        from stage.direct_idx_address s
            join stage.etl_direct_idx_insert_listings t
              on s.source_listing_id=t.source_listing_id
                   and s.source_id = t.source_id
    where s.source_id in {} and t.target_listing_id is not null
    order by t.target_listing_id
     """.format(str(sync_ids).replace("[", "(").replace("]", ")"))
    cursor_local.execute(address_query)
    result = cursor_local.fetchall()
    column_names = [desc[0] for desc in cursor_local.description]
    df_stage = pd.DataFrame(result, columns=column_names)

    if df_stage.empty:
        return

    # Step 2: Fetch existing listing_id values
    listing_ids = tuple(df_stage["listing_id"].drop_duplicates())

    # Avoid empty tuple issue in SQL
    if not listing_ids:
        return

    # Create query string for filtering existing rows
    query_existing = """
        SELECT listing_id 
        FROM public.listing_address
        WHERE listing_id IN %s
    """
    cursor_stage.execute(query_existing, (listing_ids,))
    existing_listing_ids = set(row[0] for row in cursor_stage.fetchall())

    # Step 3: Filter out existing listing_ids from df_stage
    df_stage_filtered = df_stage[~df_stage["listing_id"].isin(existing_listing_ids)]

    if df_stage_filtered.empty:
        return

    # Step 4: Insert filtered records
    insert_query = """INSERT INTO public.listing_address ({}) VALUES %s""".format(
        ",".join(df_stage_filtered.columns)
    )
    extras.execute_values(cursor_stage, insert_query, df_stage_filtered.values.tolist())


def lambda_handler(event, context):
    
    global db_connection_local
    global db_connection_stage

    source_type = event["source_type"]
    source_id = event["source_id"]

    if isinstance(event, list):
        pass
    else:
        event = [event]

    if event is None or all(element is None for element in event):

        log_msg = {"Status": "Data Sync NOT EXECUTED due to empty list"}

        logger.info(log_msg)
        return event

    else:

        rdsDB = os.environ.get("rdsDatabase")
        listingDB = os.environ.get("listingDatabase")
        sqlExecLimit = os.environ.get("sqlExecLimit")

        db_secret_local = SecretManagerHelper.get_secret(rdsDB, "us-west-2")
        db_secret_stage = SecretManagerHelper.get_secret(listingDB, "us-west-2")

        db_connection_local = get_connection(db_secret_local, db_connection_local, sqlExecLimit)
        db_connection_stage = get_connection(db_secret_stage, db_connection_stage, sqlExecLimit)

        cursor_local = db_connection_local.cursor()
        cursor_stage = db_connection_stage.cursor()

        try:

            sync_ids = []
            batch_ids = []

            for item in event:
                if item is not None and item != "null":
                    sync_ids.append(item["source_id"])
                    batch_ids.append(item["batch_id"])

            # -------------Lookup Tables Synchronization Start---------------------
            Load_Lisitng_Category_Lookup(
                sync_ids,
                batch_ids,
                cursor_local,
                cursor_stage,
                db_connection_local,
                db_connection_stage,
            )
            Load_Listing_Status(
                sync_ids,
                batch_ids,
                cursor_local,
                cursor_stage,
                db_connection_local,
                db_connection_stage,
            )
            Load_Listing_Property_Type(
                sync_ids,
                batch_ids,
                cursor_local,
                cursor_stage,
                db_connection_local,
                db_connection_stage,
            )
            load_listing_property_sub_type(
                sync_ids,
                batch_ids,
                cursor_local,
                cursor_stage,
                db_connection_local,
                db_connection_stage,
            )
            load_mls_board(
                sync_ids,
                batch_ids,
                cursor_local,
                cursor_stage,
                db_connection_local,
                db_connection_stage,
            )
            load_config_listing_property_type(
                sync_ids,
                batch_ids,
                cursor_local,
                cursor_stage,
                db_connection_local,
                db_connection_stage,
            )
            load_listing_school_type(
                sync_ids,
                batch_ids,
                cursor_local,
                cursor_stage,
                db_connection_local,
                db_connection_stage,
            )
            # -------------Lookup Tables Synchronization End---------------------

            # -------------Listing Insertions Start------------------------------
            sync_listing(sync_ids, cursor_local, cursor_stage, db_connection_local)
            # update_etl_direct_idx_insert_listings_count(
            #     sync_ids,
            #     batch_ids,
            #     cursor_local,
            #     cursor_stage,
            #     db_connection_local,
            #     db_connection_stage,
            # )
            temp_listing_rds(
                sync_ids,
                batch_ids,
                cursor_local,
                cursor_stage,
                db_connection_local,
                db_connection_stage,
            )

            # -------------Area Normalize Insert Start------------------------------
            area_normalize_subdivision_insert(
                sync_ids, cursor_local, cursor_stage, db_connection_local
            )
            area_normalize_community_insert(
                sync_ids, cursor_local, cursor_stage, db_connection_local
            )
            area_normalize_insert_listing_address_community(
                sync_ids, cursor_local, cursor_stage, db_connection_local
            )
            area_normalize_insert_listing_address_subdivision(
                sync_ids, cursor_local, cursor_stage, db_connection_local
            )
            # -------------Area Normalize Insert End------------------------------

            sync_address(sync_ids, cursor_local, cursor_stage, db_connection_stage)
            sync_description(sync_ids, cursor_local, cursor_stage, db_connection_local)
            sync_photos(sync_ids, cursor_local, cursor_stage, db_connection_local)
            sync_office_Get_Office_Rel(
                sync_ids, cursor_local, cursor_stage, db_connection_local
            )
            sync_listing_participant_rel(
                sync_ids, cursor_local, cursor_stage, db_connection_local
            )
            sync_broker(sync_ids, cursor_local, cursor_stage, db_connection_local)
            insert_real_estate_office(
                sync_ids,
                batch_ids,
                cursor_local,
                cursor_stage,
                db_connection_local,
                db_connection_stage,
            )
            insert_real_estate_participant(
                sync_ids,
                batch_ids,
                cursor_local,
                cursor_stage,
                db_connection_local,
                db_connection_stage,
            )
            sync_schools(sync_ids, cursor_local, cursor_stage, db_connection_local)
            sync_listing_school_district(
                sync_ids, cursor_local, cursor_stage, db_connection_local
            )
            sync_listing_attribute(
                sync_ids, cursor_local, cursor_stage, db_connection_local
            )
            sync_Listing_Property_Type_Search(sync_ids, cursor_local, cursor_stage)
            sync_listing_marketing_info(
                sync_ids,
                batch_ids,
                cursor_local,
                cursor_stage,
                db_connection_local,
                db_connection_stage,
            )
            # -------------Listing Insertions End-------------------------------

            # -------------Listing Updations Start------------------------------

            update_listing(
                sync_ids,
                batch_ids,
                cursor_local,
                cursor_stage,
                db_connection_local,
                db_connection_stage,
            )
            update_description(
                sync_ids,
                batch_ids,
                cursor_local,
                cursor_stage,
                db_connection_local,
                db_connection_stage,
            )

            # if source_type in ["MLS Grid V2 API"]:
            #     # and source_id != 705:
            #     update_mlsgrid_photo(
            #         sync_ids,
            #         batch_ids,
            #         cursor_local,
            #         cursor_stage,
            #         db_connection_local,
            #         db_connection_stage,
            #     )
            # else:
            update_photo(
                sync_ids,
                batch_ids,
                cursor_local,
                cursor_stage,
                db_connection_local,
                db_connection_stage,
            )

            update_listing_prefetch_photo(
                sync_ids,
                batch_ids,
                cursor_local,
                cursor_stage,
                db_connection_local,
                db_connection_stage,
            )
            update_school(
                sync_ids,
                batch_ids,
                cursor_local,
                cursor_stage,
                db_connection_local,
                db_connection_stage,
            )
            update_attributes(
                sync_ids,
                batch_ids,
                cursor_local,
                cursor_stage,
                db_connection_local,
                db_connection_stage,
            )
            update_school_district(
                sync_ids,
                batch_ids,
                cursor_local,
                cursor_stage,
                db_connection_local,
                db_connection_stage,
            )

            # -------------Area Normalize Update Start------------------------------
            area_normalize_community_update(
                sync_ids, cursor_local, cursor_stage, db_connection_local
            )
            area_normalize_subdivision_update(
                sync_ids, cursor_local, cursor_stage, db_connection_local
            )
            area_normalize_update_listing_address_community(
                sync_ids, cursor_local, cursor_stage, db_connection_local
            )
            area_normalize_update_listing_address_subdivision(
                sync_ids, cursor_local, cursor_stage, db_connection_local
            )
            # -------------Area Normalize Update End------------------------------

            update_address_insert(
                sync_ids, cursor_local, cursor_stage, db_connection_stage
            )
            update_address(
                sync_ids,
                batch_ids,
                cursor_local,
                cursor_stage,
                db_connection_local,
                db_connection_stage,
            )
            update_address_standard(
                sync_ids,
                batch_ids,
                cursor_local,
                cursor_stage,
                db_connection_local,
                db_connection_stage,
            )

            openhouse_rds_to_homelistings(
                sync_ids,
                cursor_local,
                db_connection_local,
                cursor_stage,
                db_connection_stage,
            )
            openhouse_all(sync_ids, cursor_stage, db_connection_stage)
            update_insert_Listing_Property_Type_Search(
                sync_ids, cursor_local, cursor_stage
            )
            update_listing_property_type_search(
                sync_ids,
                batch_ids,
                cursor_local,
                cursor_stage,
                db_connection_local,
                db_connection_stage,
            )
            listing_change(
                sync_ids,
                batch_ids,
                cursor_local,
                cursor_stage,
                db_connection_local,
                db_connection_stage,
            )

            update_office_Get_Office_Rel_insert(
                sync_ids, cursor_local, cursor_stage, db_connection_local
            )
            update_listing_participant_rel_insert(
                sync_ids, cursor_local, cursor_stage, db_connection_local
            )
            update_listing_real_estate_office_rel(
                sync_ids,
                batch_ids,
                cursor_local,
                cursor_stage,
                db_connection_local,
                db_connection_stage,
            )
            update_listing_participant_rel(
                sync_ids,
                batch_ids,
                cursor_local,
                cursor_stage,
                db_connection_local,
                db_connection_stage,
            )
            update_broker(
                sync_ids,
                batch_ids,
                cursor_local,
                cursor_stage,
                db_connection_local,
                db_connection_stage,
            )
            update_listing_marketing_info(
                sync_ids,
                batch_ids,
                cursor_local,
                cursor_stage,
                db_connection_local,
                db_connection_stage,
            )
            if source_id not in {924, 267, 905, 429}:
                etl_upsert_real_estate_participants(
                    sync_ids,
                    batch_ids,
                    cursor_local,
                    cursor_stage,
                    db_connection_local,
                    db_connection_stage,
                )
                etl_Upsert_real_estate_offices(
                    sync_ids,
                    batch_ids,
                    cursor_local,
                    cursor_stage,
                    db_connection_local,
                    db_connection_stage,
                )
            else:
                update_real_estate_participant(
                    sync_ids,
                    batch_ids,
                    cursor_local,
                    cursor_stage,
                    db_connection_local,
                    db_connection_stage,
                )

                update_real_estate_office(
                    sync_ids,
                    batch_ids,
                    cursor_local,
                    cursor_stage,
                    db_connection_local,
                    db_connection_stage,
                )

            insert_address_missing(
                sync_ids, cursor_local, cursor_stage, db_connection_stage
            )
            # -------------Listing Updations End--------------------------------

            etl_delete_listings(sync_ids, batch_ids, cursor_stage, db_connection_stage)

            update_load_date(
                sync_ids,
                batch_ids,
                cursor_stage,
                db_connection_local,
                db_connection_stage,
            )
            update_listing_status(
                sync_ids,
                cursor_local,
                cursor_stage,
                db_connection_local,
                db_connection_stage,
            )
            update_address_status(
                sync_ids,
                cursor_local,
                cursor_stage,
                db_connection_local,
                db_connection_stage,
            )
            listing_ids = update_listing_price(
                sync_ids,
                cursor_local,
                cursor_stage,
                db_connection_local,
                db_connection_stage,
            )
            price_history_initial(
                listing_ids,
                cursor_local,
                cursor_stage,
                db_connection_local,
                db_connection_stage,
            )
            load_price_history(
                listing_ids,
                cursor_local,
                cursor_stage,
                db_connection_local,
                db_connection_stage,
            )

            for change in event:
                if change is not None and change != "null":
                    change["success"] = True

            return event

        except Exception as e:

            logevent= {"source_id": source_id, "source_type": source_type, "batch_ids": batch_ids}

            log_msg = {
                "Status": "Exception in Lambda Handler ",
                "Error": str(e),
                "Error At line": traceback.format_exc(),
            }
 
            logger.error(f"Event : {event} | LogMessage : {log_msg}")
            logevent["success"] = False
            logevent["Error"] = log_msg
            return logevent

        finally:
            if db_connection_local:
                db_connection_local.commit()
                db_connection_local.close()
            if db_connection_stage:
                db_connection_stage.commit()
                db_connection_stage.close()
