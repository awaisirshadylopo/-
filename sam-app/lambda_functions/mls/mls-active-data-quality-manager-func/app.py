import json
import os
from urllib.parse import unquote
import logging
import psycopg2
from psycopg2 import extras
from psycopg2.extras import execute_values
import pandas as pd
import boto3
import warnings
import traceback
from botocore.exceptions import ClientError
import numpy as np

warnings.filterwarnings("ignore")

logger = logging.getLogger("mls-data_curation_func")
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
            raise e


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

def get_connection(secret, connection, sqlExecLimit):
    try:
        if connection is not None and connection.closed == 0:
            with connection.cursor() as cur:
                cur.execute("SELECT 1")
            return connection
    except Exception:
        pass

    return psycopg2.connect(
        database=secret["dbname"],
        user=secret["username"],
        password=secret["password"],
        host=secret["host"],
        port=secret["port"],
        connect_timeout=15,
        options=f"-c statement_timeout={sqlExecLimit}",
    )

# ------------------------------------------------------------------------------
#  MAIN LOGIC FUNCTION (Converted from your local code)
# ------------------------------------------------------------------------------
def purge_orphaned_ranks_from_homelisting(
    cursor_rds, cursor_homelisting, source_id, batch_id, resource
):
    """
    Comapres the Current LMD batch ranks with target and pruge the orphaned ranks from target.
    """
    logger.info(
        "Starting %s rank processing for source_id=%s, batch_id=%s",
        resource,
        source_id,
        batch_id,
    )
    # -------------------------------------------------------------
    # 1. Basequery (from your local script)
    # -------------------------------------------------------------

    Basequery = f"""
    SELECT source_listing_id, rank 
    FROM (
        SELECT source_listing_id, 1 AS rank 
        FROM stage.direct_idx_{resource}
        WHERE source_id = %s AND batch_id = %s
        UNION
        SELECT source_listing_id, 2 AS rank 
        FROM stage.direct_idx_{resource}
        WHERE source_id = %s AND batch_id = %s
        UNION
        SELECT source_listing_id, 3 AS rank 
        FROM stage.direct_idx_{resource}
        WHERE source_id = %s AND batch_id = %s
        UNION
        SELECT source_listing_id, 4 AS rank 
        FROM stage.direct_idx_{resource}
        WHERE source_id = %s AND batch_id = %s
    ) a
    GROUP BY source_listing_id, rank;
    """
    params = (
        source_id,
        batch_id,
        source_id,
        batch_id,
        source_id,
        batch_id,
        source_id,
        batch_id,
    )
    cursor_rds.execute(Basequery, params)
    base_rows = cursor_rds.fetchall()
    # ❗ Check if empty
    if not base_rows:
        msg = {
            "Resource": resource,
            "message": "No records found for given source_id and batch_id",
            "source_id": source_id,
            "batch_id": batch_id,
        }
        logger.warning(msg)
        return msg
    base_cols = [c[0] for c in cursor_rds.description]
    BaseAgent = [dict(zip(base_cols, row)) for row in base_rows]
    # print('SourceOftruth', BaseAgent)

    # -------------------------------------------------------------
    # 2. CurrentAgentRanks filtered by source_id
    # -------------------------------------------------------------
    query = f"""
    SELECT DISTINCT source_listing_id, rank
    FROM stage.direct_idx_{resource}
    WHERE source_id = %s AND batch_id = %s;
    """

    cursor_rds.execute(query, (source_id, batch_id))
    curr_rows = cursor_rds.fetchall()
    curr_cols = [c[0] for c in cursor_rds.description]
    AgentRanks = [dict(zip(curr_cols, row)) for row in curr_rows]

    curr_set = {(str(x["source_listing_id"]), str(x["rank"])) for x in AgentRanks}

    # -------------------------------------------------------------
    # 3. Find Missing Records (difference)
    # -------------------------------------------------------------
    MissingRecords = [
        rec
        for rec in BaseAgent
        if (str(rec["source_listing_id"]), str(rec["rank"])) not in curr_set
    ]
    # print('MissingRecords', MissingRecords)
    if not MissingRecords:
        return {
            "missing": [],
            "listing_records": [],
            "delete": "No records to delete",
            "delete_sql": None,
        }

    # -------------------------------------------------------------
    # 4. Query listing_p_active to fetch listing IDs
    # -------------------------------------------------------------
    source_list_ids = "', '".join({str(m["source_listing_id"]) for m in MissingRecords})

    listing_query = f"""
        SELECT 
            l.id AS listing_id,
            l.source_listing_id
        FROM listing l
        WHERE l.source_listing_id IN ('{source_list_ids}');
    """

    cursor_rds.execute(listing_query)
    listing_rows = cursor_rds.fetchall()
    listing_cols = [c[0] for c in cursor_rds.description]
    ListingRecords = [dict(zip(listing_cols, row)) for row in listing_rows]
    # print('ListingRecords', ListingRecords)

    # -------------------------------------------------------------
    # 5. Merge — Determine delete targets
    # -------------------------------------------------------------
    list_map = {str(x["source_listing_id"]): x for x in ListingRecords}
    delete_targets = []

    for missing in MissingRecords:
        sid = str(missing["source_listing_id"])
        if sid in list_map:
            delete_targets.append(
                {
                    "source_listing_id": sid,
                    "rank": str(missing["rank"]),
                    "listing_id": list_map[sid]["listing_id"],
                }
            )
    # print('delete_targets', delete_targets)
    if not delete_targets:
        return {
            "missing": MissingRecords,
            "listing_records": ListingRecords,
            "delete": [],
            "delete_sql": None,
        }

    # -------------------------------------------------------------
    # 6. Create DELETE SQL
    # -------------------------------------------------------------
    delete_tuples = [(x["listing_id"], x["rank"]) for x in delete_targets]

    # Mapping of resource type to table name
    resource_table_map = {
        "agent": "listing_participant_rel",
        "office": "listing_real_estate_office_rel",
    }

    # Get table name from resource
    table_name = resource_table_map.get(resource)
    if not table_name:
        raise ValueError(f"Unsupported resource type: {resource}")

    # Build VALUES list for logging
    values_sql = ",".join(
        cursor_rds.mogrify("(%s, %s)", t).decode() for t in delete_tuples
    )

    logged_sql = f"""
    DELETE FROM {table_name} AS d
    USING (VALUES {values_sql}) AS v(listing_id, rank)
    WHERE d.listing_id = v.listing_id
    AND d.rank = v.rank::integer;
    """

    logger.info(
        "Executing SQL to Delete Orphaned Ranks for source_id=%s, batch_id=%s:\n%s",
        source_id,
        batch_id,
        logged_sql,
    )  # log actual SQL

    # Construct SQL dynamically
    sql = f"""
    DELETE FROM {table_name} AS d
    USING (VALUES %s) AS v(listing_id, rank)
    WHERE d.listing_id = v.listing_id
    AND d.rank = v.rank::integer;
    """
    try:
        # Execute the SQL
        execute_values(cursor_homelisting, sql, delete_tuples)
        # Commit the transaction
        cursor_homelisting.connection.commit()

    except Exception as e:

        log_msg = {
            "Status": "Exception in Agent and Office Curation ",
            "Error": e,
            "Error At line": traceback.format_exc(),
        }
        raise Exception(log_msg)

    # cursor_rds.commit()


def lambda_handler(event, context):

    global db_connection_local
    global db_connection_stage
    
    # Example assuming event is a dict
    # Extract source_id and batch_id
    source_id = event[0]["source_id"]
    batch_id = event[0]["batch_id"]
    event[0]["status"] = False

    rdsDB = os.environ.get("rdsDatabase")
    listingDB = os.environ.get("listingDatabase")
    sqlExecLimit = os.environ.get("sqlExecLimit")

    db_secret_local = SecretManagerHelper.get_secret(rdsDB, "us-west-2")
    db_secret_stage = SecretManagerHelper.get_secret(listingDB, "us-west-2")

    db_connection_local = get_connection(db_secret_local, db_connection_local, sqlExecLimit)
    db_connection_stage = get_connection(db_secret_stage, db_connection_stage, sqlExecLimit)

    cursor_rds = db_connection_local.cursor()
    cursor_homelisting = db_connection_stage.cursor()

    try:

        # process_agent_rank_logic(cursor_rds, cursor_homelisting, 100, 42017489, 'agent')
        purge_orphaned_ranks_from_homelisting(
            cursor_rds, cursor_homelisting, source_id, batch_id, "agent"
        )
        purge_orphaned_ranks_from_homelisting(
            cursor_rds, cursor_homelisting, source_id, batch_id, "office"
        )
        if source_id in (858, 642, 709, 699, 619, 916, 483,826):

            retain_primary_images = f"""
                                      WITH PrimaryPhoto AS (
 
                                                                select s.source_id, s.source_status,lp.listing_id,s.batch_id , s.modification_timestamp,s.photo_count,lp.y_creation_date,
                                                                        Row_number() Over (partition by lp.listing_id order by lp.id ) as RowNumber, 
                                                                        lp.id AS PHOTO_ID ,lp.media_url, lp.media_modification_timestamp
                                                                from listing_p_sold s
                                                                join listing_photo lp on lp.listing_id = s.id and s.source_id = {source_id}  and s.y_last_update_date > now() - interval '2 days' --and s.id = '241796393'
                                                                
                                                                
                                        )
                                        select * INTO  TEMP TABLE TEMP_PRIMARY_PHOTOS  from PrimaryPhoto  where rownumber > 1 order by listing_id , PHOTO_ID;
                                        -- DROP TABLE TEMP_PRIMARY_PHOTOS
                                        -- SELECT * FROM TEMP_PRIMARY_PHOTOS;
                                        DELETE FROM listing_photo WHERE ID IN (SELECT PHOTO_ID FROM TEMP_PRIMARY_PHOTOS );
                                    """
            cursor_homelisting.execute(retain_primary_images)
            db_connection_stage.commit()

        event[0]["status"] = True
        return event

    except Exception as e:

        log_msg = {
            "status": False,
            "Error": str(e),
            "Error At line": traceback.format_exc(),
        }
        event[0].update(log_msg)
        logger.error(event)
        return event
    finally:
        if db_connection_local:
            db_connection_local.commit()
            db_connection_local.close()
        if db_connection_stage:
            db_connection_stage.commit()
            db_connection_stage.close()
