import io
import json
import logging
import os
import time
import traceback
from datetime import datetime

import boto3
import pandas as pd
import psycopg2
from psycopg2 import extras

logger = logging.getLogger("AthenaToRDSLambda")
logger.setLevel(logging.INFO)

conn = None

# ── Helper functions (identical to your original) ────────────────────────────
def fetch_secrets(secret_name: str) -> dict:
    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response["SecretString"])

# def setup_db_connection(db_secret: dict, sql_exec_limit: int):
#     return psycopg2.connect(
#         database=db_secret.get("dbname"),
#         user    =db_secret.get("username"),
#         password=db_secret.get("password"),
#         host    =db_secret.get("host"),
#         port    =db_secret.get("port"),
#         options =f"-c statement_timeout={sql_exec_limit}",
#     )

def get_connection(secret, connection, sql_exec_limit):
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
        options=f"-c statement_timeout={sql_exec_limit}",
    )

def get_prestage_prefix(source_type: str) -> str:
    return source_type.strip().split()[0].lower()

def remove_characters(value):
    return str(value).replace("[", "").replace("]", "").replace("'", "")

def clean_value(value):
    if pd.isna(value) or str(value).lower() in ["none", "nan", "na", ""]:
        return None
    return value

def formatted_date(date) -> str:
    original = datetime.strptime(str(date[0]), "%Y-%m-%d %H:%M:%S.%f")
    return original.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

def adding_extra_columns(generic_df, batch_creation_date, source_id, batch_id):
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    meta_df = pd.DataFrame({
        "source_id": [int(source_id)],
        "batch_id": [int(batch_id)],
        "source_last_update_date": [now_str],
        "y_creation_date": [batch_creation_date],
        "y_last_update_date": [batch_creation_date],
        "source_creation_date": [now_str],
    })
    meta_df = pd.concat([meta_df] * len(generic_df), ignore_index=True)
    generic_df = generic_df.reset_index(drop=True)
    return pd.concat([meta_df, generic_df], axis=1)

def table_creation_and_loading(resource_df, source_id, table_name, source_name,
                               cursor_rds, rds_connection, resource_name):
    if len(resource_df) == 0:
        return

    resource_df.drop(columns=["@odata.id", "@odata.etag", "Media@odata.nextLink"],
                     inplace=True, errors="ignore")
    cols_to_keep = [col for col in resource_df.columns if "@" not in str(col)]
    resource_df = resource_df[cols_to_keep]
    resource_df = resource_df.apply(lambda col: col.map(remove_characters))
    resource_df = resource_df.apply(lambda col: col.map(clean_value))
    resource_df.fillna(pd.NaT)
    resource_df.fillna("")
    resource_df.drop_duplicates(inplace=True)

    cursor_rds.execute(
        """SELECT column_name FROM information_schema.columns
           WHERE table_name ~* %s AND column_name NOT IN ('id', 'pid')""",
        (table_name,),
    )
    table_column_names = [row[0] for row in cursor_rds.fetchall()]
    resource_df.columns = resource_df.columns.str.lower()
    df_cols = list(resource_df.columns)

    cols_to_insert = [col for col in df_cols if col in table_column_names]
    skipped = set(df_cols) - set(cols_to_insert)
    if skipped:
        logger.info({"source_id": source_id, "table_name": table_name,
                    "skipped_cols": list(skipped), "message": "Skipping columns not in prestage"})
    resource_df = resource_df[cols_to_insert]
    df_cols = cols_to_insert

    if not df_cols:
        logger.warning({"source_id": source_id, "table_name": table_name,
                        "message": "No columns left – skipping"})
        return

    PG_RESERVED_KEYWORDS = {
        "table", "order", "select", "where", "from", "index", "column",
        "group", "limit", "offset", "check", "default", "user", "value",
        "values", "type", "key", "by", "as", "all", "set", "to", "do",
        "in", "on", "is", "at", "no",
    }
    quoted_cols = [f'"{col}"' if col.lower() in PG_RESERVED_KEYWORDS else col for col in df_cols]
    insert_query = "INSERT INTO idx_stage.{} ({}) VALUES %s".format(table_name, ",".join(quoted_cols))
    data_values = [tuple(row) for row in resource_df.values]
    extras.execute_values(cursor_rds, insert_query, data_values)
    rds_connection.commit()

    logger.info({
        "source_id": source_id, "source_name": source_name,
        "table_name": table_name, "rows_inserted": len(data_values),
        "cols_inserted": len(df_cols),
    })

def submit_athena_query(source_id, class_name,key_column, glue_database, athena_output_bucket):
    """Just submits query and returns execution_id immediately"""
    athena = boto3.client("athena")
    glue   = boto3.client("glue")
    table_name = f"{class_name.lower()}_{source_id}"

    try:
        response  = glue.get_table(DatabaseName=glue_database, Name=table_name)
        glue_cols = response["Table"]["StorageDescriptor"]["Columns"]
        part_cols = response["Table"].get("PartitionKeys", [])
        columns   = [col["Name"] for col in glue_cols + part_cols
                     if "@" not in col["Name"] and col["Name"].strip()]
    except Exception as e:
        logger.error({"source_id": source_id, "class_name": class_name, "error": str(e)})
        raise

    if not columns:
        logger.error({"source_id": source_id, "class_name": class_name, "message": "No valid columns"})
        return None

    select_cols = ", ".join([f'"{col}"' for col in columns if col != "row_num" and "@" not in col and col.strip()])
    query = f"""
        SELECT *
        FROM (
            SELECT {select_cols},
                ROW_NUMBER() OVER (PARTITION BY "{key_column}" ORDER BY CAST(batch_id AS BIGINT) DESC) AS row_num
            FROM "{glue_database}"."{table_name}"
            WHERE "{key_column}" IS NOT NULL AND "{key_column}" != ''
        )
        WHERE row_num = 1
    """

    output_location = f"s3://{athena_output_bucket}/athena-results/{source_id}/{class_name}/"
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": glue_database},
        ResultConfiguration={"OutputLocation": output_location},
    )
    execution_id = response["QueryExecutionId"]
    logger.info({"source_id": source_id, "execution_id": execution_id,
                 "message": "Athena query submitted"})
    return execution_id


def get_athena_result(source_id, class_name, execution_id, athena_output_bucket):
    """Just reads CSV from S3 and returns stream"""
    s3 = boto3.client("s3")
    # csv_key = f"athena-results/{source_id}/{class_name}/{execution_id}.csv"
    prefix   = f"athena-results/{source_id}/{class_name}/"
    csv_key  = f"{prefix}{execution_id}.csv"
    list_resp = s3.list_objects_v2(Bucket=athena_output_bucket, Prefix=prefix)
    actual_keys = [obj["Key"] for obj in list_resp.get("Contents", [])]
    logger.info({
        "source_id": source_id,
        "expected_key": csv_key,
        "actual_keys_in_s3": actual_keys
    })

    try:
        resp = s3.get_object(Bucket=athena_output_bucket, Key=csv_key)
        return resp["Body"]
    except s3.exceptions.NoSuchKey:
        logger.info({"source_id": source_id, "class_name": class_name,
                     "message": "Athena returned 0 rows"})
        return None

# ── Lambda handler ───────────────────────────────────────────────────────────
def lambda_handler(event, context):
    
    global conn
    
    source_data         = event
    source_id           = source_data["source_id"]
    source_name         = source_data["source_name"]
    source_info         = source_data.get("source_info", {})
    batch_id            = source_data["batch_id"]
    batch_creation_date = source_data["batch_creation_date"]
    source_type         = source_info.get("source_type") or source_data.get("source_type", "")


    enabled_resources = source_info.get("respecs_config", {}).get(
        "enabled_resources",
        [{"class_name": "Property", "key_column": "listingkey"}]
    )
    valid_resources = source_data.get(
        "respecs_valid_resources",
        enabled_resources
    )
    


    glue_db    = os.environ.get("glue_database", "s3_db")
    athena_out = os.environ.get("athena_output_bucket")
    sql_limit  = context.get_remaining_time_in_millis()

    # conn   = None
    cursor = None

    try:
        if not source_data.get("athena_query_done", False):
            # ── First call: just submit Athena query and exit ─────────────────
            # execution_id = submit_athena_query(source_id, class_name, glue_db, athena_out)
            execution_ids = {}
            for resource in valid_resources:
                class_name = resource["class_name"]
                key_column = resource["key_column"]
                execution_id = submit_athena_query(
                    source_id, class_name, key_column, glue_db, athena_out
                )
                execution_ids[class_name] = execution_id
                logger.info({"source_id": source_id, "class_name": class_name,
                             "execution_id": execution_id, "message": "Query submitted"})


            return {
                **event,
                "athena_query_done": True,
                "execution_ids": execution_ids,
            }
        elif source_data.get("athena_status") != "SUCCEEDED":
            # ── Second call: check ALL queries status ─────────────────
            athena        = boto3.client("athena")
            # execution_ids = source_data["execution_ids"]
            execution_ids = source_data.get("execution_ids", {})

            if not execution_ids:
                logger.error({"source_id": source_id, "message": "No execution_ids found in event"})
                raise Exception("execution_ids missing from event")


            overall_state = "SUCCEEDED"

            for class_name, execution_id in execution_ids.items():
                status = athena.get_query_execution(QueryExecutionId=execution_id)
                state  = status["QueryExecution"]["Status"]["State"]
                logger.info({"source_id": source_id, "class_name": class_name,
                             "athena_status": state})

                if state == "FAILED":
                    overall_state = "FAILED"
                    break
                elif state != "SUCCEEDED":
                    overall_state = "RUNNING"

            return {**event, "athena_status": overall_state}

        else:
            # ── Third call: load ALL resources into RDS ───────────────
            execution_ids = source_data["execution_ids"]

            db_secret = fetch_secrets(os.environ["rdsDatabase"])
            conn      = get_connection(db_secret, conn, context.get_remaining_time_in_millis())
            cursor    = conn.cursor()

            for resource in valid_resources:
                class_name   = resource["class_name"]
                execution_id = execution_ids.get(class_name)

                logger.info({
                "source_id": source_id,
                "class_name": class_name,
                "execution_id": execution_id,
                "message": "Processing resource"
                })

                if not execution_id:
                    logger.warning({"source_id": source_id, "class_name": class_name,
                                    "message": "No execution_id found for class skipping"})
                    continue

                csv_stream = get_athena_result(
                    source_id, class_name, execution_id, athena_out
                )

                logger.info({
                "source_id": source_id,
                "class_name": class_name,
                "csv_stream_found": csv_stream is not None,
                "message": "CSV stream check"
                })

                if csv_stream is None:
                    logger.info({"source_id": source_id, "class_name": class_name,
                                 "message": "No data from Athena"})
                    continue

                content = csv_stream.read()
                df = pd.read_csv(io.BytesIO(content), dtype=str, keep_default_na=False)

                logger.info({
                "source_id": source_id,
                "class_name": class_name,
                "rows": len(df),
                "columns": len(df.columns),
                "message": "DataFrame loaded"
                })
                
                df.drop(columns=["row_num"], inplace=True, errors="ignore")
                df.columns = df.columns.str.lower()
                df = df.replace({"": None, "nan": None, "None": None,
                                 "NULL": None, "NaN": None})

                if df.empty:
                    logger.info({"source_id": source_id, "class_name": class_name,
                                 "message": "Empty dataframe skipping"})
                    continue

                stale_cols = ["source_id", "batch_id", "source_last_update_date",
                              "y_creation_date", "y_last_update_date", "source_creation_date"]
                df.drop(columns=stale_cols, inplace=True, errors="ignore")
                df = adding_extra_columns(df, batch_creation_date, source_id, batch_id)
                df.drop(columns=["request_url", "request_params"],
                        inplace=True, errors="ignore")

                prestage_prefix = get_prestage_prefix(source_type)

                if source_type == "Trestle API":
                    # All Trestle sources use generic table without source_id
                    table_name = f"ps_trestle_{class_name.lower()}"
                elif source_type == "MLS Grid V2 API":
                    table_name = f"ps_mlsgrid_{class_name.lower()}_{source_id}"
                elif source_type == "R API":
                    table_name = f"ps_rapi_{class_name.lower()}_{source_id}"
                elif "bright" in source_type.lower():
                    if class_name.lower() == "property":
                        table_name = "ps_bright_listing"
                    else:
                        table_name = f"ps_bright_{class_name.lower()}"
                elif "rets" in source_type.lower():
                    table_name = f"ps_rets_{class_name.lower()}_{source_id}"
                else:
                    table_name = f"ps_{prestage_prefix}_{class_name.lower()}_{source_id}"

                table_creation_and_loading(
                    df, source_id, table_name, source_name,
                    cursor, conn, class_name
                )

                logger.info({"source_id": source_id, "class_name": class_name,
                             "message": "Athena → RDS completed", "rows": len(df)})

            return {**event, "status": True}

    except Exception as e:
        logger.error({"Error": str(e), "Traceback": traceback.format_exc()})
        raise

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()