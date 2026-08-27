"""Lambda function for downloading and processing OpenHouse data for ParagonRels RETS Sources."""

import json
import os
import re
import ssl
import traceback
import logging
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

import boto3
import certifi
import pandas as pd
import psycopg2
from psycopg2 import extras
import requests
from requests.auth import HTTPBasicAuth
from requests.adapters import HTTPAdapter
from urllib3.poolmanager import PoolManager


logging.basicConfig(format="%(levelname)s - %(message)s", force=True)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def fetch_secrets(secret_name):
    """Fetches secrets from AWS Secrets Manager."""
    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response["SecretString"])


def db_conn(db_secret, sql_exec_limit):
    """Establishes a connection to the PostgreSQL database."""
    try:
        connection = psycopg2.connect(
            database=db_secret.get("dbname"),
            user=db_secret.get("username"),
            password=db_secret.get("password"),
            host=db_secret.get("host"),
            port=db_secret.get("port"),
            options=f"-c statement_timeout={sql_exec_limit}",
        )
        return connection
    except Exception as e:
        log_msg = {
            "Message": "Connection not established",
            "Error": e,
            "Error At line": traceback.format_exc(),
        }
        logger.exception("Error while establishing connection: %s", log_msg)


class HostHeaderSSLAdapter(HTTPAdapter):
    """Custom SSL adapter that forces SNI to match the certificate hostname."""

    def __init__(self, dest_host, **kwargs):
        self.dest_host = dest_host
        super().__init__(**kwargs)

    def init_poolmanager(self, *args, **kwargs):
        context = ssl.create_default_context(cafile=certifi.where())
        kwargs["ssl_context"] = context
        kwargs["server_hostname"] = self.dest_host
        self.poolmanager = PoolManager(*args, **kwargs)


def login_rets(login_url, username, password, user_agent):
    """SSL-pinned login for sources that require it (source_ids 798, 604)."""
    session = requests.Session()
    parsed_url = urlparse(login_url)

    adapter = HostHeaderSSLAdapter(dest_host="rets.paragonrels.com")
    session.mount("https://", adapter)
    session.auth = (username, password)
    session.headers.update({
        "User-Agent": user_agent,
        "RETS-Version": "RETS/1.7.2",
        "Accept": "*/*",
        "Host": parsed_url.netloc,
    })

    try:
        response = session.get(login_url)
        response.raise_for_status()
        return response, session
    except requests.exceptions.SSLError as e:
        logger.error(f"SSL verification failed: {e}")
        return None, None
    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed: {e}")
        return None, None


def login(data):
    """RETS Server Login — ParagonRels-aware, including SSL branching for sources 798/604."""
    login_url   = data["loginUrl"]
    username    = data["user"]
    password    = data["password"]
    source_id   = data["source_id"]
    source_name = data["name"]

    session = requests.Session()
    session.auth = HTTPBasicAuth(username, password)
    session.headers.update({"rets-version": "RETS/1.8"})

    response = session.get(login_url)

    response_text = response.text

    try:
        root = ET.fromstring(response_text)
        logger.info(response_text)
        rets_response_text = root.find("RETS-RESPONSE").text.strip()  # type: ignore
        rets_data = dict(re.findall(r"(\w+)=([^\n\r]*)", rets_response_text))
        logger.info("Login successful!")
        rets_data["source_id"] = source_id
        rets_data["name"]      = source_name
        rets_data["session"]   = session
        return rets_data

    except Exception as e:
        try:
            reply_text = ET.fromstring(response_text).get("ReplyText")
        except Exception:
            reply_text = response_text
        logger.error({
            "Level": "Error",
            "Source": f"ID={source_id} Name={source_name}",
            "Function": "login()",
            "Message": reply_text,
        })
        return None


def request_source(data):
    """Download one page of RETS search results.
    Returns (DataFrame, total_record_count).
    On RETS errors or no records found returns (empty DataFrame, 0).
    """
    session     = data["session"]
    query_params = data["query_params"]

    # ParagonRels: reconstruct Search URL from Login URL
    search_url = data["Login"]
    if "paragonrels" in search_url:
        search_url = search_url.split("/rets")[0] + data["Search"]
    else:
        search_url = data["Search"]

    query_params["QueryType"] = "DMQL2"
    query_params["Format"]    = "COMPACT-DECODED"
    query_params["Count"]     = "1"

    response      = session.get(search_url, params=query_params)
    response_text = response.text

    try:
        root = ET.fromstring(response_text)
        reply_code = root.attrib.get("ReplyCode", "0")
        reply_text = root.attrib.get("ReplyText", "")
        if reply_code != "0":
            if "No Records Found" in reply_text:
                return pd.DataFrame(), 0
            logger.error(f"{reply_text} [ReplyCode={reply_code}] | Query={query_params}")
            return pd.DataFrame(), 0

        count_element = root.find(".//COUNT")
        if count_element is None:
            logger.error(f"COUNT element missing | Query={query_params}")
            return pd.DataFrame(), 0

        data_count = int(count_element.get("Records", 0))
        columns    = root.find("./COLUMNS").text.split("\t")[1:-1]  # type: ignore
        data_rows  = [elem.text.split("\t")[1:-1] for elem in root.findall("./DATA")]
        return pd.DataFrame(data_rows, columns=columns), data_count

    except Exception as e:
        try:
            reply_text = ET.fromstring(response_text).attrib.get("ReplyText", "")
            if "No Records Found" in reply_text:
                return pd.DataFrame(), 0
            logger.error(f"{reply_text} Error={e} | Query={query_params}")
            return pd.DataFrame(), 0
        except Exception as inner_e:
            logger.error(f"Unparseable RETS response. Error={inner_e} | Query={query_params}")
            return pd.DataFrame(), 0



def add_default_columns(source_id, batch_id, openhouse_df):
    """Prepend source_id, batch_id, y_creation_date, y_update_date columns."""
    current_datetime = datetime.now()
    meta_df = pd.DataFrame({
        "source_id":      [int(source_id)],
        "batch_id":       [int(batch_id)],
        "y_creation_date": current_datetime,
        "y_update_date":   current_datetime,
    })
    meta_df      = pd.concat([meta_df] * len(openhouse_df), ignore_index=True)
    openhouse_df = openhouse_df.reset_index(drop=True)
    openhouse_df = pd.concat([meta_df, openhouse_df], axis=1)
    return openhouse_df


def download_all_openhouse(source_data, cursor_rds, rets_response):
    """Download all OpenHouse records with OH_StartDate >= yesterday.
    """
    source_id   = source_data["source_id"]
    source_name = source_data["source_name"]
    last_day_time = (datetime.now(timezone.utc) - timedelta(days=1)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    # last_day_time=(datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    if source_id in (281,294,301,317,367,382,404,416,439,474):
        select_fields = "L_ListingID,OH_StartDateTime,OH_StartTime,OH_EndTime"
        hitting_query = f"(OH_StartDateTime={last_day_time}+)"
    elif source_id in (478,673):
        select_fields = "L_ListingID,OH_StartDateTime,OH_StartDateTime,OH_EndDateTime"
        hitting_query = f"(OH_StartDateTime={last_day_time}+)"
    elif source_id in (258,):
        select_fields = "L_DisplayId,OH_StartDate,OH_StartTime,OH_EndTime"
        hitting_query = f"(OH_StartDate={last_day_time}+)"

    else:
        select_fields = "L_ListingID,OH_StartDate,OH_StartTime,OH_EndTime"
        hitting_query = f"(OH_StartDate={last_day_time}+)"


    # Date filter: OH_StartDate >= yesterday  (DMQL2 "+" = greater-or-equal)
    
    

    query_params = {
        "SearchType": "OpenHouse",
        "Query":      hitting_query,
        "Select":     select_fields,
        "Limit":      1000,
    }

    # Fetch OpenHouse classes from class_metadata
    cursor_rds.execute(f"""
        SELECT class_name FROM dev.class_metadata
        WHERE source_id = {source_id}
          AND resource_name ~* 'openhouse'
          AND download_flag = 't'
    """)
    classes = [row[0] for row in cursor_rds.fetchall()]

    temp_openhouse_df = pd.DataFrame()

    for class_name in classes:
        query_params["Class"] = class_name
        skip = 0

        while True:
            query_params["Offset"]         = skip
            rets_response["query_params"]  = query_params

            df, data_count = request_source(rets_response)

            if data_count > 0:
                temp_openhouse_df = pd.concat([temp_openhouse_df, df], ignore_index=True)
            elif skip == 0:
                logger.warning({
                    "source_id":   source_id,
                    "source_name": source_name,
                    "Message":     f"No OpenHouse records found for class_name: {class_name}",
                })

            skip += len(df)
            if skip >= data_count:
                break

        logger.info({
            "source_id":   source_id,
            "source_name": source_name,
            "Message":     f"{skip} OpenHouse records found for class_name: {class_name}",
        })

    return temp_openhouse_df, select_fields


def columns_renaming(final_df, source_id, resource_name, system_names, cursor_rds):
    """Rename DataFrame columns from RETS system_names to lower(long_name)
    using dev.field_metadata"""
    system_names_sql = "'" + system_names.replace(",", "','") + "'"
    cursor_rds.execute(f"""
        SELECT DISTINCT lower(long_name), system_name
        FROM dev.field_metadata
        WHERE source_id     = {source_id}
          AND resource_name ~* '{resource_name}'
          AND system_name IN ({system_names_sql})
    """)
    for long_name, system_name in cursor_rds.fetchall():
        final_df.rename(columns={system_name: long_name}, inplace=True)
    return final_df


def fetch_openhouse_mappings(source_id, cursor_rds):
    """Fetch ordered business_transformation expressions from etl.mappings.
    Returns list: [source_listing_id_expr, date_expr, start_time_expr, end_time_expr]
    """
    cursor_rds.execute(f"""
        SELECT business_transformation
        FROM etl.mappings
        WHERE source_id = {source_id}
          AND resource_name ~* 'OpenHouse'
          AND replace(target_column, '"', '') IN ('date', 'start_time', 'end_time', 'source_listing_id')
        ORDER BY
            CASE replace(target_column, '"', '')
                WHEN 'source_listing_id' THEN 0
                WHEN 'date'              THEN 1
                WHEN 'start_time'        THEN 2
                WHEN 'end_time'          THEN 3
            END
    """)
    return [row[0] for row in cursor_rds.fetchall()]


def insert_and_transform_openhouse_data(
    source_data,
    openhouse_df,
    system_names,
    homelisting_connection,
    cursor_homelisting,
    cursor_rds,
):
    """Rename columns, load into temp table, then INSERT into stage target.
    """
    source_id          = source_data["source_id"]
    batch_id           = source_data["batch_id"]
    temp_table         = f"temp_openhouse_sync_{source_id}"
    stage_target_table = "stage.direct_idx_openhouse_sync"

    # 1. Rename RETS system_names → long_names via field_metadata
    openhouse_df = columns_renaming(
        openhouse_df, source_id, "OpenHouse", system_names, cursor_rds
    )

    # 2. Create temp table (all TEXT columns — metadata provides the schema)
    temp_table_fields = ", ".join([f"{col} TEXT" for col in openhouse_df.columns])
    cursor_homelisting.execute(f"""
        CREATE TEMP TABLE IF NOT EXISTS {temp_table} (
            {temp_table_fields}
        ) ON COMMIT PRESERVE ROWS;
    """)
    cursor_homelisting.execute(f"""
        ALTER TABLE {temp_table}
            ADD COLUMN IF NOT EXISTS y_update_date   TIMESTAMP,
            ADD COLUMN IF NOT EXISTS y_creation_date TIMESTAMP,
            ADD COLUMN IF NOT EXISTS batch_id        BIGINT,
            ADD COLUMN IF NOT EXISTS source_id       INT;
    """)
    homelisting_connection.commit()

    # 3. Add metadata columns and insert into temp table
    openhouse_df = add_default_columns(source_id, batch_id, openhouse_df)
    insertion_columns    = ", ".join(list(openhouse_df.columns))
    insertion_data_values = [tuple(row) for row in openhouse_df.values]
    extras.execute_values(
        cursor_homelisting,
        f"INSERT INTO {temp_table} ({insertion_columns}) VALUES %s",
        insertion_data_values,
    )
    homelisting_connection.commit()

    # 4. Fetch dynamic mapping expressions from etl.mappings
    business_transformations = fetch_openhouse_mappings(source_id, cursor_rds)

    # 5. Delete stale rows from staging table
    cursor_homelisting.execute(
        f"DELETE FROM {stage_target_table} WHERE source_id = {source_id}"
    )
    homelisting_connection.commit()

    # 6. Insert transformed rows into staging table
    cursor_homelisting.execute(f"""
        INSERT INTO {stage_target_table}
            (source_id, batch_id, y_creation_date, y_update_date,
             ListingKey, openhousedate, openhousestarttime, openhouseendtime)
        SELECT DISTINCT
            source_id, batch_id, y_creation_date, y_update_date,
            {business_transformations[0]},
            {business_transformations[1]},
            {business_transformations[2]},
            {business_transformations[3]}
        FROM {temp_table} o
        WHERE source_id = {source_id}
          AND batch_id  = {batch_id}
    """)
    homelisting_connection.commit()


# =============================================================================
# Lambda entry point  (mirrors MlxMatrix lambda_handler structure)
# =============================================================================

def lambda_handler(event, context):
    """Main Lambda Handler Function."""

    try:
        source_data = event
        source_id   = source_data["source_id"]
        source_name = source_data["source_name"]
        source_type = source_data["source_info"]["source_type"]
        mls_board   = source_data["source_info"].get("mls_board")
        batch_id    = source_data["batch_id"]
        auth        = source_data["auth"]
        auth["source_id"] = source_id
        auth["name"]      = source_name

        # Database connections
        listing_secret = os.environ.get("listingDatabase")
        rds_secret     = os.environ.get("rdsDatabase")
        sql_exec_limit = context.get_remaining_time_in_millis()

        rds_secrets     = fetch_secrets(rds_secret)
        listing_secrets = fetch_secrets(listing_secret)

        rds_connection        = db_conn(rds_secrets,     sql_exec_limit)
        homelisting_connection = db_conn(listing_secrets, sql_exec_limit)

        cursor_rds         = rds_connection.cursor()         # type: ignore
        cursor_homelisting = homelisting_connection.cursor() # type: ignore

        # Login to RETS
        rets_response = login(auth)
        if not rets_response:
            raise ConnectionError(f"RETS login failed for source_id={source_id}")
        rets_response["Login"] = auth["loginUrl"]

        # Download OpenHouse data
        openhouse_df, system_names = download_all_openhouse(
            source_data, cursor_rds, rets_response
        )
        openhouse_df = openhouse_df.drop_duplicates()

        if openhouse_df.empty:
            logger.info({
                "source_id":   source_id,
                "source_name": source_name,
                "Message":     "No OpenHouse records found — skipping insert",
            })
        else:
            # Clear stale staging rows then insert fresh data
            cursor_homelisting.execute(
                "DELETE FROM stage.direct_idx_openhouse_sync WHERE source_id = %s",
                (source_id,),
            )
            homelisting_connection.commit()

            insert_and_transform_openhouse_data(
                source_data,
                openhouse_df,
                system_names,
                homelisting_connection,
                cursor_homelisting,
                cursor_rds,
            )

        source_data["openhouse_records_downloaded"] = len(openhouse_df)
        source_data["download_status"] = True
        source_data["success"]         = True
        logger.info(source_data)

    except Exception as e:
        log_msg = {
            "download_status": False,
            "success":         False,
            "Error":           str(e),
            "Error At line":   traceback.format_exc(),
        }
        source_data.update(log_msg)
        logger.error(source_data)

    finally:
        if cursor_rds:
            cursor_rds.close()
            rds_connection.close()         
        if cursor_homelisting:
            cursor_homelisting.close()
            homelisting_connection.close()

    return source_data