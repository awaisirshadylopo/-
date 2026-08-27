import json
import boto3
import pandas as pd
import psycopg2
from psycopg2 import extras
from datetime import datetime
import os
import traceback
import requests
from requests.auth import HTTPDigestAuth
import xml.etree.ElementTree as ET

# from helper import LogData, LogMessage, log_message


class LogData:
    def __init__(self, message=None, event=None, query=None, **kwargs):
        self.message = message
        self.event = event
        self.query = query
        for key, value in kwargs.items():
            setattr(self, key, value)

    def to_dict(self):
        return {k: v for k, v in self.__dict__.items() if v is not None}


class LogMessage:
    def __init__(self, level, message, data=None):
        self.level = level
        self.message = message
        self.data = data
        self.timestamp = datetime.utcnow().isoformat()

    def to_dict(self):
        result = {
            "level": self.level,
            "message": self.message,
            "timestamp": self.timestamp,
        }
        if self.data:
            if hasattr(self.data, "to_dict"):
                result["data"] = self.data.to_dict()
            else:
                result["data"] = self.data
        return result


def log_message(log_msg):
    """Simple logging function that prints to console (CloudWatch in Lambda)"""
    if hasattr(log_msg, "to_dict"):
        print(json.dumps(log_msg.to_dict()))
    else:
        print(
            json.dumps(
                {
                    "level": "INFO",
                    "message": str(log_msg),
                    "timestamp": datetime.utcnow().isoformat(),
                }
            )
        )


# Function to fetch secrets from AWS Secrets Manager
def fetch_secrets(secret_name):
    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId=secret_name)
    secret = json.loads(response["SecretString"])
    return secret


def setup_db_connection(secret):
    db_user = secret["username"]
    db_password = secret["password"]
    db_host = secret["host"]
    db_port = secret["port"]
    db_name = secret["dbname"]
    conn = psycopg2.connect(
        dbname=db_name, user=db_user, password=db_password, host=db_host, port=db_port
    )

    return conn


def fetch_listings_in_question(source_id, cursor, connection):
    """
    Fetch listings that have photo_count > 0 but no photos in listing_photo table
    Using simplified LEFT JOIN query
    """
    # Updated query using your new logic
    query = """
    SELECT 
        l.id,
        l.source_listing_id
    FROM listing_p_active l
    WHERE l.source_id = %s
      AND l.photo_count > 0
      AND NOT EXISTS (
            SELECT 1
            FROM listing_photo lp
            WHERE lp.listing_id = l.id
        );
      
    """

    cursor.execute(query, (source_id,))
    result = cursor.fetchall()

    listing_ids = [row[0] for row in result]
    source_listing_ids = [row[1] for row in result]

    log_message(
        LogMessage(
            "INFO",
            f"{len(listing_ids)} listings fetched for processing (listings with photos but no photo records).",
        )
    )
    return listing_ids, source_listing_ids


def fetch_source_credentials(source_id, cursor, connection):
    query = """SELECT auth ->> 'loginUrl' AS login_url, auth ->> 'user' AS username, auth ->> 'password' AS password FROM source WHERE id = %s;"""
    cursor.execute(query, (source_id,))
    result = cursor.fetchall()
    login_url, username, password = result[0]
    return login_url, username, password


def loading_data_into_stagging_table(df, source_id, cursor, connection):
    table_name = "stage.direct_idx_photo"

    # Only delete data for this specific source_id
    previous_data_deletion = """DELETE FROM {0} WHERE source_id = %s""".format(
        table_name
    )
    cursor.execute(previous_data_deletion, (source_id,))
    connection.commit()

    cols = ",".join(list(df.columns))
    data_values = [tuple(row) for row in df.values]
    insert_query = """INSERT INTO {0} ({1}) VALUES %s""".format(table_name, cols)
    extras.execute_values(cursor, insert_query, data_values)
    connection.commit()


def stagging_to_target_updation(source_id, listing_ids, cursor, connection):
    if not listing_ids:
        log_message(
            LogMessage("INFO", "No listing_ids to process for target updation.")
        )
        return

    # Convert listing_ids to tuple for SQL IN clause
    listing_ids_tuple = tuple(listing_ids)

    # Delete existing listing_photo records for these listings
    previous_listing_photo_deletion = (
        """DELETE FROM listing_photo WHERE listing_id IN %s;"""
    )
    cursor.execute(previous_listing_photo_deletion, (listing_ids_tuple,))
    connection.commit()
    log_message(
        LogMessage(
            "INFO", f"Deleted existing photo records for {len(listing_ids)} listings."
        )
    )

    # Select data from stage - only for listings that still don't have photos
    select_data_from_stage = """
    SELECT 
        l.id AS listing_id, 
        l.batch_id, 
        l.media_modification_timestamp, 
        s.media_url,
        l.source_creation_date, 
        l.source_last_update_date, 
        l.y_creation_date, 
        l.y_last_update_date
    FROM stage.direct_idx_photo s 
    JOIN listing_p_active l ON s.source_listing_id = l.source_listing_id
        AND s.source_id = l.source_id 
    WHERE s.source_id = %s 
        AND l.id IN %s
        AND NOT EXISTS (SELECT 1 FROM listing_photo lp WHERE l.id = lp.listing_id)
    """

    cursor.execute(select_data_from_stage, (source_id, listing_ids_tuple))
    result = cursor.fetchall()
    log_message(
        LogMessage(
            "INFO", f"Stage Data Selection Query returned {len(result)} records."
        )
    )

    if not result:
        log_message(LogMessage("INFO", "No records found in stage table to insert."))
        return

    column_names = [desc[0] for desc in cursor.description]
    target_cols = ",".join(list(column_names))

    target_insert_query = """INSERT INTO public.listing_photo ({}) VALUES %s""".format(
        target_cols
    )
    extras.execute_values(cursor, target_insert_query, result)
    connection.commit()
    log_message(
        LogMessage(
            "INFO",
            f"{len(result)} records have been loaded into target table for {len(listing_ids)} listings.",
        )
    )


def download_media_from_source_and_load_into_tables(source_id, cursor, connection):

    listing_ids, source_listing_ids = fetch_listings_in_question(
        source_id, cursor, connection
    )

    if not listing_ids:
        log_message(
            LogMessage(
                "WARN",
                f"No listings found with missing media for source_id: {source_id}.",
            )
        )
        return {
            "source_id": source_id,
            "status": False,
            "message": "No listings found with missing media.",
        }

    login_url, username, password = fetch_source_credentials(
        source_id, cursor, connection
    )

    headers = {"Cookie": "ASP.NET_SessionId=41ebxi55z1zalfmjjst0qzqu"}

    try:
        response = requests.get(
            login_url, headers=headers, auth=HTTPDigestAuth(username, password)
        )
        response.raise_for_status()
    except requests.RequestException as e:
        log_message(LogMessage("ERROR", f"Failed to login session: {e}"))
        raise

    media_df = pd.DataFrame()
    chunk_size = 50
    media_ids = [
        source_listing_ids[i : i + chunk_size]
        for i in range(0, len(source_listing_ids), chunk_size)
    ]

    for chunk in media_ids:
        ids = (
            str(chunk)
            .replace("[", "")
            .replace("]", "")
            .replace("'", "")
            .replace(", ,", ",")
        )
        if ids == "":
            continue

        url = login_url.replace("Login", "Search")
        query = f"((SourceID={ids}),(DeletedYN=0),(MediaCategory=|2))"

        params = {
            "SearchType": "Media",
            "QueryType": "DMQL2",
            "Format": "COMPACT-DECODED",
            "Class": "Media",
            "Count": "1",
            "Query": query,
        }

        try:
            response = requests.get(
                url,
                headers=headers,
                auth=HTTPDigestAuth(username, password),
                params=params,
            )
            response.raise_for_status()

            root = ET.fromstring(response.text)
            # total_count = int(root.find('.//COUNT').get('Records'))
            count_node = root.find(".//COUNT")
            if count_node is None:
                reply_text = root.attrib.get("ReplyText")
                raise Exception(f"COUNT node missing. ReplyText={reply_text}")
            total_count = int(count_node.get("Records", 0))
            columns = root.find("./COLUMNS").text.split("\t")[1:-1]

            data_rows = [
                data_element.text.split("\t")[1:-1]
                for data_element in root.findall("./DATA")
            ]

            chunk_df = pd.DataFrame(data_rows, columns=columns)
            media_df = pd.concat([media_df, chunk_df], ignore_index=True)

        except Exception as e:
            reply_text = ET.fromstring(response.text).attrib.get("ReplyText")
            log_data = LogData(message=reply_text, event=str(e), query=query)
            log_message(
                LogMessage(
                    "ERROR", f"Couldn't download any media! Exception: {e}", log_data
                )
            )
            continue

    if media_df.empty:
        log_message(LogMessage("WARN", "No media records were downloaded."))
        return {"source_id": source_id, "status": False, "message": "No media records."}

    media_df["MediaOrder"] = pd.to_numeric(
        media_df["MediaOrder"], errors="coerce"
    ).astype("Int64")
    ordered_media_df = media_df.sort_values(by=["SourceID", "MediaOrder"]).reset_index(
        drop=True
    )
    selected_media_df = ordered_media_df[["SourceID", "MediaURL"]]
    selected_media_df.insert(0, "source_id", int(source_id))
    unique_media_df = selected_media_df.drop_duplicates()
    final_media_df = unique_media_df.rename(
        columns={"SourceID": "source_listing_id", "MediaURL": "media_url"}
    )

    if final_media_df.empty:
        log_message(LogMessage("WARN", "No media records to load into staging table."))
        return {"source_id": source_id, "status": False, "message": "No media to load."}

    loading_data_into_stagging_table(final_media_df, source_id, cursor, connection)
    log_message(
        LogMessage(
            "INFO",
            f"{len(final_media_df)} records have been loaded into staging table.",
        )
    )

    stagging_to_target_updation(source_id, listing_ids, cursor, connection)

    return {
        "source_id": source_id,
        "status": True,
        "message": "Media records successfully loaded.",
    }


def lambda_handler(event, context):
    # source_id = event.get('source_id')
    source_id = 306

    try:
        secret_name = os.environ.get("listingDatabase")
        secrets = fetch_secrets(secret_name)
        connection = setup_db_connection(secrets)
        cursor = connection.cursor()

        final_response = download_media_from_source_and_load_into_tables(
            source_id, cursor, connection
        )
        log_message(LogMessage("INFO", f"Response: {final_response}"))

        return final_response

    except Exception as e:
        log_msg = {
            "Error": str(e),
            "Error At line": traceback.format_exc(),
            "Payload": {"source_id": source_id, "status": False},
        }
        log_data = LogData(event=log_msg)
        log_message(LogMessage("ERROR", "Lambda execution failed.", log_data))
        return {"source_id": source_id, "status": False, "message": str(e)}

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
