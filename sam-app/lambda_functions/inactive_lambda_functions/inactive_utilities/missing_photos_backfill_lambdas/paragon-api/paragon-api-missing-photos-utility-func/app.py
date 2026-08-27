"""ParagonRels API - Missing Photos Download Utility"""

import requests
import pandas as pd
import boto3
import psycopg2
from psycopg2 import extras
import logging
import traceback
import os
import json
from botocore.exceptions import ClientError
from datetime import datetime, timezone
import time

logging.basicConfig(format="%(levelname)s - %(message)s", force=True)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class SecretManagerHelper:
    @staticmethod
    def get_secret(secret_name, region_name):
        """Fetch secrets from AWS Secrets Manager"""
        session = boto3.session.Session()
        client = session.client(service_name="secretsmanager", region_name=region_name)
        try:
            get_secret_value_response_db = client.get_secret_value(SecretId=secret_name)
            secret_db = get_secret_value_response_db["SecretString"]
            secret_dict_db = json.loads(secret_db)
            return secret_dict_db
        except ClientError as e:
            raise e


def db_conn(db_secret, sqlExecLimit):
    """Setup database connection"""
    db_username = db_secret.get("username")
    db_password = db_secret.get("password")
    db_host = db_secret.get("host")
    db_name = db_secret.get("dbname")
    db_port = db_secret.get("port")
    try:
        connection = psycopg2.connect(
            database=db_name,
            user=db_username,
            password=db_password,
            host=db_host,
            port=db_port,
            options=f"-c statement_timeout={int(sqlExecLimit)}",
        )
        return connection
    except Exception as e:
        log_msg = {
            "Message": "Connection not established",
            "Error": e,
            "Error At line": traceback.format_exc(),
        }
        logger.error(log_msg)
        raise


def fetch_listings_in_question(source_id, cursor, connection):
    """
    Fetch listings that have photo_count > 0 but no photos in listing_photo table
    Adapted for ParagonRels source_id structure
    """
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

    logger.info({
        "source_id": source_id,
        "listings_found": len(listing_ids),
        "message": f"{len(listing_ids)} listings fetched for processing (listings with photos but no photo records)."
    })

    return listing_ids, source_listing_ids


def fetch_source_credentials(source_id, cursor, connection):
    """
    Fetch ParagonRels API credentials from the source table.
    """
    query = """
        SELECT auth
        FROM source
        WHERE id = %s
    """
    cursor.execute(query, (source_id,))
    row = cursor.fetchone()

    if not row:
        raise Exception(f"No source credentials found for source_id={source_id}")

    auth = row[0]

    
    if isinstance(auth, str):
        auth = json.loads(auth)

    loginurl = auth["loginUrl"]
    token = auth["password"]

    # -------------------------------------------------------------
    # Some ParagonRels sources require an OAuth2 client-credentials flow
    # -------------------------------------------------------------
    if source_id in (904, 981):
        tokenUrl = auth["tokenUrl"]
        client_id = auth["user"]
        client_secret = auth["password"]

        token = create_token(tokenUrl, client_id, client_secret)

        if not token:
            raise Exception(f"Failed to generate OAuth2 token for source_id={source_id}")

    loginurl = loginurl.replace("$metadata", "")

    return {
        "login_url": loginurl,
        "token": token,
    }


def create_token(url, client_id, client_secret):
    """Request an OAuth2 access token via client_credentials grant."""
    headers = {"Content-Type": "application/x-www-form-urlencoded;charset=UTF-8"}
    data = {
        "grant_type": "client_credentials",
        "scope": "OData",
        "client_id": client_id,
        "client_secret": client_secret,
    }

    response = requests.post(url, headers=headers, data=data)

    if response.status_code == 200:
        return response.json().get("access_token")
    else:
        log_msg = {
            "message": "token generation failed",
            "status_code": response.status_code,
            "response_text": response.text,
        }
        logger.error(log_msg)
        return None


def get_column_mapping(source_id):
    """
    Most sources use: ResourceRecordKey, MediaUrl, Order
    Exception sources (904, 871) use: SystemID, Media_Url, Sequence
    """
    alternate_sources = [904, 871]

    if source_id in alternate_sources:
        return {
            'id_column': 'SystemID',
            'url_column': 'Media_Url',
            'order_column': 'Sequence',
            'select_clause': 'SystemID,Media_Url,Sequence',
            'is_alternate': True
        }
    else:
        return {
            'id_column': 'ListingKey',
            'url_column': 'MediaUrl',
            'order_column': 'Order',
            'select_clause': 'Media',
            'is_alternate': False
        }


def download_media_from_paragonrels(source_id, cursor_homelisting, homelisting_db_con, cursor_serverless, serverless_db_con):
    """
    Main function to download missing photos from ParagonRels API

    Args:
        source_id: The source ID to process
        cursor_homelisting: Cursor for HomeListing database (listing_p_active table)
        homelisting_db_con: Connection for HomeListing database
        cursor_serverless: Cursor for Serverless database (staging tables)
        serverless_db_con: Connection for Serverless database
    """

    listing_ids, source_listing_ids = fetch_listings_in_question(
        source_id, cursor_homelisting, homelisting_db_con
    )

    if not listing_ids:
        logger.warning({
            "source_id": source_id,
            "message": "No listings found with missing media."
        })
        return {
            "source_id": source_id,
            "status": False,
            "message": "No listings found with missing media.",
            "listings_processed": 0,
            "media_downloaded": 0
        }


    creds = fetch_source_credentials(source_id, cursor_homelisting, homelisting_db_con)

    headers = {"Authorization": f"Bearer {creds['token']}"}

 
    base_url = creds['login_url']
    if not base_url.endswith("/"):
        base_url += "/"

    if source_id in [904, 871]:
        media_endpoint = f"{base_url}Media"
    else:
        media_endpoint = f"{base_url}Property"

    
    column_mapping = get_column_mapping(source_id)

    logger.info({
        "step": "API_ENDPOINT",
        "media_endpoint": media_endpoint,
        "source_id": source_id,
        "column_mapping": column_mapping
    })

    
    media_df = pd.DataFrame()
    chunk_size = 50
    
    
    use_alternate_media_processing = source_id not in (904, 981)

    for i in range(0, len(source_listing_ids), chunk_size):
        chunk_ids = source_listing_ids[i:i + chunk_size]

        id_column = column_mapping['id_column']
        if source_id in [904, 871]: 
            id_filter = " or ".join([f"{id_column} eq {listing_id}" for listing_id in chunk_ids])
        else:
            id_filter = " or ".join([f"{id_column} eq '{listing_id}'" for listing_id in chunk_ids])

        params = {
            "$filter": id_filter,
            "$select": column_mapping['select_clause'],
            "$top": 200
        }

        try:
            logger.info({
                "step": "DOWNLOADING_CHUNK",
                "source_id": source_id,
                "chunk": i // chunk_size + 1,
                "listings_in_chunk": len(chunk_ids),
                "using_columns": column_mapping['select_clause'],
                "is_alternate": column_mapping['is_alternate'],
                "filter": id_filter[:100] + "..." if len(id_filter) > 100 else id_filter
            })

            response = requests.get(
                media_endpoint,
                headers=headers,
                params=params,
                timeout=60
            )
            response.raise_for_status()

            data = response.json()
            media_records = data.get("value", [])

          
            if use_alternate_media_processing and media_records:
                # For source_ids not in (904, 981), process each property's Media array
                processed_records = []
                for property_data in media_records:
                   
                    property_media = property_data.get("Media", [])
                    if property_media:
                       
                        resource_key = property_data.get("ListingKey", "")
                        for media in property_media:
                            processed_records.append({
                                "source_listing_id": resource_key,
                                "Order": media.get("Order", 0),
                                "MediaURL": media.get("MediaURL", "")
                            })
                
                # Convert processed records to DataFrame
                if processed_records:
                    chunk_df = pd.DataFrame(processed_records)
                else:
                    chunk_df = pd.DataFrame()
            else:
                
                if media_records:
                    chunk_df = pd.DataFrame(media_records)
                else:
                    chunk_df = pd.DataFrame()

            if not chunk_df.empty:
                media_df = pd.concat([media_df, chunk_df], ignore_index=True)
                logger.info({
                    "step": "CHUNK_DOWNLOADED",
                    "source_id": source_id,
                    "chunk": i // chunk_size + 1,
                    "records_in_chunk": len(chunk_df),
                    "processing_mode": "alternate" if use_alternate_media_processing else "standard"
                })

            
            next_link = data.get("@odata.nextLink")
            while next_link:
                response = requests.get(next_link, headers=headers, timeout=60)
                response.raise_for_status()
                data = response.json()
                media_records = data.get("value", [])
                
                if use_alternate_media_processing and media_records:
                    processed_records = []
                    for property_data in media_records:
                        property_media = property_data.get("Media", [])
                        if property_media:
                            resource_key = property_data.get("ResourceRecordKey", "")
                            for media in property_media:
                                processed_records.append({
                                    "ResourceRecordKey": resource_key,
                                    "Order": media.get("Order", 0),
                                    "MediaURL": media.get("MediaURL", "")
                                })
                    if processed_records:
                        chunk_df = pd.DataFrame(processed_records)
                    else:
                        chunk_df = pd.DataFrame()
                else:
                    if media_records:
                        chunk_df = pd.DataFrame(media_records)
                    else:
                        chunk_df = pd.DataFrame()
                
                if not chunk_df.empty:
                    media_df = pd.concat([media_df, chunk_df], ignore_index=True)
                next_link = data.get("@odata.nextLink")

        except requests.RequestException as e:
            logger.error({
                "source_id": source_id,
                "chunk": i // chunk_size + 1,
                "error": str(e),
                "message": "Failed to download media chunk"
            })
            continue

    
    if media_df.empty:
        logger.warning({
            "source_id": source_id,
            "message": "No media records were downloaded."
        })
        return {
            "source_id": source_id,
            "status": False,
            "message": "No media records.",
            "listings_processed": len(listing_ids),
            "media_downloaded": 0
        }

   
    if use_alternate_media_processing:
        # For source_ids not in (904, 981), we already have the processed structure
        id_column = 'source_listing_id'
        url_column = 'MediaURL'
        order_column = 'Order'
    else:
        # Original for source_ids in (904, 981)
        id_column = column_mapping['id_column']
        url_column = column_mapping['url_column']
        order_column = column_mapping['order_column']

    required_cols = [id_column, url_column, order_column]

    for col in required_cols:
        if col not in media_df.columns:
            logger.warning({
                "source_id": source_id,
                "missing_column": col,
                "available_columns": list(media_df.columns),
                "message": f"Column {col} not found in response"
            })
            media_df[col] = None

    media_df[order_column] = pd.to_numeric(
        media_df[order_column], errors="coerce"
    ).fillna(0).astype(int)

    ordered_media_df = media_df.sort_values(
        by=[id_column, order_column]
    ).reset_index(drop=True)

    selected_media_df = ordered_media_df[[id_column, url_column, order_column]]
    selected_media_df.insert(0, "source_id", int(source_id))

    final_media_df = selected_media_df.drop_duplicates()
    if not use_alternate_media_processing:
        final_media_df = final_media_df.rename(
            columns={id_column: "source_listing_id", url_column: "media_url", order_column: "photo_order"}
        )

    else:
        
        final_media_df = final_media_df.rename(
            columns={url_column: "media_url", order_column: "photo_order"}
    )

    if final_media_df.empty:
        logger.warning({
            "source_id": source_id,
            "message": "No media records to load into staging table."
        })
        return {
            "source_id": source_id,
            "status": False,
            "message": "No media to load.",
            "listings_processed": len(listing_ids),
            "media_downloaded": 0
        }

   
    loading_data_into_staging_table(
        final_media_df,
        source_id,
        cursor_homelisting,
        homelisting_db_con
    )

    logger.info({
        "source_id": source_id,
        "staging_records": len(final_media_df),
        "column_mapping_used": column_mapping,
        "processing_mode": "alternate" if use_alternate_media_processing else "standard",
        "message": f"{len(final_media_df)} records have been loaded into staging table."
    })

    
    stagging_to_target_updation(
        source_id, listing_ids, cursor_homelisting, homelisting_db_con
    )

    return {
        "source_id": source_id,
        "status": True,
        "message": "Media records successfully loaded.",
        "listings_processed": len(listing_ids),
        "media_downloaded": len(final_media_df),
        "column_mapping_used": column_mapping,
        "processing_mode": "alternate" if use_alternate_media_processing else "standard"
    }
def loading_data_into_staging_table(df, source_id, cursor, connection):
    """
    Load media data into staging table (uses homelisting database)
    The stage.direct_idx_photo table exists in homelisting database
    """
    table_name = "stage.direct_idx_photo"

    delete_query = f"DELETE FROM {table_name} WHERE source_id = %s"
    cursor.execute(delete_query, (source_id,))
    connection.commit()

    cols = ",".join(list(df.columns))
    data_values = [tuple(row) for row in df.values]
    insert_query = f"INSERT INTO {table_name} ({cols}) VALUES %s"
    extras.execute_values(cursor, insert_query, data_values)
    connection.commit()

    logger.info({
        "source_id": source_id,
        "records_inserted": len(data_values),
        "table": table_name,
        "message": f"Loaded {len(data_values)} records into {table_name}"
    })


def stagging_to_target_updation(source_id, listing_ids, cursor, connection):
    """
    Move data from staging to target listing_photo table (uses homelisting database)
    """
    if not listing_ids:
        logger.info("No listing_ids to process for target updation.")
        return

    listing_ids_tuple = tuple(listing_ids)

    delete_query = "DELETE FROM listing_photo WHERE listing_id IN %s"
    cursor.execute(delete_query, (listing_ids_tuple,))
    connection.commit()

    logger.info({
        "source_id": source_id,
        "deleted_records": len(listing_ids),
        "message": f"Deleted existing photo records for {len(listing_ids)} listings."
    })

    select_query = """
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
        AND NOT EXISTS (
            SELECT 1 FROM listing_photo lp 
            WHERE l.id = lp.listing_id
        )
    """

    cursor.execute(select_query, (source_id, listing_ids_tuple))
    result = cursor.fetchall()

    if not result:
        logger.info("No records found in stage table to insert.")
        return

    column_names = [desc[0] for desc in cursor.description]
    target_cols = ",".join(list(column_names))

    insert_query = f"INSERT INTO public.listing_photo ({target_cols}) VALUES %s"
    extras.execute_values(cursor, insert_query, result)
    connection.commit()

    logger.info({
        "source_id": source_id,
        "records_inserted": len(result),
        "listings_processed": len(listing_ids),
        "message": f"{len(result)} records have been loaded into target table for {len(listing_ids)} listings."
    })


def lambda_handler(event, context):
    """
    Lambda handler for ParagonRels missing photos download
    """
    source_id = event.get('source_id')
    if not source_id:
        source_id = 904  # Example ParagonRels source_id

    serverless_db_con = None
    homelisting_db_con = None
    cursor_serverless = None
    cursor_homelisting = None

    try:
        sqlExecLimit = context.get_remaining_time_in_millis() if context else 300000

        rdsDatabase = os.environ.get("rdsDatabase")
        listingDatabase = os.environ.get("listingDatabase")

        if not rdsDatabase or not listingDatabase:
            raise Exception("Database environment variables not set")

        db_secret_serverless = SecretManagerHelper.get_secret(rdsDatabase, "us-west-2")
        db_secret_homelisting = SecretManagerHelper.get_secret(listingDatabase, "us-west-2")

        serverless_db_con = db_conn(db_secret_serverless, sqlExecLimit)
        homelisting_db_con = db_conn(db_secret_homelisting, sqlExecLimit)

        cursor_serverless = serverless_db_con.cursor()
        cursor_homelisting = homelisting_db_con.cursor()

        logger.info({
            "source_id": source_id,
            "step": "CONNECTIONS_ESTABLISHED",
            "message": "Database connections established successfully"
        })

        final_response = download_media_from_paragonrels(
            source_id,
            cursor_homelisting,
            homelisting_db_con,
            cursor_serverless,
            serverless_db_con
        )

        logger.info({
            "source_id": source_id,
            "response": final_response,
            "status": "SUCCESS"
        })

        return final_response

    except Exception as e:
        log_msg = {
            "Error": str(e),
            "Error At Line": traceback.format_exc(),
            "source_id": source_id,
            "status": False
        }
        logger.error(log_msg)
        return {
            "source_id": source_id,
            "status": False,
            "message": str(e)
        }

    finally:
        if cursor_serverless:
            cursor_serverless.close()
        if cursor_homelisting:
            cursor_homelisting.close()
        if serverless_db_con:
            serverless_db_con.close()
        if homelisting_db_con:
            homelisting_db_con.close()

        logger.info({
            "source_id": source_id,
            "step": "CLEANUP",
            "message": "Database connections closed"
        })
