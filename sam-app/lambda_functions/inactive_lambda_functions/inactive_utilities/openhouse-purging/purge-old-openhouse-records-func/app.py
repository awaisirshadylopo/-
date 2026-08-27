# Import necessary modules
import os
import logging
import psycopg2
import boto3
import json
import traceback


logger = logging.getLogger("purge-old-openhouse-records-func")
logger.setLevel("INFO")


# Function to fetch secrets from AWS Secrets Manager
def fetch_secrets(secret_name):
    """Fetches secrets from AWS Secrets Manager."""
    # Initialize AWS Secrets Manager client
    client = boto3.client("secretsmanager")

    # Retrieve secret value using the provided secret name
    response = client.get_secret_value(SecretId=secret_name)

    # Parse and return the secret as a dictionary
    secret = json.loads(response["SecretString"])
    return secret


# Function to set up a PostgreSQL database connection
def setup_db_connection(secret, sql_execLimit):
    """Establishes a connection to the PostgreSQL database using credentials from AWS Secrets Manager."""
    # Extract database connection parameters from the secret
    db_user = secret["username"]
    db_password = secret["password"]
    db_host = secret["host"]
    db_port = secret["port"]
    db_name = secret["dbname"]

    # Establish a connection to the PostgreSQL database
    conn = psycopg2.connect(
        dbname=db_name,
        user=db_user,
        password=db_password,
        host=db_host,
        port=db_port,
        options=f"-c statement_timeout={sql_execLimit}",
    )
    return conn


def delete_old_openhouses(source_id, cursor, conn):
    """
    Description:
        Deletes open house records older than yesterday for a given source_id.
            - Deletes records from listing_openhouse where the date is older than yesterday
            - Joins with listing_p_active to filter by source_id

    Parameters:
        source_id (int/str): The source identifier to filter listings.
        cursor (psycopg2.cursor): Database cursor for executing queries.
        conn (psycopg2.connection): Database connection object.

    Returns:
        dict: Logging information about the deletion query result.

    Created:    2025-12-17                  Create By : Ammar Azkar
    """
    # -------------------------------------------------------------
    # 1. Define the deletion query
    #    - Deletes records from listing_openhouse where the date is older than yesterday
    #    - Joins with listing_p_active to filter by source_id
    # -------------------------------------------------------------

    query = f"""
        DELETE FROM listing_openhouse
        WHERE id IN (
            SELECT o.id
            FROM listing l
            JOIN listing_openhouse o ON l.id = o.listing_id and l.source_id = {source_id}
            WHERE l.source_id = %s
              AND o.date <= CURRENT_DATE - 1
        );
    """

    try:
        # -------------------------------------------------------------
        # 2. Execute the deletion query with parameterized input
        # -------------------------------------------------------------
        cursor.execute(query, (source_id,))
        conn.commit()

        log_msg = {
            "source_id": source_id,
            "query_result": cursor.statusmessage,
            "query": query,
        }
        logger.info(log_msg)

        return log_msg

    except Exception as e:
        conn.rollback()
        logger.error(
            {
                "source_id": source_id,
                "error": str(e),
                "query": query,
            }
        )
        raise
    return True


def purge_removed_openhouses(source_id, batch_id, cursor, conn):
    """
    Description:
        Purge open house records from the database that are no longer provided
        by the source.

    Parameters:
        event (dict): Dictionary containing at least 'source_id'.
        cursor (psycopg2.cursor): Database cursor for executing queries.
        conn (psycopg2.connection): Database connection object.

    Returns:
        Count of deleted open house records or -1 in case of an error.

    Created:    2025-12-17                  Create By : Ammar Azkar
    """
    try:
        # -------------------------------------------------------------
        # 1. Check if there are any records for this batch in the staging table
        # -------------------------------------------------------------
        cursor.execute(
            """
            SELECT COUNT(*) 
            FROM stage.direct_idx_openhouse_sync 
            WHERE source_id = %s AND batch_id = %s;
        """,
            (source_id, batch_id),
        )
        count = cursor.fetchone()[0]

        if count == 0:
            logger.info(
                f"No records found in staging table for source_id={source_id}, batch_id={batch_id}. Purge skipped."
            )
            return True  # Nothing to purge, but not an error

        logger.info(
            {
                "source_id": source_id,
                "batch_id": batch_id,
                "direct_idx_openhouse_sync_count": count,
            }
        )

        # -------------------------------------------------------------
        # 2. Clear previous tracking entries for missing openhouses
        # -------------------------------------------------------------
        cursor.execute(
            """
            DELETE FROM stage.etl_direct_idx_missing_openhouse_delete_listings
            WHERE source_id = %s;
        """,
            (source_id,),
        )
        conn.commit()
        logger.info(
            f"Cleared previous entries for source_id={source_id} in etl_direct_idx_missing_openhouse_delete_listings."
        )

        # -----------------------------------------------------------------------------------------------------
        # 3. Identify openhouses that exist in the target table but are missing from the latest staging table
        # -----------------------------------------------------------------------------------------------------
        deletion_query = f"""
        WITH OHS_NORM AS (
            SELECT
                batch_id,
                source_id,
                ListingKey,
                COALESCE(openhousedate::DATE, openhousestarttime::DATE) AS openhousedate,
                openhousestarttime::time AS openhousestarttime,
                openhouseendtime::time AS openhouseendtime
            FROM stage.direct_idx_openhouse_sync
            WHERE source_id = {source_id} AND batch_id = {batch_id}
        ),
        OHT AS (
            SELECT
                l.source_id,
                l.source_listing_id,
                lo.listing_id AS Listing_Id,
                lo.id AS OpenHouse_id,
                lo.date AS openhousedate,
                lo.start_time::time AS openhousestarttime,
                lo.end_time::time AS openhouseendtime
            FROM listing l
            JOIN listing_openhouse lo
                ON lo.listing_id = l.id
            WHERE l.source_id = {source_id}
            GROUP BY
                l.source_id,
                l.source_listing_id,
                lo.listing_id,
                lo.id,
                lo.date,
                lo.start_time,
                lo.end_time
        )
        INSERT INTO stage.etl_direct_idx_missing_openhouse_delete_listings 
        (source_id, source_listing_id, target_listing_id, OpenHouse_id, Description)
        SELECT
            OHT.source_id,
            OHT.source_listing_id,
            OHT.Listing_Id AS target_listing_id,
            OHT.OpenHouse_id AS OpenHouse_id,
            'MissingOpenHouse' AS Description
        FROM OHT
        WHERE NOT EXISTS (
            SELECT 1
            FROM OHS_NORM OHS
            WHERE OHS.source_id = OHT.source_id
                AND OHS.ListingKey = OHT.source_listing_id
                AND OHS.openhousedate = OHT.openhousedate
                AND OHS.openhousestarttime = OHT.openhousestarttime
                AND OHS.openhouseendtime = OHT.openhouseendtime
        );
        """

        cursor.execute(deletion_query)
        inserted_count = cursor.rowcount  # Number of rows inserted
        conn.commit()

        logger.info(
            {
                "source_id": source_id,
                "batch_id": batch_id,
                "inserted_count": inserted_count,
                "message": "Missing openhouses identified and inserted for deletion.",
            }
        )

        # -------------------------------------------------------------
        # 4. Delete the obsolete openhouse records from the main table
        # -------------------------------------------------------------
        delete_query = """
        DELETE FROM listing_openhouse lo
        USING stage.etl_direct_idx_missing_openhouse_delete_listings m
        WHERE lo.listing_id = m.target_listing_id
          AND lo.id = m.OpenHouse_id
          AND m.source_id = %s;
        """
        cursor.execute(delete_query, (source_id,))
        deleted_count = cursor.rowcount
        conn.commit()

        logger.info(
            {
                "source_id": source_id,
                "batch_id": batch_id,
                "deleted_count": deleted_count,
                "message": "Obsolete openhouses deleted from listing_openhouse.",
            }
        )
        return inserted_count

    except Exception as e:
        # -------------------------------------------------------------
        # 5. Handle any exception and rollback
        # -------------------------------------------------------------
        conn.rollback()
        logger.error({"source_id": source_id, "batch_id": batch_id, "error": str(e)})
        raise


# Lambda function handler
def lambda_handler(event, context):
    """Lambda function to purge old open house records from the database."""
    # logger.info(event)
    source_id = event.get("source_id")
    batch_id = event.get("batch_id")

    # TODO implement
    # Fetching database secrets from AWS Secrets Manager
    secret_name = os.environ.get("listingDatabase")
    rdsDatabase = os.environ.get("rdsDatabase")
    sql_execLimit = context.get_remaining_time_in_millis()
    secrets = fetch_secrets(secret_name)
    dev_secrets = fetch_secrets(rdsDatabase)
    serverless_conn = setup_db_connection(dev_secrets, sql_execLimit)
    cursor_serverless = serverless_conn.cursor()

    # Setting up a database connection
    homelisting_conn = setup_db_connection(secrets, sql_execLimit)
    cursor_homelisting = homelisting_conn.cursor()
    try:
        delete_old_openhouses(source_id, cursor_homelisting, homelisting_conn)
        if event["success"] == True:
            deleted_count = purge_removed_openhouses(
                source_id, batch_id, cursor_homelisting, homelisting_conn
            )
            event["Openhouse_Pruged_count"] = deleted_count
        event["success"] = True
        event["status"] = True
        # event["Openhouse_Pruged_count"] = deleted_count
        return event

    except Exception as e:

        event["success"] = False
        # Log an error message and return a 500 status code with the error details
        log_msg = {"Error": str(e), "Error At line": traceback.format_exc()}
        event.update(log_msg)
        logger.error(event)

        return event

    finally:
        # Close the database cursor and connection in the finally block
        if cursor_homelisting:
            cursor_homelisting.close()
        if homelisting_conn:
            homelisting_conn.close()
