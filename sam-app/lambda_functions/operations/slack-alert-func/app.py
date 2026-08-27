import os
import json
import traceback
import logging
import warnings
import requests
import boto3
import psycopg2
import pandas as pd
from datetime import datetime, timedelta, timezone

warnings.filterwarnings("ignore", message=".*pandas only supports SQLAlchemy.*")

logging.basicConfig(format="%(levelname)s - %(message)s", force=True)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def fetch_secrets(secret_name):
    """Fetches secrets from AWS Secrets Manager."""
    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId=secret_name)
    secret = json.loads(response["SecretString"])
    return secret


def db_conn(db_secret, sql_execlimit):
    """
    Establishes a connection to the PostgreSQL database.
    """
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
            options=f"-c statement_timeout={sql_execlimit}",
        )
        logger.info("Connection established successfully")
        return connection
    except ConnectionError as e:
        log_msg = {"Error": e, "Error At line": traceback.format_exc()}
        logger.error(log_msg)


def fetch_alert_definitions(conn):
    """
    Returns all active alerts.
    We'll fetch all and apply due logic in Python so we can order by last_executed.
    """
    sql = """
      SELECT id, title, sql_query, active, threshold_runs,
        last_executed, 
       slack_channel
      FROM etl.slack_alerts
      WHERE active = true
      ORDER BY last_executed
    """
    df = pd.read_sql(sql, conn)
    return df.to_dict("records")


# Check if alert is due
def is_due(last_executed, threshold_runs):
    """
    Determines if an alert is due based on the last execution time and threshold runs.

    This function calculates whether enough time has passed since the last execution
    to trigger a new alert, based on a 15-minute interval multiplied by the threshold runs.

    Args:
        last_executed (datetime): The timestamp of the last execution in UTC.
            If None, indicates no previous execution.
        threshold_runs (int): Number of 15-minute intervals to wait between alerts.

    Returns:
        bool: True if an alert is due (enough time has passed), False otherwise.

    Example:
        >>> last_run = datetime(2023, 1, 1, tzinfo=timezone.utc)
        >>> is_due(last_run, 2)  # Checks if 30 minutes have passed since last_run
        True
    """
    if last_executed is None:
        return True
    next_due = last_executed + timedelta(minutes=15 * threshold_runs)
    return datetime.now(timezone.utc) >= next_due


def df_to_slack_table(df: pd.DataFrame, title=":rotating_light: ETL Pipeline Alerts"):
    """
    Convert a Pandas DataFrame into a Slack Block Kit formatted JSON with a table layout.
    """
    # Create header row with bold column names
    header_row = []
    for col in df.columns:
        header_row.append(
            {
                "type": "rich_text",
                "elements": [
                    {
                        "type": "rich_text_section",
                        "elements": [
                            {"type": "text", "text": str(col), "style": {"bold": True}}
                        ],
                    }
                ],
            }
        )

    # Create data rows
    data_rows = []
    for _, row in df.iterrows():
        cells = []
        for val in row:
            cells.append(
                {
                    "type": "rich_text",
                    "elements": [
                        {
                            "type": "rich_text_section",
                            "elements": [{"type": "text", "text": str(val)}],
                        }
                    ],
                }
            )
        data_rows.append(cells)

    # Combine header + data rows
    rows = [header_row] + data_rows

    # Construct final Slack Block JSON
    slack_message = {
        "blocks": [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": title, "emoji": True},
            },
            {"type": "divider"},
            {"type": "table", "rows": rows},
        ]
    }
    slack_message = str(slack_message).replace("True", "true")
    return slack_message


def send_slack_message(slack_webhook_url, message: str):
    """
    Sends a message to Slack using an Incoming Webhook.
    """
    headers = {"Content-Type": "application/json"}

    response = requests.post(slack_webhook_url, headers=headers, data=message)

    if response.status_code != 200:
        log_msg = {
            "status_code": response.status_code,
            "respose": response.text,
            "message_content": message,
        }
        raise Exception(log_msg)

    return {"ok": True, "status": response.status_code}


def lambda_handler(event, context):

    listing_secret = os.environ.get("listingDatabase")
    rds_secret = os.environ.get("rdsDatabase")
    sql_execlimit = context.get_remaining_time_in_millis()
    listing_secrets = fetch_secrets(listing_secret)
    rds_secret = fetch_secrets(rds_secret)
    slackAlert_secret = os.environ.get("slackAlert")
    slackAlert_secret = fetch_secrets(slackAlert_secret)
    listing_conn = db_conn(listing_secrets, sql_execlimit)
    rds_conn = db_conn(rds_secret, sql_execlimit)
    listing_cursor = listing_conn.cursor()  # type: ignore
    rds_cursor = rds_conn.cursor()  # type: ignore

    alerts = fetch_alert_definitions(rds_conn)
    if not alerts:
        return {"status": "no_active_alerts"}
    results = []

    for a in alerts:
        alert_id = a["id"]
        title = a["title"]
        query = a["sql_query"]
        threshold = int(a.get("threshold_runs", 1) or 1)
        last_exec = a.get("last_executed")
        last_exec = None if last_exec is pd.NaT else last_exec
        slack_channel = a.get("slack_channel")
        if not slack_channel:
            log_msg = {"id": alert_id, "status": "skipped - no channel", "title": title}
            logger.info(log_msg)
            results.append(log_msg)
            continue

        slack_webhook_url = slackAlert_secret[slack_channel]
        if not is_due(last_exec, threshold):
            log_msg = {"id": alert_id, "status": "threshold not met", "title": title, "threshold": threshold}
            logger.info(log_msg)
            results.append(log_msg)
            continue

        try:
            df = pd.read_sql_query(query, listing_conn)
            if df.empty:
                log_msg = {"id": alert_id, "status": "Result Not Found", "title": title}
                logger.info(log_msg)
                results.append(log_msg)
                continue

            message = df_to_slack_table(df, title)
            slack_response = send_slack_message(slack_webhook_url, message)

            rds_cursor.execute(
                "UPDATE etl.slack_alerts SET last_executed = now() WHERE id = %s",
                (alert_id,),
            )
            rds_conn.commit()
            log_msg = {
                "id": alert_id,
                "status": "sent",
                "slack_response": slack_response,
            }
            logger.info(log_msg)
            results.append(log_msg)

        except Exception as e:
            log_msg = {
                "id": alert_id,
                "status": "error",
                "error": str(e),
                "title": title,
                "error at": traceback.format_exc(),
            }
            logger.error(log_msg)
            results.append(log_msg)

    log_msg = {"status": "done", "output": results}
    logger.info(log_msg)

    rds_cursor.close()
    listing_cursor.close()
    listing_conn.close()
    rds_conn.close()

    return log_msg
