import requests

# import psycopg2
import logging
import os

logger = logging.getLogger("mls-cahce-clean-func")
logger.setLevel("INFO")


def lambda_handler(event, context):
    url = os.environ.get("url")

    if event is None or all(element is None for element in event):
        log_msg = {"Status": "Cache Cleaning NOT EXECUTED due to empty list"}
        logger.info(log_msg)
        return event
    else:
        try:
            for e in event:
                if e:
                    sourceId = e["source_id"]
                    url_to_clear_cache = url + str(sourceId)
                    response = requests.post(url_to_clear_cache)
                    log_msg = {
                        "URL": url_to_clear_cache,
                        "Status Code": response.status_code,
                    }
                    logger.info(log_msg)

            return event
        except Exception as e:

            log_msg = {"URL": url_to_clear_cache, "Error": e, "Event": event}
            logger.exception(f"Event : {event}, LogMessage : {log_msg}")
            return event
