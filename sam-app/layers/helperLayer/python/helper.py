import os
import json
import logging
import boto3
from botocore.exceptions import ClientError
import dataclasses
import traceback
from typing import Optional
from dataclasses import dataclass
import psycopg2

# from sshtunnel import SSHTunnelForwarder
# from dotenv import load_dotenv


# load_dotenv()
class SecretManagerHelper:
    @staticmethod
    def get_secret(secret_name, region_name):
        # secret_name = "postgres-test-secret"
        # region_name = "us-east-2"

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


def setup_logging():
    """
    Sets up the logging configuration.

    The logging level will be DEBUG if the `DEBUG` environment variable is set to `True`, otherwise it will be INFO.
    """
    LOG_LEVEL = logging.DEBUG if os.environ.get("DEBUG", False) else logging.INFO
    default_log_args = {
        "level": LOG_LEVEL,
        "format": "%(message)s",
        "force": True,
    }
    logging.basicConfig(**default_log_args)
    logger = logging.getLogger()

    return logger


logger = setup_logging()


@dataclass(order=True)
class LogData:
    """
    Dataclass that defines the data that will be logged.

    Attributes:
        email: The email address of the user.
        event: The event object.
        query: The SQL query that was executed.
        contact_id: The contact ID of the user.
        message_id: The message ID of the user.
    """

    event: dict
    query: Optional[str] = None
    message: Optional[str] = None


@dataclass(order=True)
class LogMessage:
    """
    Dataclass that defines the log message.

    Attributes:
        level: The logging level.
        message: The message to be logged.
        data: The data to be logged.
    """

    level: str
    message: str
    data: Optional[LogData] = None


def log_message(log_msg: LogMessage):
    """
    Logs a message.

    Args:
        log_msg: The log message to be logged.
    """

    log_level = log_msg.level
    log_msg_dic = dataclasses.asdict(log_msg)

    if log_level == "INFO":
        logger.info(log_msg_dic)
    elif log_level == "DEBUG":
        logger.debug(log_msg_dic)
    elif log_level == "ERROR":
        logger.error(log_msg_dic)
    else:
        logger.info(log_msg_dic)


def db_conn(host, dbname, user, password):
    connection = psycopg2.connect(
        host=host, dbname=dbname, user=user, password=password
    )
    return connection

    # with SSHTunnelForwarder(
    #     ('35.90.67.32',22),
    #     ssh_username="ec2-user",
    #     ssh_pkey="./stagekeypair.pem",
    #     remote_bind_address=(host, 5432)
    # ) as tunnel:
    #     connection = psycopg2.connect(
    #         host='127.0.0.1',
    #         dbname=dbname,
    #         user=user,
    #         password=password,
    #         port=61066
    #     )
    #     return connection


# tunnel = sshtunnel.open_tunnel(
#         ssh_address_or_host=('35.90.67.32', 22),
#         remote_bind_address=(secrets.get('host'), secrets.get('port')),
#         ssh_username='ec2-user',
#         ssh_pkey=pem_file_path
#     )
#     tunnel.start()
