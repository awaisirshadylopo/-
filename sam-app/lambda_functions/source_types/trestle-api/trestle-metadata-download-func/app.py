import json
import requests
import xml.etree.ElementTree as et
import psycopg2
from psycopg2.extras import execute_values
import sys
import pandas as pd
from urllib.parse import urlparse
import boto3
import os
from helper import LogData, LogMessage, log_message

### TODO -- logging to be changed...


# Function to fetch secrets from AWS Secrets Manager
def fetch_secrets(secret_name):
    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId=secret_name)
    secret = json.loads(response["SecretString"])
    return secret


# Function to set up a PostgreSQL database connection
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


def create_token(client_id, client_secret, loginUrl):
    url = loginUrl
    headers = {"Content-Type": "application/x-www-form-urlencoded;charset=UTF-8"}

    data = {
        "grant_type": "client_credentials",
        "scope": "api",
        "client_id": str(client_id),
    }
    auth = (str(client_id), str(client_secret))

    response = requests.post(url=loginUrl, headers=headers, data=data, auth=auth)
    if response.status_code == 200:
        my_json = json.loads(response.content)
        token = my_json["access_token"]
        return token
    else:
        ret = {"statusCode": response.status_code, "body": "Token Generation Failed"}
        log_data = LogData(event=ret)
        log_message(LogMessage("DEBUG", "received", log_data))


def DownlaodMetaData(token, source_id, source_name, domain_url):
    # url = "https://api-prod.corelogic.com/trestle/odata/$metadata"
    s3 = boto3.client("s3")
    classList = []
    list_dict = {item: [] for item in classList}
    metadata_url = domain_url + "/trestle/odata/" + "$metadata"
    headers = {"Authorization": f"Bearer {token}"}

    resp = requests.get(url=metadata_url, headers=headers)
    xml_content = resp.content
    Key_path = "metadata/abc.xml"
    # s3.put_object(Body=xml_content, Bucket='api-based-sourcesmetdata', Key=Key_path)
    tree = et.ElementTree(et.fromstring(xml_content))
    root = tree.getroot()

    for all_tree in root:
        for inner_tree in all_tree:
            if (
                "Namespace" in inner_tree.attrib
                and inner_tree.attrib["Namespace"] == "CoreLogic.DataStandard.RESO.DD"
            ):
                for entity_type in inner_tree.findall(
                    ".//{http://docs.oasis-open.org/odata/ns/edm}EntityType"
                ):
                    class_name = entity_type.attrib["Name"]
                    classList.append(class_name)

                    if class_name not in list_dict:
                        list_dict[class_name] = []
                    columns_name = entity_type.findall(".//{*}Property")

                    for col in columns_name:
                        col_names = col.attrib["Name"]
                        list_dict[class_name].append(col_names)
    return classList, list_dict


def metadata_insertion(
    classList, list_dict, source_name, source_id, connection, cursor
):
    classmetadata_del_sql = (
        "delete from dev.stage_class_metadata where source_id = '{}'".format(
            str(source_id)
        )
    )
    fieldmetadata_del_sql = (
        "delete from dev.stage_field_metadata where source_id = '{}'".format(
            str(source_id)
        )
    )
    cursor.execute(classmetadata_del_sql)
    cursor.execute(fieldmetadata_del_sql)
    connection.commit()
    for class_name in classList:
        data = (
            source_id,
            source_name,
            class_name,
            class_name,
        )  # Create a tuple with all the values
        cursor.execute(
            "INSERT INTO dev.stage_class_metadata (source_id, source_name, resource_name, class_name) VALUES (%s, %s, %s, %s)",
            data,
        )
    connection.commit()
    # Insertion in Field Metadata table
    resource_prefix = "key_value"
    for item1, item2 in zip(
        classList, (list_dict.get(class_name) for class_name in classList)
    ):
        data_for_insert = [
            (source_id, source_name, item1, item1, field_value, resource_prefix)
            for field_value in item2
        ]
        insert_query = "INSERT INTO dev.stage_field_metadata (source_id, source_name, resource_name, class_name, long_name,key_value) VALUES %s"
        execute_values(cursor, insert_query, data_for_insert)
        connection.commit()

    class_meta_proc = "call dev.scd_class_metadata('{}')".format(source_id)
    cursor.execute(class_meta_proc)
    connection.commit()

    fields_meta_proc = "call dev.scd_fields_metadata_trestle('{}')".format(source_id)
    cursor.execute(fields_meta_proc)
    connection.commit()


def pre_stage_ddl(source_id, connection, cursor):
    db_reserved_keywords = ["add", "all", "alter", "and", "as", "order", "View"]
    try:
        query = """
            SELECT
                fm.source_id,
                fm.class_name,
                STRING_AGG(fm.long_name, '|' ORDER BY fm.long_name) AS concatenated_long_names
            FROM
                dev.class_metadata AS cm
            INNER JOIN
                dev.field_metadata AS fm
                ON cm.source_id = {}
                AND cm.source_name=fm.source_name
                AND cm.class_name=fm.class_name

            WHERE
                cm.active_flag = 'true'
                AND cm.download_flag = 'true'
                AND fm.active_flag = 'true'
                AND fm.download_flag = 'true'
            GROUP BY
                fm.source_id,
                fm.class_name
            ORDER BY
                fm.class_name;
            """.format(str(source_id))
        cursor.execute(query)
        sources = cursor.fetchall()
        df_column = ["source_id", "class_name", "concatenated_long_names"]
        df = pd.DataFrame(sources, columns=df_column)
        # Iterate over DataFrame and split concatenated_long_names
        for index, row in df.iterrows():
            schema_name = "idx_stage"
            # table_name = str(row['class_name'])  # You can replace this with your own naming convention
            table_name = str(row["class_name"]).lower()
            table_name = "ps_trestle_" + table_name
            column_names = [
                col.lower() for col in row["concatenated_long_names"].split("|")
            ]
            # Check if the table already exists

            table_exists_query = f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = '{schema_name}' AND table_name = '{table_name}');"
            cursor.execute(table_exists_query)
            table_exists = cursor.fetchone()[0]

            if table_exists:
                # The table already exists, check for new columns
                cursor.execute(
                    f"SELECT column_name FROM information_schema.columns WHERE table_schema = '{schema_name}' AND table_name = '{table_name}';"
                )
                existing_columns = [col[0] for col in cursor.fetchall()]

                new_columns = list(set(column_names) - set(existing_columns))

                if new_columns:
                    # Add new columns to the existing table
                    for col in new_columns:
                        column_query = f"ALTER TABLE {schema_name}.{table_name} ADD COLUMN {col} TEXT;"
                        cursor.execute(column_query)

                        statement = f"Added new column {col} to table {schema_name}.{table_name}"
                        log_msg = {"Statement": statement}
                        log_data = LogData(event=log_msg)
                        log_message(LogMessage("INFO", "received", log_data))

                    connection.commit()

            else:
                # The table doesn't exist, create a new table
                additional_columns = [
                    "source_id INT",
                    "source_name TEXT",
                    "batch_id INT",
                    "source_last_update_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
                    "y_creation_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
                    "y_last_update_date  TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
                    "source_creation_date  TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
                ]
                column_names_with_text = [
                    (
                        f'"{col}" TEXT'
                        if col.lower() in db_reserved_keywords
                        else f"{col} TEXT"
                    )
                    for col in column_names
                ]
                column_names_with_text[:0] = additional_columns
                create_table_query = f"CREATE TABLE {schema_name}.{table_name} ({', '.join(column_names_with_text)});"
                cursor.execute(create_table_query)
                statement = f"Table {schema_name}.{table_name} created successfully."
                log_msg = {"Statement": statement}
                log_data = LogData(event=log_message)
                log_message(LogMessage("INFO", "received", log_data))

            connection.commit()

    except Exception as e:

        log_data = LogData(event=e)
        log_message(LogMessage("DEBUG", "received", log_data))

    finally:
        connection.close()


def lambda_handler(event, context):
    """This lambda functions logs json in following format:
    {    "Token": token,
        "Domain_URL:": domain_url,
        "Return_Classname": return_classname,
        "Return_list_dict":return_list_dict}
    """

    log_data = LogData(event=event)
    log_message(LogMessage("INFO", "received", log_data))

    try:
        # DB connection postgresql
        secret_name = "ylopo/dev/db"
        secrets = fetch_secrets(secret_name)
        connection = setup_db_connection(secrets)
        cursor = connection.cursor()

        # Define Variables for Excution
        source_names_trestle = []

        source_id = event["source_id"]
        source_name = event["source_name"]
        # source_type = event['source_type']['idx_mls_type']
        source_auth = event["auth"]

        loginurl = source_auth.get("loginUrl")
        client_id = source_auth.get("user")
        client_secret = source_auth.get("password")

        # create_token Function Call
        token = create_token(client_id, client_secret, loginurl)
        parsed_url = urlparse(loginurl)
        domain_url = parsed_url.scheme + "://" + parsed_url.netloc

        # DownlaodMetaData Function Call
        return_classname, return_list_dict = DownlaodMetaData(
            token, source_id, source_name, domain_url
        )

        # Metadata Insertion Function Call
        metadata_insertion(
            return_classname,
            return_list_dict,
            source_name,
            source_id,
            connection,
            cursor,
        )

        pre_stage_ddl(source_id, connection, cursor)

        dict_list = {
            "Token": token,
            "Domain_URL:": domain_url,
            "Return_Classname": return_classname,
            "Return_list_dict": return_list_dict,
        }

        # Logging the processed data
        log_data = LogData(event=dict_list)
        log_message(LogMessage("INFO", "received", log_data))

    except Exception as e:
        log_data = LogData(event=e)
        log_message(LogMessage("DEBUG", "received", log_data))

        # Returning an error response
        return {"statusCode": 500, "body": f"Error: {str(e)}"}
    finally:
        # Close the database connection
        cursor.close()
        connection.close()
