import requests
from requests.auth import HTTPDigestAuth
import xml.etree.ElementTree as ET
from xml.etree.ElementTree import ParseError
from json import JSONDecodeError
import re
import pandas as pd
import logging


logging.basicConfig(format="%(levelname)s - %(message)s", force=True)
logger = logging.getLogger("mls-connectmls-rets-inactive-data-download")
logger.setLevel(logging.INFO)


def login(data):
    loginUrl = data["loginUrl"]
    password = data["password"]
    username = data["user"]
    source_id = data["source_id"]
    USER_AGENT = "Python/3.8 RETS Client/1.0"
    # Create a session
    session = requests.Session()
    auth = HTTPDigestAuth(username, password)
    session.auth = auth
    session.headers = {"rets-version": "RETS/1.7.2"}
    # Send login request
    response = session.get(loginUrl)

    response_text = response.text
    try:
        root = ET.fromstring(response_text)
        rets_response_text = root.find("RETS-RESPONSE").text.strip()
        rets_data = dict(re.findall(r"(\w+)=([^\n\r]*)", rets_response_text))
        logger.info("Login successful!")
        rets_data["session"] = session
        return rets_data
    except ParseError as e:
        msg = {"source_id": source_id, "message": f"Error in server login: {e}"}
        logger.error(msg)
        raise Exception(msg)


def data_download(data):
    session = data["session"]
    query_params = data["query_params"]
    search_url = data["Login"]
    source_id = data["source_id"]
    if source_id == 298:
        search_url = "http://sabor-rets.connectmls.com" + data["Search"]
    else:
        search_url = search_url.split("/rets")[0] + data["Search"]
    # query_params['rets-version']= 'rets/1.8'
    query_params["QueryType"] = "DMQL2"
    query_params["Format"] = "COMPACT-DECODED"
    query_params["Count"] = "1"
    if query_params["SearchType"] == "Media":
        Query = query_params["Query"]
        Query = (
            re.sub(r".*?=", "", Query).replace("(", "").replace(")", "")
        )  # .replace(",",":*,")
        Query = ":*,".join(Query.split(",")) + ":*"
        query_params = {"Resource": "Property", "Type": "Photo", "ID": Query}
        data["query_params"] = query_params
        df, count = get_object(data)
        return df, count, True

    response = session.get(search_url, params=query_params)
    response_text = response.text

    try:
        root = ET.fromstring(response_text)
        # Extract column names
        count_element = root.find(".//COUNT")
        data_count = int(count_element.get("Records"))
        columns = root.find("./COLUMNS").text.split("\t")[1:-1]
        # Extract data rows
        data_rows = []
        for data_element in root.findall("./DATA"):
            row = data_element.text.split("\t")[1:-1]
            data_rows.append(row)

        df_temp = pd.DataFrame(data_rows, columns=columns)
        return df_temp, data_count

    except Exception as e:
        root = ET.fromstring(response_text)
        reply_text = root.attrib.get("ReplyText")
        if "No Records Found" in reply_text:
            return pd.DataFrame(), 0

        raise Exception(f"{reply_text} Error {e}")
