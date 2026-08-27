import traceback
import logging
import boto3
import os
import json

logger = logging.getLogger("mls_ApiAuthenticationFunction")
logger.setLevel("INFO")

# Replace these values with your actual Cognito pool ID, client ID, and region
user_pool_id = os.environ["UserpoolID"]
client_id = os.environ["ClientID"]

# Initialize the Cognito Identity Provider client
client = boto3.client("cognito-idp")


def initiate_auth(email, password):
    try:
        # Authenticate the user and get the authentication token
        response = client.admin_initiate_auth(
            UserPoolId=user_pool_id,
            ClientId=client_id,
            AuthFlow="ADMIN_NO_SRP_AUTH",
            AuthParameters={"USERNAME": email, "PASSWORD": password},
            ClientMetadata={"username": email, "password": password},
        )
        return response
    except client.exceptions.NotAuthorizedException as e:
        logger.error("The username or password is incorrect")
        raise Exception(e)
    except client.exceptions.UserNotFoundException as e:
        logger.error("User Not Found")
        raise Exception(e)
    except Exception as e:
        logger.error("Authentication Attempt Failed")
        raise Exception(e)


def refresh_auth(refresh_token):
    try:
        response = client.admin_initiate_auth(
            UserPoolId=user_pool_id,
            ClientId=client_id,
            AuthFlow="REFRESH_TOKEN_AUTH",
            AuthParameters={
                "REFRESH_TOKEN": refresh_token,
            },
            ClientMetadata={},
        )
        return response
    except Exception as e:
        logger.error("Authentication Attempt Failed")
        raise Exception(e)


def lambda_handler(event, context):
    event = event["queryStringParameters"]
    email = event.get("email")
    logger.info(f"Lambda Handler Event Data : {email}")

    try:
        response = {}
        if "password" in event:
            resp = initiate_auth(email, event["password"])
            response = {
                "status": "success",
                "access_token": resp["AuthenticationResult"]["IdToken"],
                "refresh_token": resp["AuthenticationResult"]["RefreshToken"],
            }

        elif "refresh_token" in event:
            resp = refresh_auth(event["refresh_token"])
            response = {
                "status": "success",
                "access_token": resp["AuthenticationResult"]["IdToken"],
            }
        return {
            "statusCode": 200,
            "body": json.dumps(response),
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*",
            },
        }

    except Exception as e:
        log_mssg = {
            "reason": str(e),
            "traceback": traceback.format_exc(),
        }
        logger.error(log_mssg)
        return {
            "statusCode": 500,
            "body": json.dumps(log_mssg),
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*",
            },
        }
