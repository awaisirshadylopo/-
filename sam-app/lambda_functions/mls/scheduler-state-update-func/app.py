""" AWS Lambda function to update the state of an existing AWS Scheduler schedule."""
import json
import logging
import boto3

logger = logging.getLogger(__name__)
logger.setLevel("INFO")


scheduler_client = boto3.client("scheduler")


def lambda_handler(event, context):
    """ Update the state of an existing AWS Scheduler schedule."""
    event = event["queryStringParameters"]
    scheduler_name = event.get("scheduler_name")
    new_state = event.get("state")  # ENABLED or DISABLED
    group_name = event.get("group_name", "default")

    if not scheduler_name or new_state not in ["ENABLED", "DISABLED"]:
        response = {
            "statusCode": 400,
            "body": json.dumps(
                "Invalid input. Required: scheduler_name and state (ENABLED or DISABLED)"
            ),
        }
        logger.error(response)
        return response

    # Step 1: Get current scheduler details
    try:
        current = scheduler_client.get_schedule(
            Name=scheduler_name, GroupName=group_name
        )
    except scheduler_client.exceptions.ResourceNotFoundException:
        response = {
            "statusCode": 404,
            "body": json.dumps(f"Schedule '{scheduler_name}' not found."),
        }
        logger.error(response)
        return response

    # Step 2: Build update payload from existing config
    try:
        scheduler_client.update_schedule(
            Name=scheduler_name,
            GroupName=group_name,
            ScheduleExpression=current["ScheduleExpression"],
            ScheduleExpressionTimezone=current.get("ScheduleExpressionTimezone", "UTC"),
            FlexibleTimeWindow=current["FlexibleTimeWindow"],
            Target=current["Target"],
            State=new_state,
            ActionAfterCompletion=current.get("ActionAfterCompletion", "NONE"),
            Description=current.get("Description", ""),
        )
    except boto3.exceptions as e:
        response = {
            "statusCode": 500,
            "body": json.dumps(f"Failed to update schedule: {str(e)}"),
        }
        logger.error(response)
        return response

    response = {
        "statusCode": 200,
        "body": json.dumps(
            f"Schedule '{scheduler_name}' updated to state '{new_state}'"
        ),
    }
    logger.info(response)
    return response
