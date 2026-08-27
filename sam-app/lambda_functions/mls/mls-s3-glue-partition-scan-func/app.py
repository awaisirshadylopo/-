import io
import json
import logging
import os
import time
import traceback

import boto3
import pyarrow.parquet as pq

logger = logging.getLogger("PartitionRegLambda")
logger.setLevel(logging.INFO)

# ── reused helpers from your original code ──────────────────────────────────
def get_latest_batch_sample_parquet(s3, bucket_name, prefix):
    """Returns (latest_batch_id, sample_parquet_key, columns) for the LATEST batch_id folder."""
    batch_ids = []
    for page in s3.get_paginator("list_objects_v2").paginate(
        Bucket=bucket_name, Prefix=prefix, Delimiter="/"
    ):
        for cp in page.get("CommonPrefixes", []):
            batch_id = cp["Prefix"].rstrip("/").split("/")[-1]
            if "temp" in batch_id.lower():
                logger.info(f"Skipping temp folder: {batch_id}")
                continue
            batch_ids.append(batch_id)

    if not batch_ids:
        return None, None, []

    latest_batch_id = max(batch_ids, key=lambda x: int(x))
    latest_prefix   = f"{prefix}{latest_batch_id}/"

    sample_key = None
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket_name, Prefix=latest_prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".parquet") and not key.endswith("_Request.parquet") and not key.endswith("_0.parquet"):
                sample_key = key
                break
        if sample_key:
            break

    if not sample_key:
        return latest_batch_id, None, []

    obj    = s3.get_object(Bucket=bucket_name, Key=sample_key)
    buffer = io.BytesIO(obj["Body"].read())
    schema = pq.ParquetFile(buffer).schema_arrow

    columns = [
        col.name for col in schema
        if col.name.lower() != "batch_id"
        and "@" not in col.name
        and not col.name.lower().startswith("request_url")
        and not col.name.lower().startswith("request_params")
    ]
    return latest_batch_id, sample_key, columns


def sync_glue_table_columns_with_latest_data(source_id, source_name, source_type,
                                             class_name, glue_database):
    glue        = boto3.client("glue")
    s3          = boto3.client("s3")
    bucket_name = os.environ.get("bucket_name")
    table_name  = f"{class_name.lower()}_{source_id}"
    prefix      = f"{source_type}/{source_id}_{source_name}/{class_name}_{source_id}/"

    response      = glue.get_table(DatabaseName=glue_database, Name=table_name)
    storage       = response["Table"]["StorageDescriptor"]
    existing_cols = {col["Name"].lower() for col in storage["Columns"]}

    latest_batch_id, sample_key, latest_cols = get_latest_batch_sample_parquet(s3, bucket_name, prefix)
    if not sample_key:
        logger.info({"table_name": table_name, "message": "No sample parquet – skip column sync"})
        return

    latest_cols_lower = [c.lower() for c in latest_cols]
    new_cols = [col for col in latest_cols_lower if col not in existing_cols]
    if not new_cols:
        logger.info({"table_name": table_name, "message": "No new columns"})
        return

    logger.info({"table_name": table_name, "new_cols": new_cols, "message": "Adding new columns"})
    updated_columns = storage["Columns"] + [{"Name": col, "Type": "string"} for col in new_cols]
    table_input = {
        "Name": response["Table"]["Name"],
        "Description": response["Table"].get("Description", ""),
        "Owner": response["Table"].get("Owner", ""),
        "Retention": response["Table"].get("Retention", 0),
        "TableType": response["Table"].get("TableType", "EXTERNAL_TABLE"),
        "Parameters": response["Table"].get("Parameters", {}),
        "PartitionKeys": response["Table"].get("PartitionKeys", []),
        "StorageDescriptor": {**storage, "Columns": updated_columns},
    }
    glue.update_table(DatabaseName=glue_database, TableInput=table_input)
    logger.info({"table_name": table_name, "added": new_cols, "message": "Schema updated"})


def fix_glue_table_schema(glue_database: str, table_name: str):
    glue = boto3.client("glue")
    response = glue.get_table(DatabaseName=glue_database, Name=table_name)
    table   = response["Table"]
    storage = table["StorageDescriptor"]
    partition_keys = table.get("PartitionKeys", [])
    pk_names_lower = {pk["Name"].lower() for pk in partition_keys}

    seen_cols = set()
    clean_columns = []
    for col in storage["Columns"]:
        col_name = col["Name"].lower()
        if col_name in seen_cols or col_name in pk_names_lower or "@" in col["Name"]:
            continue
        seen_cols.add(col_name)
        clean_columns.append({**col, "Name": col_name, "Type": "string"})

    clean_partition_keys = []
    seen_pk = set()
    for pk in partition_keys:
        pk_name = pk["Name"].lower()
        if pk_name in seen_pk:
            continue
        seen_pk.add(pk_name)
        clean_partition_keys.append({**pk, "Type": "string"})

    table_input = {
        "Name": table["Name"],
        "Description": table.get("Description", ""),
        "Owner": table.get("Owner", ""),
        "Retention": table.get("Retention", 0),
        "TableType": table.get("TableType", "EXTERNAL_TABLE"),
        "Parameters": table.get("Parameters", {}),
        "PartitionKeys": clean_partition_keys,
        "StorageDescriptor": {**storage, "Columns": clean_columns},
    }
    glue.update_table(DatabaseName=glue_database, TableInput=table_input)
    logger.info({"glue_database": glue_database, "table_name": table_name, "clean_cols": len(clean_columns)})


def fix_glue_partition_schemas(glue_database: str, table_name: str):
    glue = boto3.client("glue")
    paginator = glue.get_paginator("get_partitions")
    partitions = []
    for page in paginator.paginate(DatabaseName=glue_database, TableName=table_name):
        partitions.extend(page["Partitions"])
    if not partitions:
        return

    partition_updates = []
    for partition in partitions:
        sd = partition["StorageDescriptor"]
        seen_cols = set()
        clean_cols = []
        for col in sd["Columns"]:
            col_name = col["Name"].lower()
            if col_name in seen_cols or "@" in col["Name"]:
                continue
            seen_cols.add(col_name)
            clean_cols.append({**col, "Name": col_name, "Type": "string"})
        sd["Columns"] = clean_cols
        partition_updates.append({
            "PartitionValueList": partition["Values"],
            "PartitionInput": {
                "Values": partition["Values"],
                "StorageDescriptor": sd,
                "Parameters": partition.get("Parameters", {}),
            },
        })

    for i in range(0, len(partition_updates), 25):
        batch = partition_updates[i : i + 25]
        glue.batch_update_partition(DatabaseName=glue_database, TableName=table_name, Entries=batch)
    logger.info({"table_name": table_name, "partitions_fixed": len(partitions)})


def create_glue_table_if_missing(source_id, source_name, source_type,
                                 class_name, glue_database, athena_output_bucket):
    """Ensures the Glue table exists. Does NOT register partitions."""
    glue    = boto3.client("glue")
    s3      = boto3.client("s3")
    athena  = boto3.client("athena")
    bucket  = os.environ.get("bucket_name")
    table_name = f"{class_name.lower()}_{source_id}"
    prefix     = f"{source_type}/{source_id}_{source_name}/{class_name}_{source_id}/"

    try:
        glue.get_table(DatabaseName=glue_database, Name=table_name)
        logger.info({"table_name": table_name, "message": "Table exists"})
        return
    except glue.exceptions.EntityNotFoundException:
        pass  # create below

    latest_batch_id, sample_key, keep_cols = get_latest_batch_sample_parquet(s3, bucket, prefix)
    if not sample_key or not keep_cols:
        raise Exception(f"No Parquet data found for {table_name} – cannot create table")

    col_defs = ",\n  ".join([f"`{col}` string" for col in keep_cols])
    ddl = f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS `{glue_database}`.`{table_name}` (
      {col_defs}
    )
    PARTITIONED BY (`batch_id` string)
    STORED AS PARQUET
    LOCATION 's3://{bucket}/{prefix}'
    TBLPROPERTIES (
        'parquet.compress'='SNAPPY',
        'exclusions'='[".*_Request\\\\.parquet", ".*_0\\\\.parquet"]'
    )
    """
    resp = athena.start_query_execution(
        QueryString=ddl,
        QueryExecutionContext={"Database": glue_database},
        ResultConfiguration={
            "OutputLocation": f"s3://{athena_output_bucket}/athena-ddl-results/{source_id}/{class_name}/"
        },
    )
    exec_id = resp["QueryExecutionId"]
    for _ in range(40):
        status = athena.get_query_execution(QueryExecutionId=exec_id)
        state  = status["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(3)
    if state != "SUCCEEDED":
        raise Exception(f"CREATE TABLE failed: {status}")

    fix_glue_table_schema(glue_database, table_name)
    fix_glue_partition_schemas(glue_database, table_name)
    logger.info({"table_name": table_name, "message": "Table created, schema fixed"})


# ── Paginated partition registration ─────────────────────────────────────────
def paginated_register_partitions(event):
    source_id   = event["source_id"]
    source_name = event["source_name"]
    source_info = event.get("source_info", {})
    source_type = source_info.get("source_type", event.get("source_type", ""))
    class_name  = event.get("class_name", "Property")
    token       = event.get("continuation_token")

    glue        = boto3.client("glue")
    s3          = boto3.client("s3")
    bucket_name = os.environ["bucket_name"]
    glue_db     = os.environ.get("glue_database", "s3_db")
    max_scan    = int(os.environ.get("max_s3_prefix_scan", 5000))

    class_lower = class_name.lower()
    s3_prefix   = f"{source_type}/{source_id}_{source_name}/{class_name}_{source_id}/"
    table_name  = f"{class_lower}_{source_id}"

    # ── KEY FIX: use the S3 continuation_token to skip already-seen prefixes,
    #    so we never need to re-fetch existing Glue partitions at all.
    #
    #    Store two tokens in the event:
    #      continuation_token     = S3 list continuation token
    #      glue_partition_token   = Glue get_partitions continuation (for first page only)
    #
    #    On the FIRST call (no token): build the existing set once by scanning Glue.
    #    On CONTINUATION calls: we only look at NEW S3 prefixes (past the S3 token),
    #    so we only need the most-recently-added Glue partition to detect if we've
    #    already registered them. Any prefix appearing after the last S3 scan point
    #    is new by definition.
    # ─────────────────────────────────────────────────────────────────────────

    existing = set()

    if not token:
        # First call: we must build existing set to avoid AlreadyExistsException
        for page in glue.get_paginator("get_partitions").paginate(
            DatabaseName=glue_db, TableName=table_name
        ):
            for part in page["Partitions"]:
                existing.add(part["Values"][0])
    # On continuation calls, existing stays empty: every S3 prefix past the
    # continuation token is guaranteed new (we've never processed it before).

    all_batch_ids = []
    continuation_token = token
    total_scanned = 0

    while len(all_batch_ids) < max_scan:
        list_params = {
            "Bucket": bucket_name,
            "Prefix": s3_prefix,
            "Delimiter": "/",
            "MaxKeys": 1000,
        }
        if continuation_token:
            list_params["ContinuationToken"] = continuation_token

        resp = s3.list_objects_v2(**list_params)
        common_prefixes = resp.get("CommonPrefixes", [])
        total_scanned += len(common_prefixes)

        for cp in common_prefixes:
            bid = cp["Prefix"].rstrip("/").split("/")[-1]

            if "temp" in bid.lower():
                logger.info({"source_id": source_id, "class_name": class_name,
                            "skipped_folder": bid,
                            "message": "Skipping temp folder"})
                continue

            if bid not in existing:
                all_batch_ids.append(bid)
                existing.add(bid)

        if not resp.get("IsTruncated"):
            continuation_token = None
            break
        continuation_token = resp["NextContinuationToken"]

        if len(all_batch_ids) >= max_scan:
            break

    # ── Register in max-size batches with AlreadyExists guard ────────────────
    if all_batch_ids:
        table_resp = glue.get_table(DatabaseName=glue_db, Name=table_name)
        sd = table_resp["Table"]["StorageDescriptor"]

        partition_inputs = [
            {
                "Values": [bid],
                "StorageDescriptor": {**sd, "Location": f"s3://{bucket_name}/{s3_prefix}{bid}/"},
            }
            for bid in all_batch_ids
        ]

        total_added = 0
        for i in range(0, len(partition_inputs), 100):
            batch = partition_inputs[i : i + 100]
            try:
                resp = glue.batch_create_partition(
                    DatabaseName=glue_db,
                    TableName=table_name,
                    PartitionInputList=batch,
                )
                # Errors list contains AlreadyExistsException entries — not thrown
                added = len(batch) - len(resp.get("Errors", []))
                total_added += added
                if resp.get("Errors"):
                    logger.warning({
                        "table_name": table_name,
                        "skipped_existing": len(resp["Errors"]),
                    })
            except Exception as e:
                logger.error({"batch_index": i, "error": str(e)})
                raise

        logger.info({"table_name": table_name, "added": total_added})

    return {
        "scan_status": "incomplete" if continuation_token else "complete",
        "continuation_token": continuation_token,
        "partitions_added": len(all_batch_ids),
        "partitions_scanned": total_scanned,
    }
# ── Lambda handler ───────────────────────────────────────────────────────────
def lambda_handler(event, context):
    source_id   = event["source_id"]
    source_name = event["source_name"]
    source_info = event.get("source_info", {})
    source_type = source_info.get("source_type") or event.get("source_type", "")

    enabled_resources = source_info.get("respecs_config", {}).get(
        "enabled_resources",
        [{"class_name": "Property", "key_column": "listingkey"}]
    )

    glue_db    = os.environ.get("glue_database", "s3_db")
    athena_out = os.environ.get("athena_output_bucket")

    is_continuation = bool(event.get("continuation_token"))

    try:
        if not is_continuation:
            # First call — validate all resources
            valid_resources = []
            for resource in enabled_resources:
                class_name = resource["class_name"]
                try:
                    create_glue_table_if_missing(source_id, source_name, source_type,
                                                 class_name, glue_db, athena_out)
                    sync_glue_table_columns_with_latest_data(source_id, source_name,
                                                             source_type, class_name, glue_db)
                    valid_resources.append(resource)
                    logger.info({"source_id": source_id, "class_name": class_name,
                                 "message": "Resource validated successfully"})
                except Exception as e:
                    logger.warning({"source_id": source_id, "class_name": class_name,
                                    "message": "No S3 data found skipping",
                                    "error": str(e)})
                    continue
        else:
            valid_resources = event.get("respecs_valid_resources", enabled_resources)

        # ── Sequential scan — process one resource at a time ──────────
        # Get current resource index from event (default 0 = first resource)
        current_index = event.get("current_resource_index", 0)
        current_resource = valid_resources[current_index]
        class_name = current_resource["class_name"]

        logger.info({"source_id": source_id, "class_name": class_name,
                     "current_index": current_index,
                     "total_resources": len(valid_resources),
                     "message": "Processing resource"})

        # Register partitions for current resource only
        resource_event = {
            **event,
            "class_name": class_name,
        }
        result = paginated_register_partitions(resource_event)

        logger.info({"source_id": source_id, "class_name": class_name,
                     "partitions_added": result["partitions_added"],
                     "scan_status": result["scan_status"]})

        if result["scan_status"] == "incomplete":
            # Current resource not done yet → keep scanning same resource
            return {
                **event,
                "scan_status": "incomplete",
                "continuation_token": result["continuation_token"],
                "current_resource_index": current_index,  # ← stay on same resource
                "respecs_valid_resources": valid_resources,
                "source_type": source_type,
                "source_id": source_id,
                "source_name": source_name,
            }
        else:
            # Current resource complete → move to next resource
            next_index = current_index + 1

            if next_index < len(valid_resources):
                # More resources to process
                logger.info({"source_id": source_id,
                             "message": f"Resource {class_name} complete moving to next"})
                return {
                    **event,
                    "scan_status": "incomplete",  # ← still incomplete overall
                    "continuation_token": None,    # ← reset token for next resource
                    "current_resource_index": next_index,  # ← move to next
                    "respecs_valid_resources": valid_resources,
                    "source_type": source_type,
                    "source_id": source_id,
                    "source_name": source_name,
                }
            else:
                # All resources complete
                logger.info({"source_id": source_id,
                             "message": "All resources complete"})
                return {
                    **event,
                    "scan_status": "complete",    # ← all done
                    "continuation_token": None,
                    "current_resource_index": 0,  # ← reset for next run
                    "respecs_valid_resources": valid_resources,
                    "source_type": source_type,
                    "source_id": source_id,
                    "source_name": source_name,
                }

    except Exception as e:
        logger.error({"Error": str(e), "Traceback": traceback.format_exc()})
        raise