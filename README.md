# LMD Unified State Machine - AWS SAM Deployment

This repository contains an AWS Serverless Application Model (SAM) project for deploying the complete infrastructure required to manage the LMD unified state machine. This deployment includes all necessary AWS resources such as Lambda functions, a Step Function state machine, IAM roles, Lambda layers, and event schedulers.

## Overview

The SAM template in this project deploys the following components:

### State Machine

**Name:** `mls-unified-lmd-state-machine`

This state machine orchestrates the workflow for the LMD unified process.

### Lambda Functions

#### Common Lambda Functions
These functions perform tasks common to all data sources:

- `mls-active-unified-locking-func`
- `mls-active-delete-data-download-func`
- `mls-active-delete-mapping-func`
- `mls-active-mapping-func`
- `mls-active-etl-action-func`
- `mls-active-no-insert-update-func`
- `mls-active-data-sync-func`
- `mls-cache-clean-func`
- `mls-active-unlocking-func`
- `state-machine-execution-name-setter-func`

#### Source-Specific Lambda Functions
Each source type has its own set of validation and data download Lambda functions:

**Source Type: Bridge**
- `bridge-active-validation-func`
- `bridge-active-data-download-func`

**Source Type: Bright**
- `bright-active-validation-func`
- `bright-active-data-download-func`

**Source Type: DDF**
- `dff-active-validation-func`
- `dff-active-data-download-func`

**Source Type: ListHub**
- `listhub-active-validation-func`
- `listhub-active-data-download-func`

**Source Type: MLS Grid**
- `mlsgrid-active-validation-func`
- `mlsgrid-active-data-download-func`

**Source Type: NavicaMLS RETS**
- `navicamls-rets-active-validation-func`
- `navicamls-rets-active-data-download-func`

**Source Type: Paragon**
- `paragon-active-validation-func`
- `paragon-active-data-download-func`

**Source Type: ParagonRETS**
- `paragonrels-rets-active-validation-func`
- `paragonrels-rets-active-data-download-func`

**Source Type: PropTx**
- `proptx-active-validation-func`
- `proptx-active-data-download-func`

**Source Type: Rapattoni**
- `rapattoni-active-validation-func`
- `rapattoni-active-data-download-func`

**Source Type: RAPI**
- `rapi-active-validation-func`
- `rapi-active-data-download-func`

**Source Type: RESO**
- `reso-active-validation-func`
- `reso-active-data-download-func`

**Source Type: Silvar RETS**
- `silvar-rets-active-validation-func`
- `silvar-rets-active-data-download-func`

**Source Type: Spark**
- `spark-active-validation-func`
- `spark-active-data-download-func`

**Source Type: Trestle**
- `trestle-active-validation-func`
- `trestle-active-data-download-func`

**Source Type: Trestle RETS**
- `trestle-rets-active-validation-func`
- `trestle-rets-active-data-download-func`

### Lambda Layers
The project includes the following Lambda layers for shared dependencies:

- `awsWrangler`
- `helperLayer`
- `psycopg2`

### IAM Roles

- `mlsLambdaFunctionRole`: Grants necessary permissions for all Lambda functions.
- `mlsStateMachineRole`: Grants permissions required by the Step Function state machine.

### Event Schedulers
Schedulers trigger Lambda functions for specific data sources at predefined intervals:

- `TrestleSchedulerEvent`
- `SparkSchedulerEvent`
- `BridgeSchedulerEvent`
- `MlsGridSchedulerEvent`
- `ParagonrelsSchedulerEvent`
- `ListhubSchedulerEvent`
- `DdfSchedulerEvent`
- `NavicamlsRetsSchedulerEvent`
- `ProptxSchedulerEvent`
- `RapattoniSchedulerEvent`
- `ParagonrelsRetsSchedulerEvent`
- `RapiSchedulerEvent`
- `ResoSchedulerEvent`
- `TrestleRetsSchedulerEvent`
- `SilvarRetsSchedulerEvent`
- `MappingBackupSchedulerEvent`

### S3 Buckets

The project utilizes two S3 buckets for data archival and backup purposes:

#### IDX Data Lake Bucket
**Name:** `idx-data-lake`
- Archives batch-specific data for each execution
- Maintains historical record of processed data
- Organized by data source and batch timestamp
- Enables data traceability and audit capabilities

#### IDX Mapping Backup Bucket
**Name:** `idx-mapping-backup`
- Stores daily backups of mapping Tables
- Enables mapping version control and recovery
- Automated daily backup process
- Maintains mapping history for rollback scenarios

## Conclusion
This SAM project provides a complete infrastructure to manage and execute the LMD unified state machine. It ensures seamless orchestration, validation, and data synchronization across multiple data sources, making it a robust and scalable solution for your workflows.
