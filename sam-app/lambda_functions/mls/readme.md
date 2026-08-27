# Nested Stack for Lambda Functions - Common Functions
This folder contains common Lambda functions that are utilized across various state machines. These functions handle shared tasks and processes critical to the workflow orchestration.

# Overview
The common Lambda functions in this folder provide essential operations such as data synchronization, mapping, locking, unlocking, and cache management. They are integral to the efficient functioning of state machines across multiple data sources.

# Lambda Functions Order
The functions are organized to match their execution sequence within the state machines, ensuring clarity and ease of understanding:

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