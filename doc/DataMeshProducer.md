# Data Mesh Producer Library

The `DataMeshProducer.py` library provides functions to assist data __Producers__ to create and manage __Data Products__. The following methods are avialable:

* [`create_data_products`](#create_data_products)
* [`list_pending_access_requests`](#list_pending_access_requests)
* [`approve_access_request`](#approve_access_request)
* [`deny_access_request`](#deny_access_request)
* [`update_subscription_permissions`](#update_subscription)
* [`delete_subscription`](#delete_subscription)

## Method Detail

### create\_data\_products

Creates a new data product offering of one-or-more tables. When creating a set of data products, the object metadata is copied into the Lake Formation catalog of the data mesh account, and appropriate grants are created to enable the product to administer the central metadata.

#### Request Syntax

```python
create_data_products(
	source_database_name: str,
	create_public_metadata: bool = True,
	table_name_regex: str = None,
	domain: str = None,
	data_product_name: str = None,
	sync_mesh_catalog_schedule: str = None,
	sync_mesh_crawler_role_arn: str = None,
	expose_data_mesh_db_name: str = None,
	expose_table_references_with_suffix: str = "_link"
)
```

#### Parameters

* `source_database_name` (String) - The name of the Source Database. Only 1 Database at a time may be used to create a set of data products
* `table_name_regex` (String) - A table name or regular expression matching a set of tables to be offered. Optional.
* `domain` (String) - A domain name to be associated with the data product
* `data_product_name` (String) - The data product name to be used for the resolved objects. If not provided, then only direct sharing grants will be possible.
* `create_public_metadata` (Boolean) - True or False indicating whether the read-only role should be granted DESCRIBE on metadata
* `sync_mesh_catalog_schedule` (String) - CRON expression indicating how often the data mesh catalog should be synced with the source. Optional. If not provided, metadata will be updated every 4 hours if a `sync_mesh_crawler_role_arn` is provided.
* `sync_mesh_crawler_role_arn` (String) - IAM Role ARN to be used to create a Glue Crawler which will update the structure of the data mesh metadata based upon changes to the source. Optional. If not provided, metadata will not be updated from source.
* `expose_data_mesh_db_name` (String) - Overrides the name of the database in the Data Mesh account with the provided value. If not provided, then the database name will be set to `<original name>-<account id>`
* `expose_table_references_with_suffix` (String) - Overrides the suffix to be set on all resource links shared back to the Producer. Default is `<original name>_link`.

#### Return Type

None

#### Response Structure

---

### list\_pending\_access\_requests

#### Request Syntax

#### Parameters

#### Return Type

#### Response Structure

---

### approve\_access\_request

#### Request Syntax

#### Parameters

#### Return Type

#### Response Structure

---

### deny\_access\_request

#### Request Syntax

#### Parameters

#### Return Type

#### Response Structure

---

### update\_subscription

#### Request Syntax

#### Parameters

#### Return Type

#### Response Structure

---

### delete\_subscription

#### Request Syntax

#### Parameters

#### Return Type

#### Response Structure
