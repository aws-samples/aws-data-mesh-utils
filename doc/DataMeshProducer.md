# Data Mesh Producer

The `DataMeshProducer.py` class provides functions to assist data __Producers__ to create and manage __Data Products__. 

### Creating a Data Mesh Producer Instance

#### Request Syntax

```python
DataMeshProducer(
	data_mesh_account_id: str, 
	region_name: str = 'us-east-1', 
	log_level: str = "INFO",
	use_credentials=None
)                 
```

#### Parameters

* `data_mesh_account_id`: The AWS Account ID to use as the central Data Mesh Account in the region
* `region_name`: The short AWS Region Name in which you want to execute Producer functions
* `log_level`: The level of information you want to see when executing. Based upon python [`logging`](https://docs.python.org/3/library/logging.html), values include `INFO`, `DEBUG`, `ERROR`, etc.
* `use_credentials`: Credentials to use to setup the instance. This can be provided as a boto3 Credentials object, a dict containing the below structure, or if None is provided the boto3 environment will be accessed.

##### Credentials dict structure
```json
{
    "AccountId": "The Consumer AWS Account ID",
    "AccessKeyId": "Your access key",
    "SecretAccessKey": "Your secret key",
    "SessionToken": "Optional - a session token, if you are using an IAM Role & temporary credentials"
}
```

The following methods are avialable:

* [`create_data_products`](#create_data_products)
* [`list_pending_access_requests`](#list_pending_access_requests)
* [`approve_access_request`](#approve_access_request)
* [`deny_access_request`](#deny_access_request)
* [`update_subscription_permissions`](#update_subscription_permissions)
* [`delete_subscription`](#delete_subscription)
* [`get_data_product`](#get_data_product)

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

dict

#### Response Syntax

```
{
	'DatabaseName': str,
	'Tables': [
		'SourceTable': str,
		'LinkTable': str,
	]
}
```

#### Response Structure

* (dict)
	* `DatabaseName`: The name of the database created
	* `Tables`: List of Tables created in the mesh account
		* `SourceTable`: The table that was shared to the data mesh
		* `LinkTable`: The resource link that is shared back to the producer Account

---

### list\_pending\_access\_requests

This method will return a list of requests made by Consumers to access to products owned by the calling principal which have not yet been approved, denied, or deleted.

#### Request Syntax

```python
list_pending_access_requests()

```

#### Parameters

None

#### Return Type

dict

#### Response Syntax

```
{
	'Subscriptions': [
		{
		  "SubscriptionId": str,
		  "DatabaseName": str,		  
		  "TableName": list<string>,
		  "RequestedGrants": list<string>,  
		  "SubscriberPrincipal": str,
		  "CreationDate": str,		  
		  "CreatedBy": str
		},
		...
	]
}
```

#### Response Structure

* (dict)
	* `Subscriptions`: List of pending subscriptions
		* `SubscriptionId`: The ID assigned to the Subscription request
		* `DatabaseName`: The name of the database containing shared objects
		* `TableName`: List of table names being requested
		* `RequestedGrants`: Grants requested by the Consumer
		* `SubscriberPrincipal`: The AWS Account Number of the requesting Consumer
		* `CreationDate`: Date the request was made, in `YYYY-MM-DD HH:MI:SS` format
		* `CreatedBy`: The Identity of the Principal who requested access.

---

### approve\_access\_request

Approves a subscription request raised by a Consumer. During this grant, the permissions can match what was requested, or overridden.

#### Request Syntax

```python
approve_access_request(
	request_id: str,
	grant_permissions: list = None,
	grantable_permissions: list = None,
	decision_notes: str = None
)
```

#### Parameters

* `request_id`: The Subscription Request that is being approved
* `grant_permissions`: The permissions to be granted to the Consumer. If None, then all requested permissions will be granted.
* `grantable_permissions`: The permissions which the Consumer can grant to other principals within their AWS Account. If None, then all `grant_permissions` will be grantable.
* `decision_notes`: String value attached to the Subscription containing information about the approval.

#### Return Type

None

#### Response Syntax

#### Response Structure

---

### deny\_access\_request

Marks a requested subscription from a Consumer as deleted. No grants are made and no objects are shared.

#### Request Syntax

```python
deny_access_request(
	request_id: str,
	decision_notes: str = None
)
```

#### Parameters

* `request_id`: The ID of the Subscription being denied
* `decision_notes`: String value indicating why the Subscription was denied.

#### Return Type

None

#### Response Syntax

#### Response Structure

---

### update\_subscription\_permissions

Method allowing a Producer to change the permissions granted to a Consumer.

#### Request Syntax

```python
update_subscription_permissions(
	subscription_id: str, 
	grant_permissions: list, 
	notes: str
)
```

#### Parameters

* `subscription_id`: The ID of the Subscription being modified
* `grant_permissions`: The permissions that will be set on the shared objects after update
* `notes`: String value associated with the permissions modification

#### Return Type

None

#### Response Syntax

#### Response Structure

---

### delete\_subscription

Tears down a granted Subscription so it can no longer be used by the Consumer. The record of the Subscription is retained for future auditing.

#### Request Syntax

```python
delete_subscription(
	subscription_id: str, 
	reason: str
)
```

#### Parameters

* `subscription_id`: The ID of the Subscription being deleted
* `reason`: String value associated with the deletion.

#### Return Type

None

#### Response Syntax

#### Response Structure

---

### get\_data\_product

Fetches information from the system about a set of tables from a given database in the Data Mesh.

#### Request Syntax

```python
get_data_product(
	database_name: str, 
	table_name_regex: str
)
```

#### Parameters

* `database_name`: The Database Name in the mesh to retrieve tables from
* `table_name_regex`: String value or regular expression matching one or more tables to retrieve

#### Return Type

list

#### Response Syntax

```
[
	{
		'Database': str,
		'TableName': str,
		'Location': str
	}
]
```

#### Response Structure

* (list)
	* (dict)
		* `DatabaseName`: The name of the database matched
		* `TableName`: The name of the table matched
		* `Location`: S3 Location of the Table

