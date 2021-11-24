
# Data Mesh Consumer

The `DataMeshConsumer.py` class provides functions to manage Consumer functions such as requesting access to data products and finalizing subscriptions. 

### Creating a Data Mesh Consumer Instance

#### Request Syntax

```python
DataMeshConsumer(
	data_mesh_account_id: str, 
	region_name: str = 'us-east-1', 
	log_level: str = "INFO",
	use_credentials=None
)                 
```

#### Parameters

* `data_mesh_account_id`: The AWS Account ID to use as the central Data Mesh Account in the region
* `region_name`: The short AWS Region Name in which to install the Data Mesh
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

The following methods are available:

* [`delete_subscription`](#delete_subscription)
* [`finalize_subscription`](#finalize_subscription)
* [`get_subscription`](#get_subscription)
* [`get_table_info`](#get_table_info)
* [`list_product_access`](#list_product_access)
* [`request_access_to_product`](#request_access_to_product)

### delete\_subscription

Method allowing a Consumer to leave a subscription.

#### Request Syntax

```python
delete_subscription(
	subscription_id: str, 
	reason: str
)
```

#### Parameters

* `subscription_id`: The ID of the Subscription to leave
* `reason`: String value to be added to the Subscription about why it's being deleted

#### Return Type

None

#### Response Syntax

#### Response Structure

---

### finalize\_subscription

Performs the final step in the Subscription workflow to import the shared objects.

#### Request Syntax

```python
finalize_subscription(
	subscription_id: str
)
```

#### Parameters

* `subscription_id`: The ID of the Subscription to complete

#### Return Type

None

#### Response Syntax

#### Response Structure

---

### get\_subscription

Fetches information about a given Subscription request.

#### Request Syntax

```python
get_subscription(
	request_id: str
)
```

#### Parameters

* `request_id`: The ID of the Subscription request to fetch

#### Return Type

dict

#### Response Syntax

```
{
	'Subscriptions': [
		{
		  "SubscriptionId": str,
		  "Status": str,
		  "DatabaseName": str,		  
		  "TableName": list<string>,
		  "RequestedGrants": list<string>,
		  "PermittedGrants": list<string>,
		  "SubscriberPrincipal": str,
		  "CreationDate": str,		  
		  "CreatedBy": str,
		  "UpdatedDate": str,
		  "UpdatedBy": str,
		  "Notes": list,
		  "RamShares": list<string>,
		  "GrantedTableARNs": list<string>
		}
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
		* `PermittedGrants`: Grants which have been approved by the Producer		* `GrantableGrants`: Grants the Consumer can share
		* `OwnerPrincipal`: The AWS Account ID of the Owner
		* `SubscriberPrincipal`: The AWS Account ID of the requesting Consumer
		* `CreationDate`: Date the request was made, in `YYYY-MM-DD HH:MI:SS` format
		* `CreatedBy`: The Identity of the Principal who requested access
		* `UpdatedDate`: The date of the last update, in `YYYY-MM-DD HH:MI:SS` format
		* `UpdatedBy`: The Identity of the Principal who made the last update
		
---

### get\_table\_info

Consumer wrapper interface for `Glue::GetTable` (https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue.html#Glue.Client.get_table).

#### Request Syntax

```python
get_table_info(
	database_name: str, 
	table_name: str
)
```

#### Parameters

* `database_name`: The name of the Glue Catalog Database where the Table is stored
* `table_name`: The name of the table to describe

#### Return Type

dict

#### Response Syntax

See https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue.html#Glue.Client.get_table.

#### Response Structure

---

### list\_product\_access

Lists the data products to which the consumer has access.

#### Request Syntax

```python
list_product_access()
```

#### Parameters

None

#### Return Type

list

#### Response Syntax

```
[
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
	...
]
```

#### Response Structure

* (list)
	* (dict)
		* `SubscriptionId`: The ID assigned to the Subscription request
		* `DatabaseName`: The name of the database containing shared objects
		* `TableName`: List of table names being requested
		* `RequestedGrants`: Grants requested by the Consumer
		* `PermittedGrants`: Grants which have been approved by the Producer
		* `GrantableGrants`: Grants the Consumer can share
		* `OwnerPrincipal`: The AWS Account ID of the Owner
		* `SubscriberPrincipal`: The AWS Account ID of the requesting Consumer
		* `CreationDate`: Date the request was made, in `YYYY-MM-DD HH:MI:SS` format
		* `CreatedBy`: The Identity of the Principal who requested access
		* `UpdatedDate`: The date of the last update, in `YYYY-MM-DD HH:MI:SS` format
		* `UpdatedBy`: The Identity of the Principal who made the last update

---

### request\_access\_to\_product

Requests a set of grants to a shared data product from the Producer.

#### Request Syntax

```python
request_access_to_product(
	owner_account_id: str, 
	database_name: str,
	request_permissions: list, 
	tables: list = None)
```

#### Parameters

* `owner_account_id`: The AWS Account who owns the objects for which the Consumer is requesting access
* `database_name`: The database name of the shared objects
* `request_permission`: The list of permissions to be requested, such as `SELECT` or `INSERT`
* `tables`: Optional list of tables or a regular expression definiting tables to request access to

#### Return Type

dict

#### Response Syntax

```json
{
	"Type": str,
	"Database":str,
	"Table":str
}
```

#### Response Structure

* `Type`: The type of access request created - one of DATABASE or TABLE
* `Database`: The name of the database for which a subscription has been created
* `Table`: Optional name of a table for which the subscription has been created

---