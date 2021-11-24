
# Data Mesh Administrator

The `DataMeshAdmin.py` class provides functions to create the Data Mesh, and to enable AWS Accounts to act as __Producers__ and __Consumers__. 

### Creating a Data Mesh Admin Instance

#### Request Syntax

```python
DataMeshAdmin(
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

* [`initialize_mesh_account`](#initialize_mesh_account)
* [`initialize_producer_account`](#initialize_producer_account)
* [`initialize_consumer_account`](#initialize_consumer_account)
* [`enable_account_as_producer`](#enable_account_as_producer)
* [`enable_account_as_consumer`](#enable_account_as_consumer)

### initialize\_mesh\_account

Sets up an AWS Account to act as the central governance account in an AWS Region.

#### Request Syntax

```python
initialize_mesh_account()
```

#### Parameters

None

#### Return Type

dict

#### Response Syntax

```json
{
	"Manager": str
	"ReadOnly": str
	"SubscriptionTracker": {
		"Table": str,
		"Stream": str,
	}
}
```

#### Response Structure

* `Manager`: ARN of the `DataMeshManager` IAM Role created for subsequent administration tasks
* `ReadOnly`: ARN of the `DataMeshReadOnly` IAM Role used to view public metadata
* `SubscriptionTracker`: dict
	* `Table`: The ARN of the DynamoDB Table used to track subscriptions over time
	* `Stream`: The ARN of the DynamoDB Stream you can subscribe to for event processing

---

### initialize\_producer\_account

Installs the required IAM security objects into an AWS Account so that it can act as a Producer.

#### Request Syntax

```python
initialize_producer_account(
	crawler_role_arn: str = None
)
```

#### Parameters

* `crawler_role_arn`: The ARN of an IAM Role to be used for Glue Crawlers by the Producer. This ARN will be enabled for iam:PassRole by the Producer principal.

#### Return Type

None

#### Response Syntax

#### Response Structure

---

### initialize\_consumer\_account

Installs the required IAM security objects into an AWS Account so that it can act as a Consumer.

#### Request Syntax

```python
initialize_consumer_account()
```

#### Parameters

None

#### Return Type

None

#### Response Syntax

#### Response Structure

---

### enable\_account\_as\_producer

Within the Data Mesh Account, enables an Account to publish data products and grant permissions using Lake Formation.

#### Request Syntax

```python
enable_account_as_producer(
	account_id: str,
	enable_crawler_role: str = None
):
```

#### Parameters

* `account_id`: The AWS Account to allow to act as a Producer
* `enable_crawler_role`: The role ARN from the Producer Account which will be updating data mesh objects.

#### Return Type

None

#### Response Syntax

#### Response Structure

---

### enable\_account\_as\_consumer

Within the Data Mesh Account, enables an Account to request subscriptions to data products and accept sharing invitations.

#### Request Syntax

```python
enable_account_as_consumer(
	account_id: str
):
```

#### Parameters

* `account_id`: The AWS Account to allow to act as a Producer

#### Return Type

None

#### Response Syntax

#### Response Structure

---