import logging
import sys
import re
import shortuuid
from datetime import datetime
from boto3.dynamodb.conditions import Attr, Or, And, Key
from enum import Enum
from data_mesh_util.lib.constants import *
import data_mesh_util.lib.utils as utils

STATUS_ACTIVE = 'Active'
STATUS_DENIED = 'Denied'
STATUS_PENDING = 'Pending'
STATUS_DELETED = 'Deleted'
SUBSCRIPTION_ID = 'SubscriptionId'
OWNER_PRINCIPAL = 'OwnerPrincipal'
SUBSCRIBER_PRINCIPAL = 'SubscriberPrincipal'
STATUS = 'Status'
CREATION_DATE = 'CreationDate'
CREATED_BY = 'CreatedBy'
UPDATED_DATE = 'UpdatedDate'
UPDATED_BY = 'UpdatedBy'
DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
DATABASE_NAME = 'DatabaseName'
TABLE_NAME = 'TableName'
REQUESTED_GRANTS = 'RequestedGrants'
PERMITTED_GRANTS = 'PermittedGrants'
GRANTABLE_GRANTS = 'GrantableGrants'
TABLE_ARNS = 'GrantedTableARNs'
RAM_SHARES = 'RamShares'
NOTES = 'Notes'


class SubType(Enum):
    DATABASE = 1
    TABLE = 2
    DATA_PRODUCT = 3
    DOMAIN = 4


def _generate_id():
    return shortuuid.uuid()


def _format_time_now():
    return datetime.now().strftime(DATE_FORMAT)


class SubscriberTracker:
    _data_mesh_account_id = None
    _dynamo_client = None
    _dynamo_resource = None
    _glue_client = None
    _sts_client = None
    _table_info = None
    _table = None
    _logger = None
    _region = None

    def __init__(self, credentials, data_mesh_account_id: str, region_name: str, log_level: str = "INFO"):
        '''
        Initialize a subscriber tracker. Requires the external creation of clients because we will span roles
        :param dynamo_client:
        :param dynamo_resource:
        :param log_level:
        '''
        self._data_mesh_account_id = data_mesh_account_id
        self._region = region_name
        self._dynamo_client = utils.generate_client(service='dynamodb', region=region_name,
                                                    credentials=credentials)
        self._dynamo_resource = utils.generate_resource(service='dynamodb', region=region_name,
                                                        credentials=credentials)
        self._glue_client = utils.generate_client(service='glue', region=region_name,
                                                  credentials=credentials)
        self._iam_client = utils.generate_client(service='iam', region=region_name,
                                                 credentials=credentials)
        self._sts_client = utils.generate_client(service='sts', region=region_name,
                                                 credentials=credentials)

        # validate that we are running from within the mesh
        utils.validate_correct_account(credentials=credentials, account_id=data_mesh_account_id)

        self._table_info = self._init_table()

        _logger = logging.getLogger("SubscriberTracker")

        # make sure we always log to standard out
        _logger.addHandler(logging.StreamHandler(sys.stdout))
        _logger.setLevel(log_level)

    def _who_am_i(self):
        return self._sts_client.get_caller_identity().get('Arn')

    def _add_www(self, item: dict, new: bool = True, notes: str = None):
        '''
        Method to decorate a DynamoDB item with Who What When attributes
        :param item:
        :param principal:
        :param new:
        :return:
        '''
        if new:
            item[CREATION_DATE] = _format_time_now()
            item[CREATED_BY] = self._who_am_i()
        else:
            item[UPDATED_DATE] = _format_time_now()
            item[UPDATED_BY] = self._who_am_i()

        if notes is not None:
            item[NOTES] = notes

        return item

    def _upd_www(self, args: dict):
        # check that the updates haven't already been added
        if "#upd_dt" not in list(args.get("ExpressionAttributeNames").keys()):
            # split the update expression and extract the SET portion, which we will rewrite
            tokens = re.split('(ADD|SET)', args.get("UpdateExpression"), flags=re.IGNORECASE)
            set_clause = tokens[tokens.index('SET') + 1]
            add_clause = tokens[tokens.index('ADD') + 1]

            # add the update expression, names, and values
            set_clause = "%s, #upd_dt = :upd_dt, #upd_by = :upd_by" % set_clause
            args["ExpressionAttributeNames"]["#upd_dt"] = UPDATED_DATE
            args["ExpressionAttributeNames"]["#upd_by"] = UPDATED_BY
            args["ExpressionAttributeValues"][":upd_dt"] = _format_time_now()
            args["ExpressionAttributeValues"][":upd_by"] = self._who_am_i()

            args["UpdateExpression"] = "SET %s ADD %s" % (set_clause, add_clause)

            return args

    def _init_table(self):
        t = None
        try:
            response = self._dynamo_client.describe_table(
                TableName=SUBSCRIPTIONS_TRACKER_TABLE
            )

            t = response.get('Table')
        except self._dynamo_client.exceptions.ResourceNotFoundException:
            t = self._create_table()

        self._table = self._dynamo_resource.Table(SUBSCRIPTIONS_TRACKER_TABLE)

        return {
            'Table': t.get('TableArn'),
            'Stream': t.get('LatestStreamArn')
        }

    def subscriber_indexname(self):
        return "%s-%s" % (SUBSCRIPTIONS_TRACKER_TABLE, 'Subscriber')

    def owner_indexname(self):
        return "%s-%s" % (SUBSCRIPTIONS_TRACKER_TABLE, 'Owner')

    def _create_table(self):
        response = self._dynamo_client.create_table(
            TableName=SUBSCRIPTIONS_TRACKER_TABLE,
            AttributeDefinitions=[
                {
                    'AttributeName': SUBSCRIPTION_ID,
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': SUBSCRIBER_PRINCIPAL,
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': OWNER_PRINCIPAL,
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': STATUS,
                    'AttributeType': 'S'
                }
            ],
            KeySchema=[
                {
                    'AttributeName': SUBSCRIPTION_ID,
                    'KeyType': 'HASH'
                }
            ],
            GlobalSecondaryIndexes=[
                {
                    'IndexName': self.owner_indexname(),
                    'KeySchema': [
                        {
                            'AttributeName': OWNER_PRINCIPAL,
                            'KeyType': 'HASH',
                        },
                        {
                            'AttributeName': STATUS,
                            'KeyType': 'RANGE',
                        }
                    ],
                    'Projection': {
                        'ProjectionType': 'ALL'
                    }
                },
                {
                    'IndexName': self.subscriber_indexname(),
                    'KeySchema': [
                        {
                            'AttributeName': SUBSCRIBER_PRINCIPAL,
                            'KeyType': 'HASH',
                        }
                    ],
                    'Projection': {
                        'ProjectionType': 'ALL'
                    }
                }
            ],
            BillingMode='PAY_PER_REQUEST',
            StreamSpecification={
                'StreamEnabled': True,
                'StreamViewType': 'NEW_AND_OLD_IMAGES'
            },
            Tags=DEFAULT_TAGS
        )

        # block until the table is ACTIVE
        t = self._dynamo_resource.Table(SUBSCRIPTIONS_TRACKER_TABLE)
        t.wait_until_exists()

        return response.get('TableDescription')

    def get_endpoints(self):
        return self._table_info

    def _validate_objects(self, database_name: str, tables: list, suppress_object_validation: bool = False):
        for table_name in tables:
            # validate if the table exists
            exists = self._validate_object(database_name=database_name, table_name=table_name,
                                           suppress_object_validation=suppress_object_validation)

            if not exists:
                raise Exception("Table %s does not exist in Database %s" % (table_name, database_name))

    def _validate_object(self, database_name: str, table_name: str, suppress_object_validation: bool = False):
        if suppress_object_validation is True:
            return True
        else:
            try:
                response = self._glue_client.get_table(
                    DatabaseName=database_name,
                    Name=table_name
                )

                if 'Table' not in response:
                    return False
                else:
                    return True
            except (
                    self._glue_client.exceptions.AccessDeniedException,
                    self._glue_client.exceptions.EntityNotFoundException):
                # if we get access denied here, it's because the object doesn't exist
                return False

    def create_subscription_request(self, owner_account_id: str, principal: str,
                                    request_grants: list, domain=None, data_product_name=None,
                                    database_name: str = None, tables: list = None,
                                    suppress_object_validation: bool = False) -> dict:
        # look up if there is already a subscription request for this object
        subscription_type = None
        if database_name is not None:
            filter = Attr(DATABASE_NAME).eq(database_name)
            if tables is None:
                subscription_type = SubType.DATABASE
            else:
                subscription_type = SubType.TABLE
        elif data_product_name is not None:
            filter = And(Attr(DATA_PRODUCT_TAG_KEY).eq(data_product_name))
            subscription_type = SubType.DATA_PRODUCT
        elif domain is not None:
            filter = And(Attr(DOMAIN_TAG_KEY).eq(domain))
            subscription_type = SubType.DOMAIN

        def _sub_exists():
            found = self._table.query(
                IndexName=self.subscriber_indexname(),
                Select='ALL_ATTRIBUTES',
                ConsistentRead=False,
                KeyConditionExpression=Key(SUBSCRIBER_PRINCIPAL).eq(principal),
                FilterExpression=filter
            )

            for i in found.get('Items'):
                if (subscription_type == SubType.TABLE and tables == i.get(TABLE_NAME)) and request_grants == i.get(
                        REQUESTED_GRANTS):
                    return i

            return None

        def _put_subscription(item: dict):
            item = self._add_www(item=item)

            self._table.put_item(
                Item=item
            )

        # check if a subscription already exists
        subscription = _sub_exists()

        # create the base subscription object to be inserted into DDB
        item = {
            SUBSCRIPTION_ID: _generate_id() if subscription is None else subscription.get(SUBSCRIPTION_ID),
            OWNER_PRINCIPAL: owner_account_id,
            SUBSCRIBER_PRINCIPAL: principal,
            REQUESTED_GRANTS: request_grants,
            STATUS: STATUS_PENDING if subscription is None else subscription.get(STATUS)
        }

        sub_type = ()

        def _return():
            return {
                "Type": subscription_type.name,
                sub_type[0]: sub_type[1],
                SUBSCRIPTION_ID: item.get(SUBSCRIPTION_ID)
            }

        if subscription_type == SubType.DATABASE:
            # validate that the database exists
            exists = self._validate_object(database_name=database_name,
                                           suppress_object_validation=suppress_object_validation)
            if not exists:
                raise Exception("Database %s does not exist" % (database_name))
            else:
                # create a database level subscription
                item[DATABASE_NAME] = database_name
                sub_type = DATABASE_NAME, database_name
        elif subscription_type == SubType.TABLE:
            # validate the table list
            self._validate_objects(database_name=database_name, tables=tables,
                                   suppress_object_validation=suppress_object_validation)
            item[DATABASE_NAME] = database_name
            item[TABLE_NAME] = tables
            sub_type = TABLE_NAME, tables
        elif subscription_type == SubType.DOMAIN:
            # create a domain level subscription
            item[DOMAIN_TAG_KEY] = domain
            sub_type = DOMAIN_TAG_KEY, domain
        else:
            # create a data product level subscription
            item[DATA_PRODUCT_TAG_KEY] = data_product_name
            _put_subscription(item=item, principal=principal)
            sub_type = DATA_PRODUCT_TAG_KEY, data_product_name

        _put_subscription(item=item)
        return _return()

    def get_subscription(self, subscription_id: str, force: bool = False) -> dict:
        args = {
            "Key": {
                SUBSCRIPTION_ID: subscription_id
            },
            "ConsistentRead": True
        }

        item = self._table.get_item(**args)

        i = item.get("Item")
        if i is None:
            return None
        else:
            if i.get(STATUS) != STATUS_DELETED or force:
                return i

    def _arg_builder(self, key: str, value):
        if value is not None:
            if isinstance(value, str):
                return Attr(key).eq(value)
            elif isinstance(value, list):
                # for this use case, lists are OR'ed together
                k = Attr(key)

                # add the first value from the list
                or_clause = Or(k.eq(value[0]), k.eq(value[1]))

                def _or_closure(value):
                    return Or(or_clause, k.eq(value))

                for v in value[2:]:
                    _or_closure(v)

                return or_clause
        else:
            return None

    def _build_filter_expression(self, args: dict):
        filter = None

        for arg in args.items():
            if arg[1] is not None:
                if filter is None:
                    filter = Attr(arg[0]).eq(arg[1])
                else:
                    filter = And(filter, Attr(arg[0]).eq(arg[1]))

        # add the deleted filter
        filter = And(filter, Attr(STATUS).ne(STATUS_DELETED))

        return filter

    def list_subscriptions(self, owner_id: str = None, principal_id: str = None, database_name: str = None,
                           tables: list = None, includes_grants: list = None, request_status: str = None,
                           start_token: str = None) -> dict:
        args = {}

        def _add_arg(key: str, value):
            if value is not None:
                args[key] = value

        _add_arg("TableName", SUBSCRIPTIONS_TRACKER_TABLE)
        _add_arg("ExclusiveStartKey", start_token)

        if principal_id is not None:
            _add_arg("IndexName", self.subscriber_indexname())
            _add_arg("KeyConditionExpression", Key(SUBSCRIBER_PRINCIPAL).eq(principal_id))
            _add_arg("Select", "ALL_PROJECTED_ATTRIBUTES")
            _add_arg("FilterExpression", Attr(STATUS).ne(STATUS_DELETED))

            response = self._table.query(**args)
            return self._format_list_response(response)
        elif owner_id is not None and request_status is not None:
            _add_arg("IndexName", self.owner_indexname())
            key_condition = And(Key(OWNER_PRINCIPAL).eq(owner_id), Key(STATUS).eq(request_status))
            _add_arg("KeyConditionExpression", key_condition)
            _add_arg("Select", "ALL_PROJECTED_ATTRIBUTES")

            response = self._table.query(**args)
            return self._format_list_response(response)
        else:
            # build the filter expression
            filter_expression = self._build_filter_expression(
                {
                    OWNER_PRINCIPAL: owner_id,
                    SUBSCRIBER_PRINCIPAL: principal_id,
                    DATABASE_NAME: database_name,
                    TABLE_NAME: tables,
                    REQUESTED_GRANTS: includes_grants
                })
            _add_arg("FilterExpression", filter_expression)

            response = self._table.scan(**args)
            return self._format_list_response(response)

    def _format_list_response(self, response) -> dict:
        response_items = []

        # filter out values not relevant to the requestor
        for i in response.get('Items'):
            del i[STATUS]
            del i[OWNER_PRINCIPAL]

            response_items.append(i)
        out = {
            'Subscriptions': response_items
        }
        lek = 'LastEvaluatedKey'
        if lek in response:
            out[lek] = response.get(lek)

        return out

    def _handle_update(self, args: dict):
        ist = "Invalid State Transition"

        # add the consumed capacity metric which allows us to check if the update worked
        if "ReturnConsumedCapacity" not in args:
            args["ReturnConsumedCapacity"] = 'TOTAL'

        # add who information
        args = self._upd_www(args)

        try:
            response = self._table.update_item(**args)

            if response is None or response.get('ConsumedCapacity') is None or response.get('ConsumedCapacity').get(
                    'CapacityUnits') == 0:
                raise Exception(ist)
            else:
                return True
        except Exception as e:
            if 'ConditionalCheckFailedException' in str(e):
                pass
            else:
                raise e

    def delete_subscription(self, subscription_id: str, reason: str):
        self.update_status(
            subscription_id=subscription_id, status=STATUS_DELETED,
            notes=reason
        )

    def update_grants(self, subscription_id: str, permitted_grants: list, notes: str, grantable_grants: list = None):
        set_expressions = [
            "#permitted = :permitted"
        ]
        expression_attribute_names = {
            "#permitted": PERMITTED_GRANTS,
            "#notes": NOTES
        }
        expression_attribute_values = {
            ":permitted": permitted_grants,
            ":notes": {notes}
        }

        if grantable_grants is not None:
            set_expressions.append("#grantable = :grantable")
            expression_attribute_names["#grantable"] = GRANTABLE_GRANTS
            expression_attribute_values[":grantable"] = grantable_grants

        args = {
            "Key": {
                SUBSCRIPTION_ID: subscription_id
            },
            "UpdateExpression": f"SET {','.join(set_expressions)} ADD #notes :notes",
            "ExpressionAttributeNames": expression_attribute_names,
            "ExpressionAttributeValues": expression_attribute_values
        }

        return self._handle_update(args)

    def update_status(self, subscription_id: str, status: str, table_arns: list, permitted_grants: list = None,
                      grantable_grants: list = None, notes: str = None, ram_shares: dict = None):
        '''
        Updates the status of a subscription. Valid transitions are:
        PENDING->ACTIVE
        PENDING->DENIED
        DENIED->ACTIVE
        ACTIVE->DELETED
        DELETED->ACTIVE
        DELETED->PENDING

        :param subscription_id:
        :param status:
        :return:
        '''
        # build the map of proposed status to allowed status
        status_attr = Attr(STATUS)
        expected = None
        if status == STATUS_ACTIVE:
            expected = Or(Or(Or(status_attr.eq(STATUS_PENDING), status_attr.eq(STATUS_DENIED)),
                             status_attr.eq(STATUS_DELETED)), status_attr.eq(STATUS_ACTIVE))
        elif status == STATUS_DENIED:
            expected = status_attr.eq(STATUS_PENDING)
        elif status == STATUS_DELETED:
            expected = status_attr.eq(STATUS_ACTIVE)
        elif status == STATUS_PENDING:
            expected = status_attr.eq(STATUS_DELETED)

        set_expressions = [
            "#status = :status",
            "#permitted = :permitted",
            "#table_arns = :table_arns",
            "#grantable = :grantable",
        ]

        expression_attribute_names = {
            "#status": STATUS,
            "#permitted": PERMITTED_GRANTS,
            "#grantable": GRANTABLE_GRANTS,
            "#table_arns": TABLE_ARNS
        }
        expression_attribute_values = {
            ":status": status,
            ":table_arns": table_arns,
            ":grantable": grantable_grants
        }

        # add the permitted grants if they are provided
        if permitted_grants is not None and len(permitted_grants) > 0:
            expression_attribute_values[":permitted"] = permitted_grants
        else:
            # permitted grants will be set to whatever was previously requested
            current_sub = self.get_subscription(subscription_id=subscription_id)
            expression_attribute_values[":permitted"] = current_sub.get(REQUESTED_GRANTS)

        if ram_shares is not None:
            set_expressions.append("#ram = :ram")
            expression_attribute_names["#ram"] = RAM_SHARES
            expression_attribute_values[":ram"] = ram_shares

        # add the notes field as a set if we got any
        add_clause = ""
        if notes is not None:
            add_clause = "ADD #notes :notes"
            expression_attribute_names["#notes"] = NOTES
            expression_attribute_values[":notes"] = {notes}

        update_expression = f"SET {','.join(set_expressions)} {add_clause}".strip()
        args = {
            "Key": {
                SUBSCRIPTION_ID: subscription_id
            },
            "UpdateExpression": update_expression,
            "ExpressionAttributeNames": expression_attribute_names,
            "ExpressionAttributeValues": expression_attribute_values,
            "ConditionExpression": expected
        }

        return self._handle_update(args)
