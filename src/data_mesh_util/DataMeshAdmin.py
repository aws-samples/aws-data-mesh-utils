import boto3
import sys
import logging
import botocore.session

from data_mesh_util.lib.constants import *
import data_mesh_util.lib.utils as utils
from data_mesh_util.lib.SubscriberTracker import SubscriberTracker
from data_mesh_util.lib.ApiAutomator import ApiAutomator


class DataMeshAdmin:
    _region = None
    _current_identity = None
    _current_credentials = None
    _data_mesh_account_id = None
    _data_producer_account_id = None
    _data_producer_role_arn = None
    _data_consumer_account_id = None
    _data_consumer_role_arn = None
    _data_mesh_manager_role_arn = None
    _session = None
    _iam_client = None
    _lf_client = None
    _sts_client = None
    _dynamo_client = None
    _dynamo_resource = None
    _config = {}
    _logger = logging.getLogger("DataMeshAdmin")
    _log_level = None
    stream_handler = logging.StreamHandler(sys.stdout)
    _logger.addHandler(stream_handler)
    _subscriber_tracker = None
    _automator = None

    def __init__(self, data_mesh_account_id: str, region_name: str = 'us-east-1', log_level: str = "INFO",
                 use_credentialss=None):
        self._data_mesh_account_id = data_mesh_account_id
        # get the region for the module
        if region_name is None:
            raise Exception("Cannot initialize a Data Mesh without an AWS Region")
        else:
            self._region = region_name

        if use_credentialss is None:
            self._session = boto3.session.Session(region_name=self._region)
        else:
            self._session = utils.create_session(credentials=use_credentialss, region=self._region)

        self._iam_client = self._session.client('iam')
        self._sts_client = self._session.client('sts')
        self._dynamo_client = self._session.client('dynamodb')
        self._dynamo_resource = self._session.client('dynamodb')
        self._lf_client = self._session.client('lakeformation')

        self._current_identity = self._sts_client.get_caller_identity()

        self._logger.setLevel(log_level)
        self._log_level = log_level
        self._automator = ApiAutomator(target_account=data_mesh_account_id, session=self._session,
                                       log_level=self._log_level)

        self._logger.debug(f"Running as {self._current_identity.get('Arn')}")

    def _create_template_config(self, config: dict):
        if config is None:
            config = {}

        # add the data mesh account to the config if it isn't provided
        if "data_mesh_account_id" not in config:
            config["data_mesh_account_id"] = self._data_mesh_account_id

        if "producer_account_id" not in config:
            config["producer_account_id"] = self._data_producer_account_id

        if "consumer_account_id" not in config:
            config["consumer_account_id"] = self._data_consumer_account_id

        self._logger.debug(self._config)

    def _create_data_mesh_ro_role(self):
        '''
        Private method to create objects needed for read-only access to the data mesh catalog
        :return:
        '''
        utils.validate_correct_account(credentials=botocore.session.get_session().get_credentials(),
                                       account_id=self._data_mesh_account_id)

        self._create_template_config(self._config)

        current_identity = self._sts_client.get_caller_identity()

        ro_tuple = self._automator.configure_iam(
            policy_name='DataMeshReadOnlyPolicy',
            policy_desc='IAM Policy to provide read-only access to metadata',
            policy_template="data_mesh_read_only_policy.pystache",
            role_name=DATA_MESH_READONLY_ROLENAME,
            role_desc='Role to be used for read-only operations on Catalog',
            account_id=self._data_mesh_account_id,
            data_mesh_account_id=self._data_mesh_account_id,
            config=self._config)

        return ro_tuple

    def _create_data_mesh_manager_role(self):
        '''
        Private method to create objects needed for an administrative role that can be used to grant access to Data Mesh roles
        :return:
        '''
        utils.validate_correct_account(credentials=botocore.session.get_session().get_credentials(),
                                       account_id=self._data_mesh_account_id)

        self._create_template_config(self._config)

        current_identity = self._sts_client.get_caller_identity()

        mgr_tuple = self._automator.configure_iam(
            policy_name='DataMeshManagerPolicy',
            policy_desc='IAM Policy to bootstrap the Data Mesh Admin',
            policy_template="data_mesh_setup_iam_policy.pystache",
            role_name=DATA_MESH_MANAGER_ROLENAME,
            role_desc='Role to be used for the Data Mesh Manager function',
            account_id=self._data_mesh_account_id,
            data_mesh_account_id=self._data_mesh_account_id,
            config=self._config)
        data_mesh_mgr_role_arn = mgr_tuple[0]

        self._logger.info("Validated Data Mesh Manager Role %s" % data_mesh_mgr_role_arn)

        if current_identity.get('Arn').startswith(f"arn:aws:sts::{current_identity.get('Account')}:assumed-role/"):
            self._logger.info(
                f"Executing using an assumed role so setting the ROLE as the LakeFormation principal, rather than the USER.")
            executing_user_role = f"arn:aws:iam::{current_identity.get('Account')}:role/{current_identity.get('Arn').split('/')[1]}"
        else:
            executing_user_role = current_identity.get('Arn')

        # grant the data mesh manager and current caller Data Lake Admin rights
        self._automator.add_datalake_admin(principal=data_mesh_mgr_role_arn)
        self._automator.add_datalake_admin(principal=executing_user_role)

        # force the creation of the lakeformation service linked role if it isn't there already
        svc_role = self._automator.get_or_create_lf_svc_linked_role(aws_region=self._region)
        self._logger.info(f"Validated Lake Formation Service Linked Role as {svc_role}")

        # turn off IAM permissions on new databases by default
        self._automator.set_default_lf_permissions()

        self._logger.info(f"New Admins are {current_identity.get('Account')} and {executing_user_role}")

        return mgr_tuple

    def _create_producer_role(self, account_id: str):
        '''
        Private method to create objects needed for a Producer account to connect to the Data Mesh and create data products
        :return:
        '''
        self._create_template_config(self._config)

        # create the policy and role to be used for data producers
        producer_tuple = self._automator.configure_iam(
            policy_name=f'DataMeshProducerPolicy-{account_id}',
            policy_desc=f'IAM Role enabling Account {account_id} to become a Data Producer',
            policy_template="producer_mesh_policy.pystache",
            role_name=utils.get_central_role_name(account_id=account_id, type=PRODUCER),
            role_desc=f'Role to be used for Data Mesh Producer {account_id}',
            account_id=self._data_mesh_account_id,
            data_mesh_account_id=self._data_mesh_account_id,
            config=self._config)
        producer_iam_role_arn = producer_tuple[0]
        self._logger.info("Validated Data Mesh Producer Role %s" % producer_iam_role_arn)

        self._automator.lf_grant_create_db(iam_role_arn=producer_iam_role_arn)

        # make the iam role a data lake admin
        self._automator.add_datalake_admin(principal=producer_iam_role_arn)
        self._logger.info(f"Granted {producer_iam_role_arn} Data Lake Admin")

        return producer_tuple

    def _create_consumer_role(self, account_id: str):
        '''
        Private method to create objects needed for a Consumer account to connect to the Data Mesh and mirror data
        products into their account
        :return:
        '''
        self._create_template_config(self._config)

        return self._automator.configure_iam(
            policy_name=f'DataMeshConsumerPolicy-{account_id}',
            policy_desc=f'IAM Role enabling Account {account_id} to become Data Consumers',
            policy_template="consumer_mesh_policy.pystache",
            role_name=utils.get_central_role_name(account_id=account_id, type=CONSUMER),
            role_desc=f'Role to be used for Data Mesh Consumer {account_id}',
            account_id=self._data_mesh_account_id,
            data_mesh_account_id=self._data_mesh_account_id,
            config=self._config)

    def _api_tuple(self, item_tuple: tuple):
        return {
            "RoleArn": item_tuple[0],
            "UserArn": item_tuple[1],
            "GroupArn": item_tuple[2]
        }

    def initialize_mesh_account(self):
        '''
        Sets up an AWS Account to act as a Data Mesh central account. This method should be invoked by an Administrator
        of the Data Mesh Account. Creates IAM Roles & Policies for the DataMeshManager, DataProducer, and DataConsumer
        :return:
        '''
        self._data_mesh_account_id = self._sts_client.get_caller_identity().get('Account')

        self._current_credentials = boto3.session.Session().get_credentials()
        self._subscription_tracker = SubscriberTracker(data_mesh_account_id=self._data_mesh_account_id,
                                                       credentials=self._current_credentials,
                                                       region_name=self._region,
                                                       log_level=self._log_level)

        # create the read-only consumer role for metadata descriptions
        ro_tuple = self._create_data_mesh_ro_role()

        # create a new IAM role in the Data Mesh Account to be used for future grants
        mgr_tuple = self._create_data_mesh_manager_role()

        return {
            "Manager": self._api_tuple(mgr_tuple),
            "ReadOnly": self._api_tuple(ro_tuple),
            "SubscriptionTracker": self._subscription_tracker.get_endpoints()
        }

    def initialize_producer_account(self, crawler_role_arn: str = None):
        '''
        Sets up an AWS Account to act as a Data Provider into the central Data Mesh Account. This method should be invoked
        by an Administrator of the Producer Account. Creates IAM Role & Policy to get and put restricted S3 Bucket Policies.
        Requires at least 1 S3 Bucket Policy be enabled for future grants.
        :return:
        '''
        return self._initialize_account_as(type=PRODUCER, crawler_role_arn=crawler_role_arn)

    def _add_trust_relationship(self, account_id: str, trust_role: str, update_role: str):
        '''
        Enables a remote role to act as a data consumer by granting them access to the DataMeshAdminConsumer Role
        :return:
        '''
        utils.validate_correct_account(self._session.get_credentials(), self._data_mesh_account_id)

        # create trust relationships for the AdminProducer roles
        self._automator.add_aws_trust_to_role(account_id_to_trust=account_id,
                                              trust_role_name=trust_role,
                                              update_role_name=update_role)

    def enable_account_as_producer(self, account_id: str, enable_crawler_role: str = None):
        '''
        Enables a remote role to act as a data producer by granting them access to the DataMeshAdminProducer Role
        :return:
        '''
        if account_id is None:
            raise Exception("Must Provide Account ID")

        self._create_producer_role(account_id=account_id)

        self._add_trust_relationship(
            account_id=account_id,
            trust_role=DATA_MESH_PRODUCER_ROLENAME,
            update_role=utils.get_central_role_name(account_id=account_id, type=PRODUCER)
        )

        # add trust to the read only role
        self._add_trust_relationship(
            account_id=account_id,
            trust_role=DATA_MESH_PRODUCER_ROLENAME,
            update_role=DATA_MESH_READONLY_ROLENAME
        )

    def enable_account_as_consumer(self, account_id: str):
        '''
        Enables a remote account to act as a data consumer by granting them access to the DataMeshAdminConsumer Role
        :return:
        '''
        if account_id is None:
            raise Exception("Must Provide Account ID")

        self._create_consumer_role(account_id=account_id)

        self._add_trust_relationship(
            account_id=account_id,
            trust_role=DATA_MESH_CONSUMER_ROLENAME,
            update_role=utils.get_central_role_name(account_id=account_id, type=CONSUMER)
        )

        # add trust to the read only role
        self._add_trust_relationship(
            account_id=account_id,
            trust_role=DATA_MESH_CONSUMER_ROLENAME,
            update_role=DATA_MESH_READONLY_ROLENAME
        )

    def _initialize_account_as(self, type: str, crawler_role_arn: str = None):
        '''
        Sets up an AWS Account to act as a Data Producer or Consumer from the central Data Mesh Account. This method should
        be invoked by an Administrator of the Producer or Consumer Account. Creates IAM Role & Policy which allows a remote account to
        access the central IAM Roles and produce or subscribe to products.
        :return:
        '''
        utils.validate_correct_account(self._session.get_credentials(), self._data_mesh_account_id,
                                       should_match=False)

        source_account = self._sts_client.get_caller_identity().get('Account')

        local_role_name = None
        remote_role_name = None
        remote_role_arn = None
        policy_name = None
        policy_template = None

        if type == CONSUMER:
            self._data_consumer_account_id = source_account
            local_role_name = DATA_MESH_CONSUMER_ROLENAME
            remote_role_name = f"{DATA_MESH_ADMIN_CONSUMER_ROLENAME}-{source_account}"
            policy_name = CONSUMER_POLICY_NAME
            policy_template = "consumer_account_policy.pystache"
            target_account = self._data_consumer_account_id
        else:
            self._data_producer_account_id = source_account
            local_role_name = DATA_MESH_PRODUCER_ROLENAME
            remote_role_name = f"{DATA_MESH_ADMIN_PRODUCER_ROLENAME}-{source_account}"
            policy_name = PRODUCER_POLICY_NAME
            policy_template = "producer_account_policy.pystache"
            target_account = self._data_producer_account_id

        # run a pre-flight check here to check that the caller has data lake admin
        self._automator.assert_is_data_lake_admin(
            principal=self._current_identity.get('Arn'))

        self._logger.info(f"Setting up Account {source_account} as a Data {type}")

        group_name = f"{local_role_name}Group"

        # setup the consumer IAM role, user, and group
        iam_details = self._automator.configure_iam(
            policy_name=policy_name,
            policy_desc=f'IAM Policy enabling Accounts to Assume the {local_role_name} Role',
            policy_template=policy_template,
            role_name=local_role_name,
            role_desc=f'{local_role_name} facilitating principals to act as {type}',
            account_id=target_account,
            data_mesh_account_id=self._data_mesh_account_id
        )

        self._logger.info(f"Role {iam_details[0]}")
        self._logger.info(f"User {iam_details[1]}")
        self._logger.info(f"Group {iam_details[2]}")

        local_role_arn = iam_details[0]
        if type == CONSUMER:
            self._data_consumer_role_arn = iam_details[0]
        else:
            self._data_producer_role_arn = iam_details[0]

        # allow the local group to assume the local role
        policy_name = f"Assume{local_role_name}"
        policy_arn = self._automator.create_assume_role_policy(
            source_account_id=target_account,
            policy_name=policy_name,
            role_arn=local_role_arn
        )
        self._logger.debug(f"Validated Policy {policy_name} as {policy_arn}")
        self._iam_client.attach_group_policy(GroupName=group_name, PolicyArn=policy_arn)
        self._logger.info(f"Bound {policy_arn} to Group {group_name}")

        # allow the local iam role to assume the remote data mesh iam role
        remote_role_arn = utils.get_role_arn(account_id=self._data_mesh_account_id, role_name=remote_role_name)
        policy_arn = self._automator.create_assume_role_policy(
            source_account_id=target_account,
            policy_name=f"Assume{remote_role_name}",
            role_arn=remote_role_arn
        )
        self._iam_client.attach_role_policy(RoleName=local_role_name, PolicyArn=policy_arn)
        self._logger.info(f"Enabled Account {target_account} to Assume {remote_role_arn} through {policy_arn}")

        # create a service linked role for lakeformation
        try:
            self._iam_client.create_service_linked_role(
                AWSServiceName='lakeformation.amazonaws.com'
            )
            self._logger.info("Created new Service Linked Role for AWS LakeFormation")
        except self._iam_client.exceptions.InvalidInputException as iie:
            if "has been taken in this account, please try a different suffix" in str(iie):
                pass
            else:
                raise iie
        except self._iam_client.exceptions.AlreadyExistsException:
            pass

        # allow the local role to create databases, if they don't have it already
        self._automator.lf_grant_create_db(iam_role_arn=local_role_arn)

        # enable the crawler role if provided
        if crawler_role_arn is not None:
            self.enable_crawler_passrole(crawler_role_arn=crawler_role_arn, target_role=local_role_arn)

        return iam_details

    def initialize_consumer_account(self):
        '''
        Sets up an AWS Account to act as a Data Consumer from the central Data Mesh Account. This method should be invoked
        by an Administrator of the Consumer Account. Creates IAM Role & Policy which allows an end user to assume the
        DataMeshAdminConsumer Role and subscribe to products.
        :return:
        '''
        return self._initialize_account_as(type=CONSUMER, crawler_role_arn=None)
