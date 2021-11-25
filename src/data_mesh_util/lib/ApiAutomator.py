import sys
import logging
import time
import boto3
import botocore.exceptions
import shortuuid
import json

from data_mesh_util.lib.constants import *
import data_mesh_util.lib.utils as utils


class ApiAutomator:
    _target_account = None
    _session = None
    _logger = None
    _logger = logging.getLogger("ApiAutomator")
    # make sure we always log to standard out
    _logger.addHandler(logging.StreamHandler(sys.stdout))
    _clients = None

    def __init__(self, target_account: str, session: boto3.session.Session, log_level: str = "INFO"):
        self._target_account = target_account
        self._session = session
        self._logger.setLevel(log_level)
        self._clients = {}

    def _get_client(self, client_name):
        client = self._clients.get(client_name)

        if client is None:
            client = self._session.client(client_name)
            self._clients[client_name] = client

        return client

    def _get_bucket_name(self, bucket_value):
        if 's3://' in bucket_value:
            return bucket_value.split('/')[2]
        else:
            return bucket_value

    def add_aws_trust_to_role(self, account_id_to_trust: str, trust_role_name: str, update_role_name: str):
        '''
        Method to add a trust relationship to an AWS Account to a Role
        :return:
        '''
        iam_client = self._get_client('iam')

        # update the  trust policy to include the provided account ID
        response = iam_client.get_role(RoleName=update_role_name)

        policy_doc = response.get('Role').get('AssumeRolePolicyDocument')

        trust_role_name = utils.get_role_arn(account_id=account_id_to_trust, role_name=trust_role_name)
        # add the account to the trust relationship
        trusted_entities = policy_doc.get('Statement')[0].get('Principal').get('AWS')
        if account_id_to_trust not in trusted_entities:
            trusted_entities.append(trust_role_name)
            policy_doc.get('Statement')[0].get('Principal')['AWS'] = trusted_entities

        print(policy_doc)
        iam_client.update_assume_role_policy(RoleName=update_role_name, PolicyDocument=json.dumps(policy_doc))

        self._logger.info("Enabled Account %s to assume %s" % (account_id_to_trust, update_role_name))

    def _validate_tag(self, tag_key: str, tag_body: dict) -> None:
        lf_client = self._get_client('lakeformation')

        # create the tag or validate it exists
        try:
            lf_client.create_lf_tag(
                TagKey=tag_key,
                TagValues=tag_body.get('ValidValues')
            )
        except lf_client.exceptions.AlreadyExistsException:
            pass
        except lf_client.exceptions.InvalidInputException as e:
            if 'Tag key already exists' in str(e):
                pass
            else:
                raise e

        # add all missing tag values to valid values (as they must have existed somewhere to be assigned)
        current_tag_values = lf_client.get_lf_tag(
            TagKey=tag_key
        ).get('TagValues')
        missing_tag_values = []
        for value in tag_body.get('TagValues'):
            if value not in current_tag_values:
                missing_tag_values.append(value)

        if len(missing_tag_values) > 0:
            lf_client.update_lf_tag(
                TagKey=tag_key,
                TagValuesToAdd=missing_tag_values
            )

    def attach_tag(self, database: str, table: str, tag: tuple):
        # create the tag or make sure it already exists
        tag_key = tag[0]
        tag_body = tag[1]
        self._validate_tag(tag_key=tag_key, tag_body=tag_body)

        # attach the tag to the table
        lf_client = self._get_client('lakeformation')
        try:
            args = {
                "Resource": {
                    'Table': {
                        'DatabaseName': database,
                        'Name': table
                    }
                },
                "LFTags": [
                    {
                        'TagKey': tag_key,
                        'TagValues': tag_body.get('TagValues')
                    },
                ]
            }
            lf_client.add_lf_tags_to_resource(**args)
        except lf_client.exceptions.AlreadyExistsException:
            pass

    def configure_iam(self, policy_name: str, policy_desc: str, policy_template: str, role_name: str, role_desc: str,
                      account_id: str, data_mesh_account_id: str, config: dict = None,
                      additional_assuming_principals: dict = None, managed_policies_to_attach: list = None):
        iam_client = self._get_client('iam')

        policy_arn = None
        try:
            # create an IAM Policy from the template
            policy_doc = utils.generate_policy(policy_template, config)

            response = iam_client.create_policy(
                PolicyName=policy_name,
                Path=DATA_MESH_IAM_PATH,
                PolicyDocument=policy_doc,
                Description=policy_desc,
                Tags=DEFAULT_TAGS
            )
            policy_arn = response.get('Policy').get('Arn')
            waiter = iam_client.get_waiter('policy_exists')
            waiter.wait(PolicyArn=policy_arn)
            self._logger.info(f"Policy {policy_name} created as {policy_arn}")
        except iam_client.exceptions.EntityAlreadyExistsException:
            policy_arn = utils.get_policy_arn(account_id, policy_name)
            while True:
                try:
                    iam_client.create_policy_version(
                        PolicyArn=policy_arn,
                        PolicyDocument=policy_doc,
                        SetAsDefault=True
                    )
                    self._logger.info(f"Policy {policy_name} version created as {policy_arn}")
                    break
                except iam_client.exceptions.LimitExceededException as le:
                    if "versions" in str(le):
                        versions = iam_client.list_policy_versions(
                            PolicyArn=policy_arn,
                            MaxItems=10
                        )

                        # delete the policy version at the last position
                        last_index = len(versions.get('Versions')) - 1
                        v = versions.get('Versions')[last_index].get('VersionId')
                        iam_client.delete_policy_version(
                            PolicyArn=policy_arn,
                            VersionId=v
                        )
                        self._logger.info(f"Deleted Policy Version {v}")

                        # after this we'll retry immediately
                    else:
                        # this is a general throttling issue and we'll retry
                        time.sleep(1)

        # create a non-root user who can assume the role
        try:
            response = iam_client.create_user(
                Path=DATA_MESH_IAM_PATH,
                UserName=role_name,
                Tags=DEFAULT_TAGS
            )
            self._logger.info(f"Created new User {role_name}")

            waiter = iam_client.get_waiter('user_exists')
            waiter.wait(UserName=role_name)
        except iam_client.exceptions.EntityAlreadyExistsException:
            self._logger.info(f"User {role_name} already exists. No action required.")

        user_arn = "arn:aws:iam::%s:user%s%s" % (account_id, DATA_MESH_IAM_PATH, role_name)

        # create a group for the user
        group_name = f"{role_name}Group"
        try:
            response = iam_client.create_group(
                Path=DATA_MESH_IAM_PATH,
                GroupName=group_name
            )
            self._logger.info(f"Created new Group {group_name}")
        except iam_client.exceptions.EntityAlreadyExistsException:
            self._logger.info(f"Group {group_name} already exists. No action required.")

        group_arn = "arn:aws:iam::%s:group%s%sGroup" % (account_id, DATA_MESH_IAM_PATH, role_name)

        # put the user into the group
        try:
            response = iam_client.add_user_to_group(
                GroupName=group_name,
                UserName=role_name
            )
            self._logger.info(f"Added User {role_name} to Group {group_name}")
        except iam_client.exceptions.EntityAlreadyExistsException:
            self._logger.info(f"User {role_name} already in {group_name}. No action required.")

        role_arn = None

        self._logger.debug("Waiting for User to be ready for inclusion in AssumeRolePolicy")

        role_created = False
        retries = 0
        while role_created is False and retries < 5:
            try:
                # now create the IAM Role with a trust policy to the indicated principal and the root user
                aws_principals = [user_arn, ("arn:aws:iam::%s:root" % account_id)]
                iam_client.create_role(
                    Path=DATA_MESH_IAM_PATH,
                    RoleName=role_name,
                    AssumeRolePolicyDocument=json.dumps(
                        utils.create_assume_role_doc(aws_principals=aws_principals,
                                                     additional_principals=additional_assuming_principals)),
                    Description=role_desc,
                    Tags=DEFAULT_TAGS
                )
                # wait for role active
                waiter = iam_client.get_waiter('role_exists')
                waiter.wait(RoleName=role_name)
                role_created = True

                role_arn = utils.get_role_arn(account_id, role_name)
            except iam_client.exceptions.EntityAlreadyExistsException:
                role_arn = iam_client.get_role(RoleName=role_name).get(
                    'Role').get('Arn')
                role_created = True
            except iam_client.exceptions.MalformedPolicyDocumentException as mpde:
                if "Invalid principal" in str(mpde):
                    # this is raised when something within IAM hasn't yet propagated correctly. Boto waiters
                    # don't seem to catch it, so we have to inject a manual sleep/retry
                    time.sleep(2)
                    retries += 1

        self._logger.info(f"Validated Role {role_name} as {role_arn}")
        self._logger.debug("Waiting for Role to be ready for Policy Attach")

        # attach the created policy to the role
        policy_attached = False
        retries = 0
        while policy_attached is False and retries < 5:
            try:
                iam_client.attach_role_policy(
                    RoleName=role_name,
                    PolicyArn=policy_arn
                )
                policy_attached = True
                self._logger.info(f"Attached Policy {policy_arn} to {role_name}")
            except iam_client.exceptions.MalformedPolicyDocumentException as mpde:
                if "Invalid principal" in str(mpde):
                    # this is raised when something within IAM hasn't yet propagated correctly.
                    time.sleep(2)
                    retries += 1

        # attach the indicated managed policies
        if managed_policies_to_attach:
            for policy in managed_policies_to_attach:
                iam_client.attach_role_policy(
                    RoleName=role_name,
                    PolicyArn="arn:aws:iam::aws:policy/%s" % policy
                )
                self._logger.info(f"Attached managed policy {policy}")

        # create an assume role policy
        policy_arn = self.create_assume_role_policy(
            source_account_id=account_id,
            policy_name=("Assume%s" % role_name),
            role_arn=role_arn
        )

        # now let the group assume the role
        iam_client.attach_group_policy(GroupName=group_name, PolicyArn=policy_arn)
        self._logger.info(f"Bound {policy_arn} to Group {group_name}")

        # let the role assume the read only consumer policy
        if account_id == data_mesh_account_id and role_name != DATA_MESH_READONLY_ROLENAME:
            iam_client.attach_role_policy(RoleName=role_name,
                                          PolicyArn=utils.get_policy_arn(data_mesh_account_id,
                                                                         f"Assume{DATA_MESH_READONLY_ROLENAME}"))

        return role_arn, user_arn, group_arn

    def create_assume_role_policy(self, source_account_id: str, policy_name: str, role_arn: str):
        iam_client = self._get_client('iam')

        # create a policy that lets someone assume this new role
        policy_arn = None
        try:
            response = iam_client.create_policy(
                PolicyName=policy_name,
                Path=DATA_MESH_IAM_PATH,
                PolicyDocument=json.dumps(utils.create_assume_role_doc(resource=role_arn)),
                Description=("Policy allowing the grantee the ability to assume Role %s" % role_arn),
                Tags=DEFAULT_TAGS
            )
            policy_arn = response.get('Policy').get('Arn')
        except iam_client.exceptions.EntityAlreadyExistsException:
            policy_arn = "arn:aws:iam::%s:policy%s%s" % (source_account_id, DATA_MESH_IAM_PATH, policy_name)

        self._logger.info(f"Validated {policy_name} as {policy_arn}")

        return policy_arn

    def leave_ram_shares(self, principal: str, ram_shares: dict) -> None:
        ram_client = self._get_client('ram')

        for object, share_info in ram_shares.items():
            ram_client.disassociate_resource_share(
                resourceShareArn=share_info.get('arn'),
                principals=[
                    principal,
                ]
            )

    def lf_grant_create_db(self, iam_role_arn: str):
        # this call is subject to race conditions with IAM roles which haven't propagated to LF, so retry 5 times
        tries = 0
        completed = False
        while completed is False and tries < 5:
            try:
                # grant this role the ability to create databases and tables
                lf_client = self._get_client('lakeformation')
                lf_client.grant_permissions(
                    Principal={
                        'DataLakePrincipalIdentifier': iam_role_arn
                    },
                    Resource={'Catalog': {}},
                    Permissions=[
                        'CREATE_DATABASE'
                    ]
                )
                self._logger.info(f"Granted {iam_role_arn} CREATE_DATABASE privileges on Catalog")
                completed = True
            except lf_client.exceptions.InvalidInputException as iie:
                if 'Invalid principal' in str(iie):
                    tries += 1
                    time.sleep(2)
                else:
                    raise iie

    def get_table_partitions(self, database_name: str, table_name: str) -> list:
        # load the partitions for the table if there are any
        glue_client = self._get_client('glue')
        all_partitions = []
        partition_args = {
            "DatabaseName": database_name,
            "TableName": table_name,
            "ExcludeColumnSchema": False
        }
        has_more_partitions = True
        while has_more_partitions is True:
            partitions = glue_client.get_partitions(**partition_args)
            all_partitions.extend(partitions.get('Partitions'))

            if 'NextToken' in partitions:
                partition_args['NextToken'] = partitions.get('NextToken')
            else:
                has_more_partitions = False

        return all_partitions

    def enable_crawler_role(self, crawler_role_arn: str, grant_to_role_name: str):
        if crawler_role_arn is None or grant_to_role_name is None:
            raise Exception("Cannot enable Crawler Role without Role Arn and Target Role Name")

        crawler_role_name = crawler_role_arn.split('/')[-1]
        config = {
            "role_name": crawler_role_name,
            "role_arn": crawler_role_arn
        }
        policy = utils.generate_policy("enable_crawler_role.pystache", config)
        iam_client = self._get_client('iam')
        try:
            response = iam_client.create_policy(
                PolicyName=f"EnableCrawlerRole{crawler_role_name}",
                Path=DATA_MESH_IAM_PATH,
                PolicyDocument=policy,
                Description=(f"Policy allowing the grantee to pass Crawler Role {crawler_role_name}"),
                Tags=DEFAULT_TAGS
            )
            policy_arn = response.get('Policy').get('Arn')
            waiter = iam_client.get_waiter('policy_exists')
            waiter.wait(PolicyArn=policy_arn)
        except iam_client.exceptions.EntityAlreadyExistsException:
            pass

        # attach to the input role
        iam_client.attach_role_policy(
            RoleName=grant_to_role_name,
            PolicyArn=policy_arn
        )

        self._logger.info(f"Enabled {grant_to_role_name} to pass role {crawler_role_name} to Glue Crawlers")

    def create_table_partition_metadata(self, database_name: str, table_name: str, partition_input_list: list):
        glue_client = self._get_client('glue')

        partitions_created = 0
        for p in partition_input_list:
            keys = [
                'DatabaseName', 'TableName', 'CreationTime', 'LastAnalyzedTime', 'CatalogId'
            ]
            p = utils.remove_dict_keys(input_dict=p, remove_keys=keys)

            try:
                glue_client.create_partition(
                    DatabaseName=database_name,
                    TableName=table_name,
                    PartitionInput=p
                )
                partitions_created += 1
            except glue_client.exceptions.AlreadyExistsException:
                pass

        self._logger.info(f"Create {partitions_created} new Table Partitions")

    def load_glue_tables(self, catalog_id: str, source_db_name: str,
                         table_name_regex: str, load_lf_tags: bool = True):
        glue_client = self._get_client('glue')
        lf_client = self._get_client('lakeformation')

        # get the tables which are included in the set provided through args
        get_tables_args = {
            'CatalogId': catalog_id,
            'DatabaseName': source_db_name
        }

        # add the table filter as a regex matching anything including the provided table
        if table_name_regex is not None:
            get_tables_args['Expression'] = table_name_regex

        finished_reading = False
        last_token = None
        all_tables = []

        def _no_data():
            raise Exception("Unable to find any Tables matching %s in Database %s" % (table_name_regex,
                                                                                      source_db_name))

        while finished_reading is False:
            if last_token is not None:
                get_tables_args['NextToken'] = last_token

            try:
                get_table_response = glue_client.get_tables(
                    **get_tables_args
                )
            except glue_client.exceptions.EntityNotFoundException:
                _no_data()

            if 'NextToken' in get_table_response:
                last_token = get_table_response.get('NextToken')
            else:
                finished_reading = True

            # add the tables returned from this instance of the request
            if not get_table_response.get('TableList'):
                _no_data()
            else:
                all_tables.extend(get_table_response.get('TableList'))

        self._logger.info(f"Loaded {len(all_tables)} tables matching {table_name_regex} from Glue")

        # now load all lakeformation tags for the supplied objects
        if load_lf_tags is True:
            for t in all_tables:
                tags = lf_client.get_resource_lf_tags(
                    CatalogId=catalog_id,
                    Resource={
                        'Table': {
                            'CatalogId': catalog_id,
                            'DatabaseName': t.get('DatabaseName'),
                            'Name': t.get('Name')
                        }
                    },
                    ShowAssignedLFTags=True
                )
                key = 'LFTagsOnTable'
                use_tags = {}
                if tags.get(key) is not None and len(tags.get(key)) > 0:
                    for table_tag in tags.get(key):
                        # get all the valid values for the tag in LF
                        lf_tag = lf_client.get_lf_tag(TagKey=table_tag.get('TagKey'))
                        use_tags[table_tag.get('TagKey')] = {
                            'TagValues': table_tag.get('TagValues'),
                            'ValidValues': lf_tag.get('TagValues')
                        }
                    t['Tags'] = use_tags

        return all_tables

    def update_glue_catalog_resource_policy(self, region: str, producer_account_id: str, consumer_account_id: str,
                                            database_name: str, tables: list):
        glue_client = self._get_client('glue')
        new_resource_policy = None
        current_resource_policy = None
        try:
            current_resource_policy = glue_client.get_resource_policy()
        except glue_client.exceptions.EntityNotFoundException:
            pass

        cf = {
            'region': region,
            'producer_account_id': producer_account_id,
            'consumer_account_id': consumer_account_id,
            "database_name": database_name,
            'tables': tables
        }

        # add the table list and generate full policy
        cf['table_list'] = tables
        policy = json.loads(utils.generate_policy('lf_cross_account_tbac.pystache', config=cf))

        policy_condition = None
        if current_resource_policy is None:
            new_resource_policy = {
                "Version": "2012-10-17",
                "Statement": [policy]
            }
            glue_client.put_resource_policy(
                PolicyInJson=json.dumps(new_resource_policy),
                PolicyExistsCondition='NOT_EXIST',
                EnableHybrid='TRUE'
            )
            self._logger.info(
                f"Created new Catalog Resource Policy on {producer_account_id} allowing Tag Based Access by {consumer_account_id}")
        else:
            new_resource_policy = json.loads(current_resource_policy.get('PolicyInJson'))
            current_hash = current_resource_policy.get('PolicyHash')

            update_statement, policy_index, did_modification = self._get_glue_resource_policy_statement_to_modify(
                region=region,
                policy=new_resource_policy, producer_account_id=producer_account_id,
                consumer_account_id=consumer_account_id,
                database_name=database_name, tables=tables
            )

            # add the new statement
            if update_statement is None:
                new_resource_policy['Statement'].append(policy)
                did_modification = True
            elif update_statement is not None:
                new_resource_policy['Statement'][policy_index] = update_statement

            if did_modification is True:
                glue_client.put_resource_policy(
                    PolicyInJson=json.dumps(new_resource_policy),
                    PolicyHashCondition=current_hash,
                    PolicyExistsCondition='MUST_EXIST',
                    EnableHybrid='TRUE'
                )
                self._logger.info(
                    f"Updated Catalog Resource Policy on {producer_account_id} allowing Tag Based Access by {consumer_account_id}")

    def _get_glue_resource_policy_statement_to_modify(self, region: str, policy: dict, producer_account_id: str,
                                                      consumer_account_id: str,
                                                      database_name: str, tables: list) -> tuple:
        # go through the policy to find if there's a match on region, consumer principal, and database
        target_statement_index = 0
        statement_match = None
        missing_tables = []
        if tables is not None:
            missing_tables = tables.copy()

        for i, statement in enumerate(policy.get('Statement')):
            if statement is not None and 'AWS' in statement.get('Principal') and consumer_account_id in statement.get(
                    'Principal').get('AWS'):
                # go through the resources to get region and DB match
                for j, resource in enumerate(statement.get('Resource')):
                    # resources will be in format:
                    #
                    # "arn:aws:glue:<region>:<account-id>:table/*",
                    # "arn:aws:glue:<region>:<account-id>:database/*",
                    # "arn:aws:glue:<region>:<account-id>:catalog"
                    if region in resource and producer_account_id in resource and (
                            ':database' in resource and database_name in resource):
                        # this statement is a match
                        target_statement_index = i
                        statement_match = statement
                        break

        did_modification = False
        if statement_match is not None:
            for k, resource in enumerate(statement_match.get('Resource')):
                resource_name = resource.split("/")[-1]
                try:
                    resource_index = missing_tables.index(resource_name)
                    del missing_tables[resource_index]
                except ValueError:
                    pass

            # add the tables that were missing
            if len(missing_tables) > 0:
                statement_match['Resource'].extend(missing_tables)
                did_modification = True

            return statement_match, target_statement_index, did_modification
        else:
            return None, None, None

    def assert_is_data_lake_admin(self, principal):
        lf_client = self._get_client('lakeformation')

        admin_matched = False
        for admin in lf_client.get_data_lake_settings().get('DataLakeSettings').get("DataLakeAdmins"):
            if principal == admin.get('DataLakePrincipalIdentifier'):
                admin_matched = True
                break

        if admin_matched is False:
            raise Exception(f"Principal {principal} is not Data Lake Admin")

    def _get_dummy_bucket_name(self):
        return f"data-mesh-delete-me-{self._target_account}"

    def _create_dummy_bucket(self, aws_region: str):
        s3_client = self._get_client('s3')

        try:
            args = {
                "ACL": 'private',
                "Bucket": self._get_dummy_bucket_name()
            }

            if aws_region != 'us-east-1':
                args["CreateBucketConfiguration"] = {
                    'LocationConstraint': aws_region
                }
            s3_client.create_bucket(**args)
        except s3_client.exceptions.BucketAlreadyExists:
            pass

    def _drop_dummy_bucket(self):
        s3_client = self._get_client('s3')

        s3_client.delete_bucket(
            Bucket=self._get_dummy_bucket_name()
        )

    def get_or_create_lf_svc_linked_role(self, aws_region: str):
        # check if the role already exists
        iam_client = self._get_client('iam')
        svc_role = 'AWSServiceRoleForLakeFormationDataAccess'
        existing_role = None
        try:
            existing_role = iam_client.get_role(
                RoleName=svc_role
            )
        except iam_client.exceptions.NoSuchEntityException:
            pass

        if existing_role is not None:
            return existing_role.get('Role').get('Arn')
        else:
            # create a dummy bucket in S3
            self._create_dummy_bucket(aws_region)
            dummy_s3_arn = utils.convert_s3_path_to_arn(f"s3://{self._get_dummy_bucket_name()}")

            # register the bucket as a data lake location
            lf_client = self._get_client('lakeformation')
            try:
                lf_client.register_resource(
                    ResourceArn=dummy_s3_arn,
                    UseServiceLinkedRole=True
                )
            except lf_client.exceptions.AlreadyExistsException:
                pass

            # confirm service linked role exists
            waiter = iam_client.get_waiter(waiter_name='role_exists')
            waiter.wait(
                RoleName=svc_role
            )

            # drop the data lake location
            lf_client.deregister_resource(
                ResourceArn=dummy_s3_arn
            )

            # drop the bucket
            self._drop_dummy_bucket()

            # return the role arn
            return f"arn:aws:iam::{self._target_account}:role/aws-service-role/lakeformation.amazonaws.com/AWSServiceRoleForLakeFormationDataAccess"

    def describe_table(self, database_name: str, table_name: str):
        glue_client = self._get_client('glue')

        table = glue_client.get_table(
            DatabaseName=database_name,
            Name=table_name
        )

        return table.get('Table')

    def lf_batch_revoke_permissions(self,
                                    data_mesh_account_id: str,
                                    consumer_account_id: str,
                                    permissions: list,
                                    database_name: str,
                                    grantable_permissions: list = None,
                                    table_list: list = None) -> int:
        lf_client = self._get_client('lakeformation')

        entries = []

        for t in table_list:
            entries.extend(self.create_lf_permissions_entry(
                data_mesh_account_id=data_mesh_account_id,
                target_account_id=consumer_account_id,
                database_name=database_name,
                table_name=t,
                permissions=permissions,
                grantable_permissions=grantable_permissions,
                target_batch=True
            ))

        response = lf_client.batch_revoke_permissions(
            CatalogId=data_mesh_account_id,
            Entries=entries
        )
        perms_revoked = len(entries)
        if 'Failures' in response:
            perms_revoked -= len(response.get('Failures'))

        return perms_revoked

    def create_lf_permissions_entry(self,
                                    data_mesh_account_id: str,
                                    target_account_id: str,
                                    database_name: str, table_name: str,
                                    permissions: list,
                                    grantable_permissions: list = None,
                                    target_batch: bool = False
                                    ) -> list:
        db_entries = []
        table_entries = []
        column_entries = []

        log_message = None
        if table_name is None:
            # create a db resource
            entry = {
                "Principal": {
                    'DataLakePrincipalIdentifier': target_account_id
                },
                "Resource": {
                    'Database': {
                        'CatalogId': data_mesh_account_id,
                        'Name': database_name
                    }
                },
                "Permissions": permissions
            }
            if target_batch is True:
                entry["Id"] = shortuuid.uuid()

            log_message = f"{target_account_id} Database {database_name} Permissions:{permissions}"
            if grantable_permissions is not None:
                entry["PermissionsWithGrantOption"] = grantable_permissions
                log_message = f"{log_message}, {grantable_permissions} WITH GRANT OPTION"

            self._logger.info(log_message)
            db_entries.append(entry)
        else:
            # create grants at table and column level depending on what's being granted/revoked
            if 'SELECT' in permissions:
                entry = {
                    "Principal": {
                        'DataLakePrincipalIdentifier': target_account_id
                    },
                    "Resource": {
                        'TableWithColumns': {
                            'CatalogId': data_mesh_account_id,
                            'DatabaseName': database_name,
                            'Name': table_name,
                            'ColumnWildcard': {}
                        }
                    },
                    "Permissions": ['SELECT']
                }
                log_message = f"{target_account_id} Table {table_name} Column Permissions:{permissions}"

                if grantable_permissions is not None and 'SELECT' in grantable_permissions:
                    entry["PermissionsWithGrantOption"] = grantable_permissions
                    log_message = f"{log_message}, {grantable_permissions} WITH GRANT OPTION"

                column_entries.append(entry)
                self._logger.info(log_message)

            # remove select from remaining permissions
            other_permissions = list(set(permissions) - set(['SELECT']))
            other_grantable_permissions = None
            if grantable_permissions is not None:
                other_grantable_permissions = list(set(grantable_permissions) - set(['SELECT']))

            if other_permissions is not None and len(other_permissions) > 0:
                entry = {
                    "Principal": {
                        'DataLakePrincipalIdentifier': target_account_id
                    },
                    "Resource": {
                        'Table': {
                            'CatalogId': data_mesh_account_id,
                            'DatabaseName': database_name,
                            'Name': table_name
                        }
                    },
                    "Permissions": other_permissions
                }
                log_message = f"{target_account_id} Table {table_name} Permissions:{other_permissions}"

                if other_grantable_permissions is not None and len(other_grantable_permissions) > 0:
                    entry["PermissionsWithGrantOption"] = other_grantable_permissions
                    log_message = f"{log_message}, {other_grantable_permissions} WITH GRANT OPTION"

                self._logger.info(log_message)
                table_entries.append(entry)

        # create a list of the permissions groups
        final_entries = []
        final_entries.extend(table_entries)
        final_entries.extend(column_entries)
        final_entries.extend(db_entries)

        return final_entries

    def lf_batch_grant_permissions(self,
                                   data_mesh_account_id: str,
                                   target_account_id: str,
                                   permissions: list,
                                   database_name: str,
                                   grantable_permissions: list = None,
                                   table_list: list = None) -> int:
        entries = []

        # always grant describe
        if 'DESCRIBE' not in permissions:
            permissions.append('DESCRIBE')

        for t in table_list:
            entries.extend(self.create_lf_permissions_entry(data_mesh_account_id=data_mesh_account_id,
                                                            target_account_id=target_account_id,
                                                            database_name=database_name, table_name=t,
                                                            permissions=permissions,
                                                            grantable_permissions=grantable_permissions,
                                                            target_batch=True))

        lf_client = self._get_client('lakeformation')

        response = lf_client.batch_grant_permissions(
            CatalogId=data_mesh_account_id,
            Entries=entries
        )

        perms_added = len(entries)
        if 'Failures' in response:
            perms_added -= len(response.get('Failures'))

        return perms_added

    def lf_grant_permissions(self, data_mesh_account_id: str, principal: str, database_name: str,
                             table_name: str = None,
                             permissions: list = ['ALL'],
                             grantable_permissions: list = None) -> None:
        table_list = table_name if isinstance(table_name, list) else [table_name]
        return self.lf_batch_grant_permissions(
            data_mesh_account_id=data_mesh_account_id,
            target_account_id=principal,
            database_name=database_name,
            table_list=table_list,
            permissions=permissions,
            grantable_permissions=grantable_permissions
        )

    def create_crawler(self, crawler_role: str, database_name: str, table_name: str, s3_location: str,
                       sync_schedule: str, enable_lineage: bool = True):
        glue_client = self._get_client('glue')
        crawler_name = '%s-%s' % (database_name, table_name)
        try:
            glue_client.get_crawler(Name=crawler_name)
        except glue_client.exceptions.from_code('EntityNotFoundException'):
            glue_client.create_crawler(
                Name=crawler_name,
                Role=crawler_role,
                DatabaseName=database_name,
                Description="S3 Crawler to sync structure of %s.%s to Data Mesh" % (database_name, table_name),
                Targets={
                    'S3Targets': [
                        {
                            'Path': s3_location
                        },
                    ]
                },
                Schedule="cron(0 */4 * * ? *)" if sync_schedule is None else sync_schedule,
                SchemaChangePolicy={
                    'UpdateBehavior': 'LOG',
                    'DeleteBehavior': 'LOG'
                },
                RecrawlPolicy={
                    'RecrawlBehavior': 'CRAWL_NEW_FOLDERS_ONLY'
                },
                LineageConfiguration={
                    'CrawlerLineageSettings': 'ENABLE' if enable_lineage is True else 'DISABLE'
                },
                Tags=utils.flatten_default_tags()
            )
            self._logger.info("Created new Glue Crawler %s" % crawler_name)

        # create lakeformation permissions in the mesh account for the glue crawler role

        # create s3 permission for glue crawler role

        return crawler_name

    def create_remote_table(self, data_mesh_account_id: str,
                            database_name: str,
                            local_table_name: str,
                            remote_table_name: str) -> None:
        try:
            glue_client = self._get_client(('glue'))
            glue_client.create_table(
                DatabaseName=database_name,
                TableInput={"Name": local_table_name,
                            "TargetTable": {"CatalogId": data_mesh_account_id,
                                            "DatabaseName": database_name,
                                            "Name": remote_table_name
                                            }
                            }
            )
            self._logger.info(f"Created Resource Link Table {local_table_name}")
        except glue_client.exceptions.from_code('AlreadyExistsException'):
            self._logger.info(f"Resource Link Table {local_table_name} Already Exists")

    def get_or_create_database(self, database_name: str, database_desc: str, source_account: str = None):
        glue_client = self._get_client('glue')

        args = {
            "DatabaseInput": {
                "Name": database_name,
                "Description": database_desc,
            }
        }

        if source_account is not None:
            args['DatabaseInput']['TargetDatabase'] = {
                "CatalogId": source_account,
                "DatabaseName": database_name
            }
            del args['DatabaseInput']["Description"]

        # create the database
        try:
            created_db = glue_client.create_database(
                **args
            )

            # tag the database with default tags
            glue_client.tag_resource(
                ResourceArn=created_db.get('Arn'),
                TagsToAdd=DEFAULT_TAGS
            )
        except glue_client.exceptions.AlreadyExistsException:
            pass

        self._logger.info(f"Verified Database {database_name}")

    def set_default_db_permissions(self, database_name: str):
        glue_client = self._get_client('glue')

        glue_client.update_database(
            CatalogId=self._target_account,
            Name=database_name,
            DatabaseInput={
                "Name": database_name,
                "CreateTableDefaultPermissions": []
            }
        )

    def set_default_lf_permissions(self):
        # remove default IAM settings in lakeformation for the account, and setup the manager role and this caller as admins
        lf_client = self._get_client('lakeformation')
        settings = lf_client.get_data_lake_settings().get('DataLakeSettings')
        settings['CreateTableDefaultPermissions'] = []
        lf_client.put_data_lake_settings(DataLakeSettings=settings)

    def add_datalake_admin(self, principal: str):
        lf_client = self._get_client('lakeformation')

        admins = lf_client.get_data_lake_settings().get('DataLakeSettings').get("DataLakeAdmins")

        admins.append({
            'DataLakePrincipalIdentifier': principal
        })
        # Horrible retry logic required to avoid boto3 exception using a role as a principal too soon after it's been created
        retries = 0
        while True:
            try:
                lf_client.put_data_lake_settings(
                    DataLakeSettings={
                        'DataLakeAdmins': admins
                    }
                )
            except lf_client.exceptions.InvalidInputException:
                self._logger.info(f"Error setting DataLakeAdmins as {admins}. Backing off....")
                retries += 1
                if retries > 5:
                    raise
                time.sleep(3)
                continue
            break

    def _get_s3_path_prefix(self, prefix: str) -> str:
        return prefix.replace(f"s3://{self._get_bucket_name(prefix)}", "")

    def _transform_bucket_policy(self, bucket_policy: dict, principal_account: str,
                                 access_path: str) -> dict:
        use_bucket_name = self._get_bucket_name(access_path)
        policy_sid = f"{BUCKET_POLICY_STATEMENT_SID}-{use_bucket_name}"

        # generate a new bucket policy from the template
        s3_path = self._get_s3_path_prefix(access_path)
        base_policy = json.loads(utils.generate_policy(template_file='producer_bucket_policy.pystache', config={
            'account_id': principal_account,
            'access_path': s3_path,
            'sid': policy_sid
        }))

        if bucket_policy is None:
            generated_policy = {
                "Version": "2012-10-17",
                "Id": shortuuid.uuid(),
                "Statement": [
                    base_policy
                ]
            }
            self._logger.info(
                f"Creation new S3 Bucket policy enabling Data Mesh LakeFormation Service Role access for {principal_account}")
            self._logger.info(f"Creating new Bucket Policy for {access_path}")
            return generated_policy
        else:
            # we already have a bucket policy, so determine if there is already a data mesh grant created for this bucket
            statements = bucket_policy.get('Statement')
            data_mesh_statement_index = -1

            for i, s in enumerate(statements):
                if s.get('Sid') == policy_sid:
                    data_mesh_statement_index = i
                    break

            if data_mesh_statement_index == -1:
                # there was not a previously created data mesh auth statement, so add it to the end
                statements.append(base_policy)
                self._logger.info(
                    f"Adding new Data Mesh LakeFormation Service Role statement for {principal_account} to existing Bucket Policy")
            else:
                # we already have a data mesh auth statement, so check if the principal is already there first
                statement = statements[data_mesh_statement_index]
                set_principal = f"arn:aws:iam::{principal_account}:role/aws-service-role/lakeformation.amazonaws.com/AWSServiceRoleForLakeFormationDataAccess"
                if set_principal not in statement.get('Principal').get('AWS'):
                    current_principals = statement.get('Principal').get('AWS')
                    if isinstance(current_principals, list):
                        statement.get('Principal').get('AWS').append(set_principal)
                    else:
                        statement.get('Principal')['AWS'] = [current_principals, set_principal]

                    statements[data_mesh_statement_index] = statement
                    self._logger.info(
                        f"Adding principal {principal_account} to existing Data Mesh LakeFormation Service Role statement")

                    bucket_policy['Statement'] = statements
                else:
                    self._logger.info(
                        f"Not modifying bucket policy as principal {principal_account} has already been added")

            return bucket_policy

    def _get_current_bucket_policy(self, s3_client, bucket_name: str):
        try:
            current_policy = s3_client.get_bucket_policy(Bucket=bucket_name)
            return current_policy
        except botocore.exceptions.ClientError as ce:
            if 'NoSuchBucketPolicy' in str(ce):
                return None
            else:
                raise ce

    def add_bucket_policy_entry(self, principal_account: str, access_path: str):
        s3_client = self._get_client('s3')

        bucket_name = self._get_bucket_name(access_path)

        # get the existing policy, if there is one
        current_policy = self._get_current_bucket_policy(s3_client, bucket_name)

        bucket_policy = None
        if current_policy is not None:
            bucket_policy = json.loads(current_policy.get('Policy'))

        # transform the existing or None policy into the desired target lakeformation policy
        new_policy = self._transform_bucket_policy(
            bucket_policy=bucket_policy, principal_account=principal_account,
            access_path=access_path
        )

        # put the policy back into the bucket store
        s3_client.put_bucket_policy(Bucket=bucket_name, Policy=json.dumps(new_policy))

    def accept_pending_lf_resource_shares(self, sender_account: str, filter_resource_arn: str = None):
        ram_client = self._get_client('ram')

        get_response = ram_client.get_resource_share_invitations()

        accepted_share = False
        for r in get_response.get('resourceShareInvitations'):
            # only accept peding lakeformation shares from the source account
            if r.get('senderAccountId') == sender_account and 'LakeFormation' in r.get('resourceShareName') and r.get(
                    'status') == 'PENDING':
                if filter_resource_arn is None or r.get('resourceShareArn') == filter_resource_arn:
                    ram_client.accept_resource_share_invitation(
                        resourceShareInvitationArn=r.get('resourceShareInvitationArn')
                    )
                    accepted_share = True
                    self._logger.info(f"Accepted RAM Share {r.get('resourceShareInvitationArn')}")

        if accepted_share is False:
            self._logger.info("No Pending RAM Shares to Accept")
