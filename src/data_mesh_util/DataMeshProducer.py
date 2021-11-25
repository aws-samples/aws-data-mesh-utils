import time
import boto3
import os
import sys

from data_mesh_util.lib.ApiAutomator import ApiAutomator

sys.path.append(os.path.join(os.path.dirname(__file__), "resource"))
sys.path.append(os.path.join(os.path.dirname(__file__), "lib"))

from data_mesh_util.lib.SubscriberTracker import *


class DataMeshProducer:
    _data_mesh_account_id = None
    _data_producer_account_id = None
    _data_mesh_manager_role_arn = None
    _session = None
    _iam_client = None
    _sts_client = None
    _config = {}
    _current_region = None
    _log_level = None
    _logger = logging.getLogger("DataMeshProducer")
    stream_handler = logging.StreamHandler(sys.stdout)
    _logger.addHandler(stream_handler)
    _data_mesh_account_id = None
    _data_producer_role_arn = None
    _data_mesh_credentials = None
    _data_mesh_boto_session = None
    _subscription_tracker = None
    _data_producer_identity = None
    _producer_automator = None
    _mesh_automator = None

    def __init__(self, data_mesh_account_id: str, region_name: str, log_level: str = "INFO", use_credentials=None):
        self._data_mesh_account_id = data_mesh_account_id

        if region_name is None:
            raise Exception("Cannot initialize a Data Mesh Producer without an AWS Region")
        else:
            self._current_region = region_name

        # Assume the producer account DataMeshProducer role, unless we have been supplied temporary credentials for that role
        self._session, _producer_credentials = utils.assume_iam_role(role_name=DATA_MESH_PRODUCER_ROLENAME,
                                                                     region_name=self._current_region,
                                                                     use_credentials=use_credentials)

        self._iam_client = self._session.client('iam')
        self._sts_client = self._session.client('sts')

        self._log_level = log_level
        self._logger.setLevel(log_level)

        self._data_producer_identity = self._sts_client.get_caller_identity()
        self._data_producer_account_id = self._data_producer_identity.get('Account')

        self._producer_automator = ApiAutomator(target_account=self._data_producer_account_id,
                                                session=self._session, log_level=self._log_level)

        # now assume the DataMeshProducer-<account-id> Role in the Mesh Account
        self._data_mesh_session, self._data_mesh_credentials = utils.assume_iam_role(
            role_name=utils.get_central_role_name(self._data_producer_account_id, PRODUCER),
            region_name=self._current_region,
            use_credentials=_producer_credentials,
            target_account=self._data_mesh_account_id
        )

        # validate that we are running in the data mesh account
        utils.validate_correct_account(self._data_mesh_credentials, self._data_mesh_account_id)

        # generate an API Automator in the mesh
        self._mesh_automator = ApiAutomator(target_account=self._data_mesh_account_id,
                                            session=self._data_mesh_session, log_level=self._log_level)

        self._logger.debug("Created new STS Session for Data Mesh Admin Producer")
        self._logger.debug(self._data_mesh_credentials)

        self._subscription_tracker = SubscriberTracker(credentials=self._data_mesh_credentials,
                                                       data_mesh_account_id=data_mesh_account_id,
                                                       region_name=self._current_region,
                                                       log_level=log_level)

    def _create_mesh_table(self, table_def: dict, data_mesh_glue_client, source_database_name: str,
                           data_mesh_database_name: str,
                           producer_account_id: str,
                           data_mesh_account_id: str, create_public_metadata: bool = True,
                           expose_table_references_with_suffix: str = "_link", use_original_table_name: bool = False):
        '''
        API to create a table as a data product in the data mesh
        :param table_def:
        :param data_mesh_glue_client:
        :param data_mesh_lf_client:
        :param producer_ram_client:
        :param producer_glue_client:
        :param data_mesh_database_name:
        :param producer_account_id:
        :param data_mesh_account_id:
        :return:
        '''
        # cleanup the TableInfo object to be usable as a TableInput
        # remove properties from a TableInfo object returned from get_table to be compatible with put_table
        keys = [
            'DatabaseName', 'CreateTime', 'UpdateTime', 'CreatedBy', 'IsRegisteredWithLakeFormation', 'CatalogId',
            'Tags'
        ]
        t = utils.remove_dict_keys(input_dict=table_def, remove_keys=keys)
        t['Owner'] = producer_account_id

        self._logger.debug("Existing Table Definition")
        self._logger.debug(t)

        table_name = t.get('Name')

        # create the glue catalog entry
        try:
            data_mesh_glue_client.create_table(
                DatabaseName=data_mesh_database_name,
                TableInput=t
            )
            self._logger.info(f"Created new Glue Table {table_name}")
        except data_mesh_glue_client.exceptions.from_code('AlreadyExistsException'):
            self._logger.info(f"Glue Table {table_name} Already Exists")

        table_partitions = self._producer_automator.get_table_partitions(
            database_name=source_database_name,
            table_name=table_name
        )

        if table_partitions is not None and len(table_partitions) > 0:
            self._mesh_automator.create_table_partition_metadata(
                database_name=data_mesh_database_name,
                table_name=table_name,
                partition_input_list=table_partitions
            )

        # grant access to the producer account
        perms = ['INSERT', 'SELECT', 'ALTER', 'DELETE', 'DESCRIBE']
        created_object = self._mesh_automator.lf_grant_permissions(
            data_mesh_account_id=self._data_mesh_account_id,
            principal=producer_account_id,
            database_name=data_mesh_database_name,
            table_name=table_name,
            permissions=perms,
            grantable_permissions=perms
        )

        # if create public metadata is True, then grant describe to the general data mesh consumer role
        if create_public_metadata is True:
            created_object = self._mesh_automator.lf_grant_permissions(
                data_mesh_account_id=self._data_mesh_account_id,
                principal=utils.get_role_arn(self._data_mesh_account_id, DATA_MESH_READONLY_ROLENAME),
                database_name=data_mesh_database_name,
                table_name=table_name,
                permissions=['DESCRIBE'],
                grantable_permissions=None
            )

        # in the producer account, accept the RAM share after 1 second - seems to be an async delay
        if created_object is not None:
            time.sleep(1)
            self._producer_automator.accept_pending_lf_resource_shares(
                sender_account=data_mesh_account_id
            )

            # create a resource link for the data mesh table in producer account
            if use_original_table_name is True:
                link_table_name = table_name
            else:
                link_table_name = f"{table_name}_link"
                if expose_table_references_with_suffix is not None:
                    link_table_name = f"{table_name}{expose_table_references_with_suffix}"

            self._producer_automator.create_remote_table(
                data_mesh_account_id=self._data_mesh_account_id,
                database_name=data_mesh_database_name,
                local_table_name=link_table_name,
                remote_table_name=table_name
            )

            return table_name, link_table_name

    def _make_database_name(self, database_name: str):
        return "%s-%s" % (database_name, self._data_producer_identity.get('Account'))

    def migrate_tables_to_mesh(self, source_database_name: str,
                               create_public_metadata: bool = True,
                               table_name_regex: str = None,
                               domain: str = None,
                               data_product_name: str = None,
                               sync_mesh_catalog_schedule: str = None,
                               sync_mesh_crawler_role_arn: str = None):
        # create a data product in the mesh, and share back to the producer as the same object
        created_products = self.create_data_products(
            source_database_name=source_database_name,
            create_public_metadata=create_public_metadata,
            table_name_regex=table_name_regex,
            domain=domain,
            data_product_name=data_product_name,
            sync_mesh_catalog_schedule=sync_mesh_catalog_schedule,
            sync_mesh_crawler_role_arn=sync_mesh_crawler_role_arn,
            expose_data_mesh_db_name=source_database_name,
            expose_table_references_with_suffix=None,
            use_original_table_name=True
        )

        table_list = [t.get('SourceTable') for t in created_products.get('Tables')]

        # remove the existing objects from the producer account
        self._producer_automator.safe_remove_glue_objects(
            database_name=source_database_name,
            table_list=table_list
        )

    def create_data_products(self, source_database_name: str,
                             create_public_metadata: bool = True,
                             table_name_regex: str = None,
                             domain: str = None,
                             data_product_name: str = None,
                             sync_mesh_catalog_schedule: str = None,
                             sync_mesh_crawler_role_arn: str = None,
                             expose_data_mesh_db_name: str = None,
                             expose_table_references_with_suffix: str = "_link",
                             use_original_table_name: bool = False):
        # generate the target database name for the mesh
        data_mesh_database_name = self._make_database_name(source_database_name)
        if expose_data_mesh_db_name is not None:
            data_mesh_database_name = expose_data_mesh_db_name

        # create clients for the new credentials in the data mesh account
        data_mesh_glue_client = utils.generate_client(service='glue', region=self._current_region,
                                                      credentials=self._data_mesh_credentials)
        data_mesh_lf_client = utils.generate_client(service='lakeformation', region=self._current_region,
                                                    credentials=self._data_mesh_credentials)

        # load the specified tables to be created as data products
        all_tables = self._producer_automator.load_glue_tables(
            catalog_id=self._data_producer_account_id,
            source_db_name=source_database_name,
            table_name_regex=table_name_regex
        )

        # get or create the target database exists in the mesh account
        self._mesh_automator.get_or_create_database(
            database_name=data_mesh_database_name,
            database_desc="Database to contain objects from Source Database %s.%s" % (
                self._data_producer_account_id, source_database_name)
        )
        self._logger.info("Validated Data Mesh Database %s" % data_mesh_database_name)

        # set default permissions on db
        self._mesh_automator.set_default_db_permissions(database_name=data_mesh_database_name)

        # grant the producer permissions to create tables on this database
        self._mesh_automator.lf_grant_permissions(
            data_mesh_account_id=self._data_mesh_account_id,
            principal=self._data_producer_account_id,
            database_name=data_mesh_database_name,
            permissions=['CREATE_TABLE', 'DESCRIBE'],
            grantable_permissions=None
        )
        self._logger.info("Granted access on Database %s to Producer" % data_mesh_database_name)

        # get or create a data mesh shared database in the producer account
        self._producer_automator.get_or_create_database(
            database_name=data_mesh_database_name,
            database_desc="Database to contain objects objects shared with the Data Mesh Account",
        )
        self._logger.info("Validated Producer Account Database %s" % data_mesh_database_name)

        shared_objects = {
            DATABASE_NAME: source_database_name,
            'Tables': []
        }
        for table in all_tables:
            table_s3_path = table.get('StorageDescriptor').get('Location')

            table_s3_arn = utils.convert_s3_path_to_arn(table_s3_path)

            # create a data lake location for the s3 path
            try:
                data_mesh_lf_client.register_resource(
                    ResourceArn=table_s3_arn,
                    UseServiceLinkedRole=True
                )
            except data_mesh_lf_client.exceptions.AlreadyExistsException:
                pass

            # grant data lake location access
            producer_central_role_arn = utils.get_role_arn(account_id=self._data_mesh_account_id,
                                                           role_name=utils.get_central_role_name(
                                                               account_id=self._data_producer_account_id,
                                                               type=PRODUCER))
            data_mesh_lf_client.grant_permissions(
                Principal={
                    'DataLakePrincipalIdentifier': producer_central_role_arn
                },
                Resource={
                    'DataLocation': {'ResourceArn': table_s3_arn}
                },
                Permissions=['DATA_LOCATION_ACCESS']
            )

            # create a mesh table for the local copy
            created_table = self._create_mesh_table(
                table_def=table,
                data_mesh_glue_client=data_mesh_glue_client,
                source_database_name=source_database_name,
                data_mesh_database_name=data_mesh_database_name,
                producer_account_id=self._data_producer_account_id,
                data_mesh_account_id=self._data_mesh_account_id,
                create_public_metadata=create_public_metadata,
                expose_table_references_with_suffix=expose_table_references_with_suffix,
                use_original_table_name=use_original_table_name
            )

            shared_objects.get('Tables').append({
                'SourceTable': created_table[0],
                'LinkTable': created_table[1]
            })

            # propagate lakeformation tags and attach to table
            if 'Tags' in table:
                for tag in table.get('Tags').items():
                    self._mesh_automator.attach_tag(database=data_mesh_database_name, table=table.get('Name'), tag=tag)

            # add the domain tag
            if domain is not None:
                self._mesh_automator.attach_tag(
                    database=data_mesh_database_name,
                    table=table.get('Name'),
                    tag=(DOMAIN_TAG_KEY, {'TagValues': [domain], 'ValidValues': [domain]})
                )

            # add the data product tag
            if data_product_name is not None:
                self._mesh_automator.attach_tag(
                    database=data_mesh_database_name,
                    table=table.get('Name'),
                    tag=(DATA_PRODUCT_TAG_KEY, {'TagValues': [data_product_name], 'ValidValues': [data_product_name]})
                )

            # add a bucket policy entry allowing the data mesh lakeformation service linked role to perform GetObject*
            table_bucket = table_s3_path.split("/")[2]
            self._producer_automator.add_bucket_policy_entry(
                principal_account=self._data_mesh_account_id,
                access_path=table_bucket
            )

            if sync_mesh_catalog_schedule is not None:
                glue_crawler = self._producer_automator.create_crawler(
                    database_name=data_mesh_database_name,
                    table_name=created_table,
                    s3_location=table_s3_path,
                    crawler_role=sync_mesh_crawler_role_arn,
                    sync_schedule=sync_mesh_catalog_schedule
                )

        return shared_objects

    def get_data_product(self, database_name: str, table_name_regex: str):
        # generate a new glue client for the data mesh account
        data_mesh_glue_client = utils.generate_client('glue', region=self._current_region,
                                                      credentials=self._data_mesh_credentials)
        # grab the tables that match the regex
        all_tables = self._load_glue_tables(
            glue_client=data_mesh_glue_client,
            catalog_id=self._data_mesh_account_id,
            source_db_name=self._make_database_name(database_name),
            table_name_regex=table_name_regex
        )
        response = []
        for t in all_tables:
            response.append({"Database": t.get('DatabaseName'), "TableName": t.get('Name'),
                             "Location": t.get('StorageDescriptor').get("Location")})

        return response

    def list_pending_access_requests(self):
        '''
        Lists all access requests that have been made by potential consumers. Pending requests can be approved or denied
        with close_access_request()
        :return:
        '''
        me = self._sts_client.get_caller_identity().get('Account')
        return self._subscription_tracker.list_subscriptions(owner_id=me, request_status=STATUS_PENDING)

    def approve_access_request(self, request_id: str,
                               grant_permissions: list = None,
                               grantable_permissions: list = None,
                               decision_notes: str = None):
        '''
        API to close an access request as approved. Approvals must be accompanied by the
        permissions to grant to the specified principal.
        :param request_id:
        :param grant_permissions:
        :param grantable_permissions
        :param decision_notes:
        :return:
        '''
        # load the subscription
        subscription = self._subscription_tracker.get_subscription(subscription_id=request_id)

        # approver can override the requested grants
        if grant_permissions is None:
            set_permissions = subscription.get(REQUESTED_GRANTS)
        else:
            set_permissions = grant_permissions

        # grant the approved permissions in lake formation
        data_mesh_lf_client = utils.generate_client(service='lakeformation', region=self._current_region,
                                                    credentials=self._data_mesh_credentials)
        tables = subscription.get(TABLE_NAME)
        ram_shares = {}

        table_arns = []
        for t in tables:
            # resolve the original database name
            original_db = subscription.get(DATABASE_NAME).replace(f"-{self._data_producer_account_id}", "")

            # get the catalog definition of this table including if its a regex subscription
            all_tables = self._producer_automator.load_glue_tables(
                catalog_id=self._data_producer_account_id,
                source_db_name=original_db,
                table_name_regex=t,
                load_lf_tags=False
            )

            for resolved_table in all_tables:
                table_name = resolved_table.get('Name')
                # get the data location for the table
                table_s3_path = resolved_table.get('StorageDescriptor').get('Location')

                # add a bucket policy entry allowing the consumer lakeformation service linked role to perform GetObject*
                table_bucket = table_s3_path.split("/")[2]
                self._producer_automator.add_bucket_policy_entry(
                    principal_account=subscription.get(SUBSCRIBER_PRINCIPAL),
                    access_path=table_bucket
                )

                # grant describe on the database
                self._mesh_automator.lf_grant_permissions(
                    data_mesh_account_id=self._data_mesh_account_id,
                    principal=subscription.get(SUBSCRIBER_PRINCIPAL),
                    database_name=subscription.get(DATABASE_NAME),
                    permissions=['DESCRIBE'],
                    grantable_permissions=None
                )

                # grant validated permissions to object
                self._mesh_automator.lf_grant_permissions(
                    data_mesh_account_id=self._data_mesh_account_id,
                    principal=subscription.get(SUBSCRIBER_PRINCIPAL),
                    database_name=subscription.get(DATABASE_NAME),
                    table_name=table_name,
                    permissions=set_permissions,
                    grantable_permissions=grantable_permissions
                )

                rs = utils.load_ram_shares(lf_client=data_mesh_lf_client,
                                           data_mesh_account_id=self._data_mesh_account_id,
                                           database_name=subscription.get(DATABASE_NAME), table_name=t,
                                           target_principal=subscription.get(SUBSCRIBER_PRINCIPAL))
                ram_shares.update(rs)

                # add the shared table arn to the list of ARNs
                table_arns.append(utils.get_table_arn(region_name=self._current_region,
                                                      catalog_id=self._data_mesh_account_id,
                                                      database_name=subscription.get(DATABASE_NAME),
                                                      table_name=table_name))

                self._logger.info("Subscription RAM Shares")
                self._logger.info(ram_shares)

            # apply a glue catalog resource policy allowing the consumer to access objects by tag
            self._add_principal_to_glue_resource_policy(
                database_name=subscription.get(DATABASE_NAME),
                tables=subscription.get(TABLE_ARNS),
                add_principal=subscription.get(SUBSCRIBER_PRINCIPAL)
            )

        # update the subscription to reflect the changes
        self._subscription_tracker.update_status(
            subscription_id=request_id, status=STATUS_ACTIVE,
            permitted_grants=grant_permissions, grantable_grants=grantable_permissions, notes=decision_notes,
            ram_shares=ram_shares, table_arns=table_arns
        )

    def _add_principal_to_glue_resource_policy(self, database_name: str, tables: list, add_principal: str):
        self._mesh_automator.update_glue_catalog_resource_policy(
            region=self._current_region,
            database_name=database_name,
            tables=tables,
            producer_account_id=self._data_mesh_account_id,
            consumer_account_id=add_principal
        )

    def deny_access_request(self, request_id: str,
                            decision_notes: str = None):
        '''
        API to close an access request as denied. The reason for the denial should be included in decision_notes.
        :param request_id:
        :param decision_notes:
        :return:
        '''
        return self._subscription_tracker.update_status(
            subscription_id=request_id, status=STATUS_DENIED,
            notes=decision_notes
        )

    def update_subscription_permissions(self, subscription_id: str, grant_permissions: list, notes: str,
                                        grantable_permissions: list = None):
        '''
        Update the permissions on a subscription
        :param subscription_id:
        :param grant_permissions:
        :param notes:
        :return:
        '''
        subscription = self._subscription_tracker.get_subscription(subscription_id)

        current_permissions = subscription.get(PERMITTED_GRANTS)
        current_grantable_permissions = subscription.get(GRANTABLE_GRANTS)
        if current_grantable_permissions is None:
            current_grantable_permissions = []

        # calculate the permissions to be added
        perms_to_add = list(set(grant_permissions) - set(current_permissions))
        grantable_perms_to_add = []
        if len(grantable_permissions or '') > 0:
            grantable_perms_to_add = list(set(grantable_permissions) - set(current_grantable_permissions))

        # cant add grantable permissions without granting them first
        if len(perms_to_add or '') == 0 and len(grantable_perms_to_add or '') > 0:
            perms_to_add = grantable_perms_to_add

        permissions_modified = 0
        if len(perms_to_add or '') > 0:
            permissions_modified += self._mesh_automator.lf_batch_grant_permissions(
                data_mesh_account_id=self._data_mesh_account_id,
                target_account_id=subscription.get(SUBSCRIBER_PRINCIPAL),
                database_name=subscription.get(DATABASE_NAME),
                table_list=subscription.get(TABLE_NAME),
                permissions=perms_to_add,
                grantable_permissions=grantable_perms_to_add
            )

        # modify the current permissions to reflect the state of the addition
        current_permissions.extend(perms_to_add)
        current_grantable_permissions.extend(grantable_perms_to_add)

        # calculate the permissions to be removed
        perms_to_remove = list(set(current_permissions) - set(grant_permissions))

        if grantable_permissions is None:
            grantable_permissions = []

        grantable_perms_to_remove = list(set(current_grantable_permissions) - set(grantable_permissions))

        # revoke permissions at the lakeformation level
        if len(perms_to_remove or '') > 0:
            permissions_modified += self._mesh_automator.lf_batch_revoke_permissions(
                data_mesh_account_id=self._data_mesh_account_id,
                consumer_account_id=subscription.get(SUBSCRIBER_PRINCIPAL),
                database_name=subscription.get(DATABASE_NAME),
                table_list=subscription.get(TABLE_NAME),
                permissions=perms_to_remove,
                grantable_permissions=grantable_perms_to_remove
            )

        self._subscription_tracker.update_grants(
            subscription_id=subscription_id,
            permitted_grants=grant_permissions,
            grantable_grants=grantable_permissions,
            notes=notes
        )

        self._logger.info(f"Modified {permissions_modified} Permissions")

    def get_subscription(self, request_id: str) -> dict:
        return self._subscription_tracker.get_subscription(subscription_id=request_id)

    def delete_subscription(self, subscription_id: str, reason: str):
        '''
        Soft delete a subscription
        :param subscription_id:
        :param reason:
        :return:
        '''
        subscription = self.get_subscription(request_id=subscription_id)

        if subscription is None:
            raise Exception("No Subscription Found")
        else:
            lf_client = self._data_mesh_session.client('lakeformation')

            entries = []
            # generate the list of permissions at table and column level
            for t in subscription.get(TABLE_NAME):
                entries.extend(self._mesh_automator.create_lf_permissions_entry(
                    data_mesh_account_id=self._data_mesh_account_id,
                    target_account_id=subscription.get(SUBSCRIBER_PRINCIPAL),
                    database_name=subscription.get(DATABASE_NAME),
                    table_name=t,
                    permissions=subscription.get(PERMITTED_GRANTS),
                    grantable_permissions=subscription.get(GRANTABLE_GRANTS),
                    target_batch=True
                ))

            # add the database DESCRIBE grant
            entries.extend(self._mesh_automator.create_lf_permissions_entry(
                data_mesh_account_id=self._data_mesh_account_id,
                target_account_id=subscription.get(SUBSCRIBER_PRINCIPAL),
                database_name=subscription.get(DATABASE_NAME),
                permissions=['DESCRIBE'],
                grantable_permissions=['DESCRIBE'],
                target_batch=True
            ))

            lf_client.batch_revoke_permissions(
                Entries=entries
            )

            return self._subscription_tracker.delete_subscription(subscription_id=subscription_id, reason=reason)
