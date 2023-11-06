from data_mesh_util.lib.constants import *
from data_mesh_util import DataMeshAdmin as data_mesh_admin
import data_mesh_util.lib.utils as utils


class DataMeshMacros:
    _data_mesh_account_id = None
    _region = None
    _log_level = None
    _BOTH = 'Both'

    def __init__(self, data_mesh_account_id: str, region_name: str = 'us-east-1', log_level: str = "INFO"):
        self._data_mesh_account_id = data_mesh_account_id
        self._region = region_name
        self._log_level = log_level

        if self._log_level == 'DEBUG':
            utils.log_instance_signature(self, self._logger)

    def bootstrap_account(self, account_type: str, mesh_credentials=None, account_credentials=None, crawler_role_arn: str = None, 
                          mesh_profile=None, account_profile=None):
        # create a data mesh admin for the mesh account
        mesh_admin = data_mesh_admin.DataMeshAdmin(
            data_mesh_account_id=self._data_mesh_account_id,
            region_name=self._region,
            log_level=self._log_level,
            use_credentials=mesh_credentials,
            use_profile=mesh_profile
        )

        # create a data mesh admin for the target account
        account_admin = data_mesh_admin.DataMeshAdmin(
            data_mesh_account_id=self._data_mesh_account_id,
            region_name=self._region,
            log_level=self._log_level,
            use_credentials=account_credentials,
            use_profile=account_profile
        )

        if account_type.lower() == PRODUCER.lower() or account_type.lower() == self._BOTH.lower():
            account_admin.initialize_producer_account()
            mesh_admin.enable_account_as_producer(account_id=account_admin._sts_client.get_caller_identity()["Account"])
        elif account_type.lower() == CONSUMER.lower() or account_type.lower() == self._BOTH.lower():
            account_admin.initialize_consumer_account()
            mesh_admin.enable_account_as_consumer(account_id=account_admin._sts_client.get_caller_identity()["Account"])
        else:
            raise Exception(f"Unknown Account Type {account_type}")
