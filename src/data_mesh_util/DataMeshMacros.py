from data_mesh_util.lib.constants import *
from data_mesh_util import DataMeshAdmin as data_mesh_admin


class DataMeshMacros:
    _data_mesh_account_id = None
    _region = None
    _log_level = None
    _BOTH = 'Both'

    def __init__(self, data_mesh_account_id: str, region_name: str, log_level: str):
        self._data_mesh_account_id = data_mesh_account_id
        self._region = region_name
        self._log_level = log_level

    def bootstrap_account(self, account_type: str, mesh_credentials, account_credentials, crawler_role_arn: str = None):
        # create a data mesh admin for the mesh account
        mesh_admin = data_mesh_admin.DataMeshAdmin(
            data_mesh_account_id=self._data_mesh_account_id,
            region_name=self._region,
            log_level=self._log_level,
            use_credentialss=mesh_credentials
        )

        # create a data mesh admin for the target account
        account_admin = data_mesh_admin.DataMeshAdmin(
            data_mesh_account_id=self._data_mesh_account_id,
            region_name=self._region,
            log_level=self._log_level,
            use_credentialss=account_credentials
        )

        if account_type == PRODUCER or account_type.lower() == self._BOTH.lower():
            account_admin.initialize_producer_account()
            mesh_admin.enable_account_as_producer(account_id=account_credentials.get('AccountId'))
        elif account_type == CONSUMER or account_type.lower() == self._BOTH.lower():
            account_admin.initialize_consumer_account()
            mesh_admin.enable_account_as_consumer(account_id=account_credentials.get('AccountId'))
