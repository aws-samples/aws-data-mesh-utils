import warnings
import logging
import sys
import os
import inspect
import argparse

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))))
sys.path.insert(0, parent_dir)

from data_mesh_util.lib.constants import *
import data_mesh_util.lib.utils as utils
from data_mesh_util import DataMeshAdmin as dmu

warnings.filterwarnings(action="ignore", message="unclosed", category=ResourceWarning)

BOTH = 'both'


class Step0_5():
    '''
    Script to configure an set of accounts as central data mesh, producer, and consumer. Mesh credentials must already
    have DataLakeAdmin permissions.
    '''
    _logger = None
    _region = None
    _clients = None
    _account_ids = None
    _creds = None

    def __init__(self, credentials_file: str = None):
        self._logger = logging.getLogger("DataMeshAdmin")
        self._region, self._clients, self._account_ids, self._creds = utils.load_client_info_from_file(credentials_file)

    def setUp(self) -> None:
        warnings.filterwarnings("ignore", category=ResourceWarning)

    def setup_account_as(self, role: str, crawler_role_arn: str = None):
        # connect to the mesh account
        mesh_admin = dmu.DataMeshAdmin(data_mesh_account_id=self._account_ids.get(MESH), region_name=self._region,
                                       log_level=logging.DEBUG, use_credentials=self._creds.get(MESH))

        # setup the account as a producer
        if role in [PRODUCER, BOTH]:
            producer_admin = dmu.DataMeshAdmin(data_mesh_account_id=self._account_ids.get(MESH),
                                               region_name=self._region,
                                               log_level=logging.DEBUG, use_credentials=self._creds.get(PRODUCER_ADMIN))
            producer_admin.initialize_producer_account(crawler_role_arn=crawler_role_arn)
            mesh_admin.enable_account_as_producer(self._account_ids.get(PRODUCER_ADMIN))

        # set the account as a consumer
        if role in [CONSUMER, BOTH]:
            consumer_admin = dmu.DataMeshAdmin(data_mesh_account_id=self._account_ids.get(MESH),
                                               region_name=self._region,
                                               log_level=logging.DEBUG, use_credentials=self._creds.get(CONSUMER_ADMIN))
            consumer_admin.initialize_consumer_account()
            mesh_admin.enable_account_as_consumer(self._account_ids.get(CONSUMER_ADMIN))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--use_role', dest='use_role', required=True, choices=[CONSUMER, PRODUCER, BOTH])
    parser.add_argument('--crawler_role_arn', dest='crawler_role_arn', required=False)
    parser.add_argument('--credentials_file', dest='credentials_file', required=False)
    args = parser.parse_args()

    Step0_5(args.credentials_file).setup_account_as(
        role=args.use_role,
        crawler_role_arn=args.crawler_role_arn
    )
