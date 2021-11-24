import warnings
import logging
import sys
import os
import inspect
import argparse

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))))
sys.path.insert(0, parent_dir)

import example_utils
from data_mesh_util.lib.constants import *
from data_mesh_util import DataMeshAdmin as data_mesh_admin
from data_mesh_util import DataMeshMacros as data_mesh_macros

warnings.filterwarnings(action="ignore", message="unclosed", category=ResourceWarning)


class Step0():
    '''
    Script to configure an set of accounts as central data mesh, producer, and consumer. Mesh credentials must already
    have DataLakeAdmin permissions.
    '''
    _logger = logging.getLogger("DataMeshAdmin")
    _region, _clients, _account_ids, _creds = example_utils.load_client_info_from_file()

    def setUp(self) -> None:
        warnings.filterwarnings("ignore", category=ResourceWarning)

    def setup_central_account(self, producer_crawler_role_arn: str = None, consumer_crawler_role_arn: str = None):
        # create the data mesh
        mesh_admin = data_mesh_admin.DataMeshAdmin(data_mesh_account_id=self._account_ids.get(MESH),
                                                   region_name=self._region,
                                                   log_level=logging.DEBUG, use_creds=self._creds.get(MESH))
        mesh_admin.initialize_mesh_account()

        # create a macro handler which works across accounts
        mesh_macros = data_mesh_macros.DataMeshMacros(data_mesh_account_id=self._account_ids.get(MESH),
                                                      region_name=self._region,
                                                      log_level=logging.DEBUG)

        # create the producer account
        mesh_macros.bootstrap_account(account_type=PRODUCER,
                                      mesh_credentials=self._creds.get(MESH),
                                      account_credentials=self._creds.get(PRODUCER_ADMIN),
                                      crawler_role_arn=producer_crawler_role_arn)

        # create the consumer_account
        mesh_macros.bootstrap_account(account_type=CONSUMER,
                                      mesh_credentials=self._creds.get(MESH),
                                      account_credentials=self._creds.get(CONSUMER_ADMIN),
                                      crawler_role_arn=consumer_crawler_role_arn)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--producer_crawler_role_arn', dest='producer_crawler_role_arn', required=False)
    parser.add_argument('--consumer_crawler_role_arn', dest='consumer_crawler_role_arn', required=False)
    args = parser.parse_args()

    Step0().setup_central_account(producer_crawler_role_arn=args.producer_crawler_role_arn,
                                  consumer_crawler_role_arn=args.consumer_crawler_role_arn)
