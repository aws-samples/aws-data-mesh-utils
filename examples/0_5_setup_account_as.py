import warnings
import logging
import sys
import os
import inspect
import argparse

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))))
sys.path.insert(0, parent_dir)

import data_mesh_util.lib.utils as utils
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
    _logger = logging.getLogger("DataMeshAdmin")
    _region, _clients, _account_ids, _creds = utils.load_client_info_from_file()

    def setUp(self) -> None:
        warnings.filterwarnings("ignore", category=ResourceWarning)

    def setup_account_as(self, account_id: str, role: str, use_credentials: dict, crawler_role_arn: str = None):
        # connect to the mesh account
        mesh_admin = dmu.DataMeshAdmin(data_mesh_account_id=self._account_ids.get(MESH), region_name=self._region,
                                       log_level=logging.DEBUG, use_credentials=self._creds.get(MESH))

        # setup the account as a producer
        if role in [PRODUCER, BOTH]:
            producer_admin = dmu.DataMeshAdmin(data_mesh_account_id=self._account_ids.get(MESH),
                                               region_name=self._region,
                                               log_level=logging.DEBUG, use_credentials=use_credentials)
            producer_admin.initialize_producer_account(crawler_role_arn=crawler_role_arn)
            mesh_admin.enable_account_as_producer(account_id)

        # set the account as a consumer
        if role in [CONSUMER, BOTH]:
            consumer_admin = dmu.DataMeshAdmin(data_mesh_account_id=self._account_ids.get(MESH),
                                               region_name=self._region,
                                               log_level=logging.DEBUG, use_credentials=use_credentials)
            consumer_admin.initialize_consumer_account()
            mesh_admin.enable_account_as_consumer(account_id)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--account_id', dest='account_id', required=True)
    parser.add_argument('--use_role', dest='use_role', required=True, choices=[CONSUMER, PRODUCER, BOTH])
    parser.add_argument('--crawler_role_arn', dest='crawler_role_arn', required=False)
    args = parser.parse_args()

    # generate a session from the environment
    current_session = utils.create_session(credentials=None, region=None)

    Step0_5().setup_account_as(account_id=args.account_id, role=args.use_role,
                               use_credentials=current_session.get_credentials(),
                               crawler_role_arn=args.crawler_role_arn)
