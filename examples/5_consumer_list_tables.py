import argparse
import logging
import warnings
import sys
import os
import inspect

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))))
sys.path.insert(0, parent_dir)

import example_utils
from data_mesh_util.lib.constants import *
from data_mesh_util import DataMeshConsumer as dmc
import pprint

warnings.filterwarnings(action="ignore", message="unclosed", category=ResourceWarning)


class Step5():
    '''
    Consumer functionality to finalize a subscription request.
    '''
    _region, _clients, _account_ids, _creds = example_utils.load_client_info_from_file()

    _consumer = dmc.DataMeshConsumer(data_mesh_account_id=_account_ids.get(MESH),
                                     log_level=logging.DEBUG,
                                     region_name=_region,
                                     use_credentials=_creds.get(CONSUMER))

    def setUp(self) -> None:
        warnings.filterwarnings("ignore", category=ResourceWarning)

    def list_subscriptions_and_tables(self):
        # confirm that the consumer can see that it's status is now Active
        subscriptions = self._consumer.list_product_access()

        print('Subscriptions')
        print('-------------')
        for s in subscriptions.get('Subscriptions'):
            pprint.pprint(s)
            for t in s.get('TableName'):
                print('Table')
                print('-----')
                pprint.pprint(self._consumer.get_table_info(s.get('DatabaseName'), t))


if __name__ == "__main__":
    Step5().list_subscriptions_and_tables()
