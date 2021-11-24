import logging
import warnings
import sys
import os
import inspect

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))))
sys.path.insert(0, parent_dir)

import example_utils
from src.data_mesh_util.lib.constants import *
from src.data_mesh_util import DataMeshProducer as dmp
from src.data_mesh_util.lib.SubscriberTracker import *

warnings.filterwarnings(action="ignore", message="unclosed", category=ResourceWarning)


class List():
    '''
    Class to test the functionality of a data producer. Should be run using credentials for a principal who can assume
    the DataMeshAdminProducer role in the data mesh. Requires environment variables:

    AWS_REGION
    AWS_ACCESS_KEY_ID
    AWS_SECRET_ACCESS_KEY
    AWS_SESSION_TOKEN (Optional)
    '''
    _region, _clients, _account_ids, _creds = example_utils.load_client_info_from_file()

    _mgr = dmp.DataMeshProducer(data_mesh_account_id=_account_ids.get(MESH),
                                log_level=logging.DEBUG,
                                region_name=_region,
                                use_credentials=_creds.get(PRODUCER))

    def setUp(self) -> None:
        warnings.filterwarnings("ignore", category=ResourceWarning)

    def list_pending_requests(self):
        # approve access from the producer
        return self._mgr.list_pending_access_requests()


if __name__ == "__main__":
    print(List().list_pending_requests())
