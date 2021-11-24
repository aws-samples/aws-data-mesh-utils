import argparse
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
import logging

warnings.filterwarnings(action="ignore", message="unclosed", category=ResourceWarning)


class Step1():
    '''
    Create a data product. Should be run using credentials for a principal who can assume
    the DataMeshAdminProducer role in the data mesh.
    '''
    _region, _clients, _account_ids, _creds = example_utils.load_client_info_from_file()

    _mgr = dmp.DataMeshProducer(data_mesh_account_id=_account_ids.get(MESH),
                                log_level=logging.DEBUG,
                                region_name=_region,
                                use_credentials=_creds.get(PRODUCER))
    _subscription_tracker = SubscriberTracker(data_mesh_account_id=_account_ids.get(MESH),
                                              credentials=_creds.get(MESH),
                                              region_name=_region,
                                              log_level=logging.DEBUG)

    def setUp(self) -> None:
        warnings.filterwarnings("ignore", category=ResourceWarning)

    def create_data_product(self, database_name: str, table_regex: str, domain: str, data_product_name: str,
                            cron_expr: str,
                            crawler_role: str):
        return self._mgr.create_data_products(
            source_database_name=database_name,
            table_name_regex=table_regex,
            domain=domain,
            data_product_name=data_product_name,
            create_public_metadata=True,
            sync_mesh_catalog_schedule=cron_expr,
            sync_mesh_crawler_role_arn=crawler_role,
            expose_data_mesh_db_name=None,
            expose_table_references_with_suffix=None
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--database_name', dest='database_name', required=True)
    parser.add_argument('--table_regex', dest='table_regex', required=True)
    parser.add_argument('--domain', dest='domain', required=False)
    parser.add_argument('--data_product_name', dest='data_product_name', required=False)
    parser.add_argument('--cron_expr', dest='cron_expr', required=False)
    parser.add_argument('--crawler_role', dest='crawler_role', required=False)

    args = parser.parse_args()
    Step1().create_data_product(database_name=args.database_name, table_regex=args.table_regex,
                                domain=args.domain,
                                data_product_name=args.data_product_name, cron_expr=args.cron_expr,
                                crawler_role=args.crawler_role)
