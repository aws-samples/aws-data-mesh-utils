import json
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), "../src"))
sys.path.append(os.path.join(os.path.dirname(__file__), "../src/resource"))
sys.path.append(os.path.join(os.path.dirname(__file__), "../src/lib"))

import data_mesh_util.lib.utils as utils
from data_mesh_util.lib.constants import *

def load_client_info_from_file(from_path: str = None):
    if from_path is None:
        use_path = os.getenv('CredentialsFile')
    else:
        use_path = from_path

    if use_path is None:
        raise Exception(
            "Unable to load Client Connection information without a file reference. Provide a direct path or set environment variable CredentialsFile")

    _creds = None
    with open(use_path, 'r') as w:
        _creds = json.load(w)
        w.close()

    _clients = {}
    _account_ids = {}
    _credentials_dict = {}
    _region = _creds.get('AWS_REGION')

    for token in [MESH, PRODUCER, CONSUMER, PRODUCER_ADMIN, CONSUMER_ADMIN]:
        _clients[token] = utils.generate_client('sts', region=_region, credentials=_creds.get(token))
        _account_ids[token] = _creds.get(token).get('AccountId')
        _credentials_dict = _creds

    return _region, _clients, _account_ids, _credentials_dict


def assume_source_role(sts_client, account_id, type: str):
    current_account = sts_client.get_caller_identity()
    session_name = utils.make_iam_session_name(current_account)
    if type == PRODUCER:
        return sts_client.assume_role(
            RoleArn=utils.get_role_arn(account_id, DATA_MESH_PRODUCER_ROLENAME),
            RoleSessionName=session_name)
    elif type == CONSUMER:
        return sts_client.assume_role(
            RoleArn=utils.get_role_arn(account_id, DATA_MESH_CONSUMER_ROLENAME),
            RoleSessionName=session_name)
