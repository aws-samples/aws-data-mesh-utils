import data_mesh_util.lib.utils as utils
from data_mesh_util.lib.constants import *


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
