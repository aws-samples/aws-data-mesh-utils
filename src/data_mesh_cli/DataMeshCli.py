import os
import sys
import json
import argparse
import inspect
from inspect import FullArgSpec

current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, current_dir)
sys.path.insert(1, parent_dir)

import data_mesh_util.lib.utils as utils
from data_mesh_util.DataMeshAdmin import DataMeshAdmin
from data_mesh_util.DataMeshProducer import DataMeshProducer
from data_mesh_util.DataMeshConsumer import DataMeshConsumer
from data_mesh_util.DataMeshMacros import DataMeshMacros


def _usage(message: str, all_commands: dict = None) -> None:
    print(message)
    print()
    print("aws-data-mesh <command> <args>")
    print(
        "   <command> - The command to perform, such as 'create-data-product', or 'request-access'. The command automatically infers the credential context to use.")
    print("   <args> - Arguments for the command, using '--<parameter> <value>' syntax")
    if all_commands is not None:
        print()
        print("   Valid Commands:")
        for key in all_commands.keys():
            print(f"      {key}")
    sys.exit(-1)


def _get_command(command: str) -> dict:
    '''
    Load the command provided from the command_mappings.json file, indicating context, required, and optional args
    :param command:
    :return:
    '''
    all_commands = None
    with open(f'{current_dir}/command_mappings.json', 'r') as commands:
        all_commands = json.load(commands)

    this_command = all_commands.get(command)

    if this_command is None:
        _usage(f"Command \"{command}\" Invalid", all_commands)
    else:
        return this_command


def _load_constructor_args(args):
    constructor_args = {
        "data_mesh_account_id": args.data_mesh_account_id
    }
    if args.region_name is not None:
        constructor_args["region_name"] = args.region_name
    if args.log_level is not None:
        constructor_args["log_level"] = args.log_level
    else:
        constructor_args["log_level"] = 'INFO'

    return constructor_args


def _extract_reqopt_params(args_spec: FullArgSpec) -> tuple:
    # count how many required parameters there are
    required_count = len(args_spec.args) - len(args_spec.defaults)

    required_args = []
    opt_args = []
    if len(args_spec.args) > 1:
        for i in range(1, required_count):
            required_args.append(args_spec.args[i])

    if len(args_spec.defaults) > 0:
        for j in range(len(args_spec.defaults) + 1, 1, -1):
            opt_args.append(args_spec.args[j])

    return required_args, opt_args


class DataMeshCli:
    _caller_name = "DataMeshCli"

    def __init__(self, caller_name: str = None):
        if caller_name is not None:
            self._caller_name = caller_name

    def run(self):
        if len(sys.argv) == 1:
            _usage("No Valid Arguments Supplied")

        # resolve the supplied command
        command_name = sys.argv[1]
        command_data = _get_command(command_name)

        # load the command context
        context = command_data.get('Context')

        # create an argument parser with the caller name listed so we get a nice usage string
        parser = argparse.ArgumentParser(prog=self._caller_name)

        # add constructor args for callable classes
        parser.add_argument("--data_mesh_account_id", dest="data_mesh_account_id", required=True)
        parser.add_argument("--region_name", dest="region_name", required=False)
        parser.add_argument("--log_level", dest="log_level", required=False)
        parser.add_argument("--use_credentials", dest="use_credentials", required=False)
        parser.add_argument("--credentials_file", dest="credentials_file", required=False)

        constructor_params, _ = parser.parse_known_args(args=sys.argv[2:])

        # load the target class for the provide context
        cls = None
        constructor_args = _load_constructor_args(constructor_params)

        # load credentials from args or from credentials file
        if constructor_params.use_credentials is not None:
            constructor_args['use_credentials'] = constructor_params.use_credentials

        credentialset = None
        if constructor_params.credentials_file is not None:
            region, clients, account_ids, credentials_dict = utils.load_client_info_from_file(
                constructor_params.credentials_file)
            constructor_args['region_name'] = region
            constructor_args['use_credentials'] = credentials_dict.get(context)

        if context == 'Producer':
            cls = DataMeshProducer(**constructor_args)
        elif context == 'Consumer':
            cls = DataMeshConsumer(**constructor_args)
        elif context == 'Mesh':
            cls = DataMeshAdmin(**constructor_args)
        else:
            cls = DataMeshMacros(**constructor_args)

        # load the class method to be invoked
        method_name = command_data.get("Method")
        method = getattr(cls, method_name)

        # load the required an optional arguments from the function signature
        method_params = inspect.getfullargspec(method)
        required_params, optional_params = _extract_reqopt_params(method_params)

        def _add_args(params: list, required: bool) -> None:
            for name in params:
                parser.add_argument(f"--{name}", dest=name, required=required)

        # reset the parser so we can extract just the function args
        parser = argparse.ArgumentParser(prog=self._caller_name)
        _add_args(required_params, True)
        _add_args(optional_params, False)

        args, _ = parser.parse_known_args(args=sys.argv[2:])

        # generate a dict from the required and optional args, and their values
        method_args = vars(args)

        # call the class method using keyword args
        try:
            method(**method_args)
            sys.exit(0)
        except Exception as e:
            print(e)
            sys.exit(-1)


if __name__ == '__main__':
    cli = DataMeshCli()
    cli.run()
