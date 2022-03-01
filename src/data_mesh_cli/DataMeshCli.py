import os
import sys
import json
import argparse
import inspect
from inspect import FullArgSpec
import pprint

current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, current_dir)
sys.path.insert(1, parent_dir)

import data_mesh_util.lib.utils as utils
from data_mesh_util.lib.constants import *
from data_mesh_util.DataMeshAdmin import DataMeshAdmin
from data_mesh_util.DataMeshProducer import DataMeshProducer
from data_mesh_util.DataMeshConsumer import DataMeshConsumer
from data_mesh_util.DataMeshMacros import DataMeshMacros

USAGE_STATUS = 126

CONTEXT_MAPPING = {
    'Producer': DataMeshProducer,
    'Consumer': DataMeshConsumer,
    'Mesh': DataMeshAdmin,
    'Macro': DataMeshMacros
}

_printer = pprint.PrettyPrinter(indent=4)


def _cli_usage(message: str = None, all_commands: dict = None) -> None:
    '''
    Method to print usage information for the cli to standard out
    :param message:
    :param all_commands:
    :return:
    '''
    if message is not None:
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
    sys.exit(USAGE_STATUS)


def _command_usage(caller_name: str, command_name: str, method_name: str, cls) -> None:
    '''
    Method to print usage information for a single command to standard out

    :param command_name:
    :param method_name:
    :param cls:
    :return:
    '''
    print(f"{caller_name} {command_name} <args>")

    # get the required, optional and defaults for the class
    required, optional, defaults_map = _get_req_opt_constructor_args(cls)

    # add the implicit support for credentials file
    optional.append("credentials_file")

    # load the function args
    method = inspect.getattr_static(cls, method_name)
    method_args = inspect.getfullargspec(method)
    x, y, _ = _extract_req_opt_params(method_args)
    required.extend(x)
    optional.extend(y)

    print("   Required Arguments:")
    [print(f"      * {arg}") for arg in required]

    print("   Optional Arguments:")
    for arg in optional:
        if defaults_map.get(arg) is not None:
            print(f"      * {arg} - default '{defaults_map.get(arg)}'")
        else:
            print(f"      * {arg}")

    sys.exit(USAGE_STATUS)


def _build_constructor_arg_dict(context, args):
    '''
    Generate a dict of class constructor arguments and values from the provided Namespace
    :param context:
    :param args:
    :return:
    '''
    constructor_args = {
        "data_mesh_account_id": args.data_mesh_account_id
    }
    if args.region_name is not None:
        constructor_args["region_name"] = args.region_name
    else:
        if 'AWS_REGION' not in os.getenv():
            print("Using default region 'us-east-1'")
        else:
            constructor_args["region_name"] = os.getenv('AWS_REGION')

    if args.log_level is not None:
        constructor_args["log_level"] = args.log_level
    else:
        constructor_args["log_level"] = 'INFO'

    # load credentials from args or from credentials file
    if 'use_credentials' in args and args.use_credentials is not None:
        constructor_args['use_credentials'] = json.loads(args.use_credentials)

    if "credentials_file" in args and args.credentials_file is not None:
        region, clients, account_ids, credentials_dict = utils.load_client_info_from_file(
            args.credentials_file)
        # use the region from the credentials file instead of env
        constructor_args['region_name'] = region
        if context in credentials_dict:
            constructor_args['use_credentials'] = credentials_dict.get(context)

    return constructor_args


def _extract_req_opt_params(args_spec: FullArgSpec) -> tuple:
    '''
    Method to convert an inspect.FullArgSpec object into a list of required parameters, a list of optional parameters,
    and a dict to map parameter name to default value
    :param args_spec:
    :return:
    '''
    required_args = []
    opt_args = []
    defaults_map = {}

    if len(args_spec.args) > 1:
        # count how many required parameters there are
        required_count = 0
        if args_spec.args is not None and args_spec.defaults is not None:
            required_count = len(args_spec.args) - len(args_spec.defaults)
        else:
            if args_spec.args is None:
                required_count = 0
            else:
                required_count = len(args_spec.args)

        if args_spec.args is not None and len(args_spec.args) > 1:
            # add all required arguments that don't have defaults
            for i in range(1, required_count):
                required_args.append(args_spec.args[i])

        # add all default args working from back to front
        if args_spec.defaults is not None and len(args_spec.defaults) > 0:
            for j in range(0, len(args_spec.defaults)):
                opt_args.append(args_spec.args[len(args_spec.args) - j - 1])

        # build a defaults map
        defaults_map = _convert_argspec_to_default_mapping(args_spec)

    return required_args, opt_args, defaults_map


def _add_constructor_args(cls, parser) -> None:
    '''
    Method which adds the required and optional arguments from a class constructor to a parser
    :param cls:
    :param parser:
    :return:
    '''

    def add(key: str, req: bool) -> None:
        parser.add_argument(f"--{key}", dest=key, required=req)

    # get the constructor args for the class
    required, optional, _ = _get_req_opt_constructor_args(cls)

    [add(arg, True) for arg in required]
    [add(arg, False) for arg in optional]

    # add the "special" constructor arguments that will be extracted before use
    add("credentials_file", False)


def _convert_argspec_to_default_mapping(arg_spec: FullArgSpec) -> dict:
    '''
    Converts an inspect.FullArgSpec to a dict that maps a parameter name to its default value
    :param arg_spec:
    :return:
    '''

    # arg specs are end-first indexed with each other, so operate on reversed parameters and defaults
    r_args = arg_spec.args.copy()
    r_args.reverse()
    r_defs = []
    if arg_spec.defaults is not None:
        r_defs = list(arg_spec.defaults)
        r_defs.reverse()
    arg_list = []
    for i, arg in enumerate(r_args):
        if arg != 'self':
            default_value = None
            if len(r_defs) - 1 >= i:
                default_value = r_defs[i]

            arg_list.append((arg, default_value))

    arg_list.reverse()
    return dict(arg_list)


def _get_req_opt_constructor_args(cls) -> tuple:
    '''
    Statically inspect a class's constructor method and return the required, optional, and default value mapping arguments
    :param cls:
    :return:
    '''
    constructor = inspect.getattr_static(cls, "__init__")
    constructor_args = inspect.getfullargspec(constructor)
    return _extract_req_opt_params(constructor_args)


class DataMeshCli:
    _caller_name = "DataMeshCli"
    _all_commands = None
    _region = None
    _creds = None
    _account_ids = None

    def __init__(self, caller_name: str = None):
        if caller_name is not None:
            self._caller_name = caller_name

        # load the set of valid commands from the filesystem
        with open(f'{current_dir}/command_mappings.json', 'r') as commands:
            self._all_commands = json.load(commands)

    def _get_command(self, command) -> dict:
        '''
        Method to fetch a command, or display usage information

        :param command:
        :return:
        '''
        this_command = self._all_commands.get(command)

        if this_command is None:
            _cli_usage(f"Command \"{command}\" Invalid", self._all_commands)
        else:
            return this_command

    def _load_creds_data(self, from_filename: str) -> None:
        '''
        Method to pack sys.argv with values from a credentials file, so we can 'spoof' that they have been provided by
        the caller
        :param from_filename:
        :return:
        '''
        self._region, x, self._account_ids, self._creds = utils.load_client_info_from_file(
            from_filename)

        utils.load_sysarg("region_name", self._region)
        if MESH in self._account_ids:
            utils.load_sysarg("data_mesh_account_id", self._account_ids.get(MESH))

    def run(self):
        '''
        Main runner method for the CLI class. All parameters are harvested from sys.argv
        :return:
        '''
        if len(sys.argv) == 1:
            _cli_usage("No Valid Arguments Supplied")

        # fix sys.argv values which might contain rubbish
        utils.purify_sysargs()

        # create an argument parser with the caller name listed so we get a nice usage string
        parser = argparse.ArgumentParser(prog=self._caller_name)

        # add command line arguments to grab credentials information at first
        parser.add_argument("--data_mesh_account_id", required=False)
        parser.add_argument("--credentials_file", required=False)
        parser.add_argument("--use_credentials", required=False)
        cred_args, _ = parser.parse_known_args(args=sys.argv[2:])

        # resolve the supplied command
        command_name = sys.argv[1]
        if sys.argv[1] == 'help':
            _cli_usage(all_commands=self._all_commands)
        else:
            command_data = self._get_command(command_name)

        # load the command context
        context = command_data.get('Context')

        if context == 'Macro' and cred_args.credentials_file is None:
            raise Exception("This method requires the use of a credentials_file")

        # special handler for cases where the credentials file is supplied, which allows us to extract many of the required arguments
        if cred_args.credentials_file is not None and cred_args.data_mesh_account_id is None:
            self._load_creds_data(cred_args.credentials_file)

        if cred_args.credentials_file is None and cred_args.use_credentials is None:
            print("Will load credentials from boto environment")

        # lookup the class for the context
        cls = CONTEXT_MAPPING.get(context)

        if len(sys.argv) == 2 or (len(sys.argv) == 3 and sys.argv[2].lower() == 'help'):
            _command_usage(self._caller_name, command_name, command_data.get("Method"), cls)

        # reset parser and add constructor args for callable classes
        parser = argparse.ArgumentParser(prog=self._caller_name)
        _add_constructor_args(cls, parser)
        constructor_params, _ = parser.parse_known_args(args=sys.argv[2:])
        constructor_args = _build_constructor_arg_dict(context, constructor_params)

        # statically load the class method to be invoked and load the required and optional arguments from the function signature
        method_name = command_data.get("Method")
        method = inspect.getattr_static(cls, method_name)
        method_params = inspect.getfullargspec(method)
        required_params, optional_params, _ = _extract_req_opt_params(method_params)

        def _add_args(params: list, required: bool) -> None:
            for name in params:
                parser.add_argument(f"--{name}", dest=name, required=required)

        # reset the parser so we can extract just function args
        parser = argparse.ArgumentParser(prog=f"{self._caller_name} {command_name}")

        if command_name == 'enable-account':
            # macro functions require different handling of credentials, and must have been invoked with a credentials file
            parser.add_argument('--account_type', required=True)
            parser.add_argument('--crawler_role_arn', required=False)
            macro_args, _ = parser.parse_known_args(args=sys.argv[2:])

            if macro_args.account_type.capitalize() == PRODUCER:
                cred_name = PRODUCER_ADMIN
            elif macro_args.account_type.capitalize() == CONSUMER:
                cred_name = CONSUMER_ADMIN

            method_args = {
                'account_type': macro_args.account_type,
                'mesh_credentials': self._creds.get(MESH),
                'account_credentials': self._creds.get(cred_name),
                'crawler_role_arn': macro_args.crawler_role_arn
            }
        else:
            _add_args(required_params, True)
            _add_args(optional_params, False)
            args, _ = parser.parse_known_args(args=sys.argv[2:])

            # generate a dict from the required and optional args so we can use it for a keywords invocation
            method_args = vars(args)

        # call the class method using keyword args
        try:
            # load live versions of the class and method so they are callable
            class_object = cls(**constructor_args)
            method = getattr(class_object, method_name)
            response = method(**method_args)

            if response is not None:
                message = response
                if isinstance(response, bool):
                    message = "Success"

                _printer.pprint(message)

            sys.exit(0)
        except Exception as e:
            print(e)
        sys.exit(-1)


if __name__ == '__main__':
    DataMeshCli().run()
