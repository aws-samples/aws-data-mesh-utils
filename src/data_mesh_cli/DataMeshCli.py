import os
import sys
import json
import argparse
import inspect

current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
sys.path.insert(0, current_dir)


def usage(message: str, all_commands: dict = None) -> None:
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


def get_command(command: str) -> dict:
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
        usage(f"Command \"{command}\" Invalid", all_commands)
    else:
        return this_command


class DataMeshCli:
    _caller_name = "DataMeshCli"

    def __init__(self, caller_name: str = None):
        if caller_name is not None:
            self._caller_name = caller_name

    def run(self):
        if len(sys.argv) == 1:
            usage("No Valid Arguments Supplied")

        # resolve the supplied command
        command_name = sys.argv[1]
        command_data = get_command(command_name)
        context = command_data.get('Context')

        print(context)

        # add arguments to argparse for required and optional args
        parser = argparse.ArgumentParser(prog=self._caller_name)
        for required in command_data.get('RequiredArgs'):
            parser.add_argument(f"--{required}", dest=required, required=True)

        for optional in command_data.get('OptionalArgs'):
            parser.add_argument(f"--{optional}", dest=optional, required=False)

        args = parser.parse_args(args=sys.argv[2:])

        print(args)


if __name__ == '__main__':
    cli = DataMeshCli()
    cli.run()
