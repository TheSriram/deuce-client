#!/usr/bin/python3
"""
Ben's Deuce Testing Client
"""
from __future__ import print_function
import argparse
import json
import logging
import pprint
import sys

import deuceclient.deuce


class ProgramArgumentError(ValueError):
    pass


def __api_operation_prep(log, arguments):
    """
    API Operation Common Functionality
    """
    # Parse the user data
    example_user_config_json = "{\n    'user': <username>," \
                               "\n    'apikey': <apikey>\n}\n"
    try:
        user_data = json.load(arguments.user_config)
        username = user_data['user']
        apikey = user_data['apikey']
    except ValueError:
        sys.stderr.write('Invalid User Config file. Format:\n{0:}'.format(
            example_user_config_json))
        sys.exit(-3)
    except KeyError:
        sys.stderr.write('Invalid User Config file. Format:\n{0:}'.format(
            example_user_config_json))
        sys.exit(-2)

    # Setup the Authentication
    datacenter = arguments.datacenter
    auth_engine = rcbu.client.auth.Authentication(user_data['user'],
                                                  user_data['apikey'],
                                                  usertype='user',
                                                  datacenter=datacenter)
    uri = arguments.url

    # Setup Agent Access
    deuce = rcbu.client.deuce.DeuceClient(False, auth_engine, uri)

    return (auth_engine, deuce, uri)


def vault_create(log, arguments):
    """
    Create a vault with the given name
    """
    auth_engine, deuceclient, api_url = __api_operation_prep(log, arguments)

    deuceclient.CreateVault(arguments.vault_name)


def vault_exists(log, arguments):
    """
    Determine whether or not a vault of the given name exists
    """
    auth_engine, deuceclient, api_url = __api_operation_prep(log, arguments)

    result = deuceclient.VaultExists(arguments.vault_name)
    if result:
        print('Vault {0:} exists'.format(arguments.vault_name))
    else:
        print('Vault {0:} does NOT exist'.format(arguments.vault_name))


def vault_stats(log, arguments):
    """
    Get the Stats for the vault with the given name
    """
    auth_engine, deuceclient, api_url = __api_operation_prep(log, arguments)

    stats = deuceclient.GetVaultStatistics(arguments.vault_name)
    for k in stats.keys():
        print('{0:} = {1:}'.format(k, stats[k]))


def vault_delete(log, arguments):
    """
    Delete the vault with the given name
    """
    auth_engine, deuceclient, api_url = __api_operation_prep(log, arguments)

    deuceclient.DeleteVault(arguments.vault_name)


def block_list(log, arguments):
    """
    List the blocks in a vault
    """
    auth_engine, deuceclient, api_url = __api_operation_prep(log, arguments)

    block_list = deuceclient.GetBlockList(arguments.vault_name,
                                          marker=arguments.marker,
                                          limit=arguments.limit)
    print('Block List:')
    pprint.pprint(block_list)


def block_upload(log, arguments):
    """
    Upload blocks to a vault
    """
    auth_engine, deuceclient, api_url = __api_operation_prep(log, arguments)

    block_list = deuceclient.UploadBlock(arguments.vault_name,
                                         arguments.blockid,
                                         arguments.blockcontent)
    print('Block List:')
    pprint.pprint(block_list)


def file_create(log, arguments):
    """
    Creates a file
    """
    auth_engine, deuceclient, api_url = __api_operation_prep(log, arguments)

    location = deuceclient.CreateFile(arguments.vault_name)
    print ("Location where file is create {0:}".format(location))


def file_assign_blocks(log, arguments):
    """
    Assign blocks to a file
    NEED to check the way value is passed in the command line,
    right now it does not accept it.
    """
    auth_engine, deuceclient, api_url = __api_operation_prep(log, arguments)

    result = deuceclient.AssignBlocksToFile(arguments.vaultname,
                                            arguments.fileid,
                                            arguments.value)
    print(result)


def main():
    arg_parser = argparse.ArgumentParser(
        description="Cloud Backup Agent Status")
    arg_parser.add_argument('--user-config',
                            default=None,
                            type=argparse.FileType('r'),
                            required=True,
                            help='JSON file containing username and API Key')
    arg_parser.add_argument('--url',
                            default='127.0.0.1:8080',
                            type=str,
                            required=False,
                            help="Network Address for the Deuce Server."
                                 " Default: 127.0.0.1:8080")
    arg_parser.add_argument('-lg', '--log-config',
                            default=None,
                            type=str,
                            dest='logconfig',
                            help='log configuration file')
    arg_parser.add_argument('-dc', '--datacenter',
                            default='ord',
                            type=str,
                            dest='datacenter',
                            required=True,
                            help='Datacenter the system is in',
                            choices=['lon', 'syd', 'hkg', 'ord', 'iad', 'dfw'])
    sub_argument_parser = arg_parser.add_subparsers(title='subcommands')

    vault_parser = sub_argument_parser.add_parser('vault')
    vault_parser.add_argument('--vault-name',
                              default=None,
                              required=True,
                              help="Vault Name")
    vault_subparsers = vault_parser.add_subparsers(title='operations',
                                                   help='Vault Operations')

    vault_create_parser = vault_subparsers.add_parser('create')
    vault_create_parser.set_defaults(func=vault_create)

    vault_exists_parser = vault_subparsers.add_parser('exists')
    vault_exists_parser.set_defaults(func=vault_exists)

    vault_stats_parser = vault_subparsers.add_parser('stats')
    vault_stats_parser.set_defaults(func=vault_stats)

    vault_delete_parser = vault_subparsers.add_parser('delete')
    vault_delete_parser.set_defaults(func=vault_delete)

    block_parser = sub_argument_parser.add_parser('blocks')
    block_parser.add_argument('--vault-name',
                              default=None,
                              required=True,
                              help="Vault Name")
    block_subparsers = block_parser.add_subparsers(title='operations',
                                                   help='Block Operations')

    block_list_parser = block_subparsers.add_parser('list')
    block_list_parser.add_argument('--marker',
                                   default=None,
                                   required=False,
                                   type=str,
                                   help="Marker for retrieving partial "
                                        "contents. Unspecified means "
                                        "return everything.")
    block_list_parser.add_argument('--limit',
                                   default=None,
                                   required=False,
                                   type=int,
                                   help="Number of entries to return at most")
    block_list_parser.set_defaults(func=block_list)

    block_upload_parser = block_subparsers.add_parser('upload')
    block_upload_parser.add_argument('--blockid',
                                     default=None,
                                     required=True,
                                     type=str,
                                     help="sha1 of the block to be uploaded.")
    block_upload_parser.add_argument('--blockcontent',
                                     default=None,
                                     required=True,
                                     type=argparse.FileType('r'),
                                     help="The block to be uploaded")
    block_upload_parser.set_defaults(func=block_upload)

    file_parser = sub_argument_parser.add_parser('files')
    file_parser.add_argument('--vault-name',
                             default=None,
                             required=True,
                             help="Vault Name")
    file_subparsers = file_parser.add_subparsers(title='operations',
                                                 help='File Operations')

    file_create_parser = file_subparsers.add_parser('create')
    file_create_parser.set_defaults(func=file_create)

    file_assign_parser = file_subparsers.add_parser('assign_data')
    file_assign_parser.add_argument('--fileid',
                                    default=None,
                                    required=True,
                                    type=str,
                                    help="File to assign it to.")
    file_assign_parser.add_argument('--value',
                                    default=None,
                                    required=True,
                                    type=dict,
                                    help="Value passed in as a dict "
                                         "with offset and size")
    file_assign_parser.set_defaults(func=file_assign_blocks)
    arguments = arg_parser.parse_args()

    # If the caller provides a log configuration then use it
    # Otherwise we'll add our own little configuration as a default
    # That captures stdout and outputs to output/integration-slave-server.out
    if arguments.logconfig is not None:
        logging.config.fileConfig(arguments.logconfig)
    else:
        lf = logging.FileHandler('.deuce_client-py.log')
        lf.setLevel(logging.DEBUG)

        log = logging.getLogger()
        log.addHandler(lf)
        log.setLevel(logging.DEBUG)

    # Build the logger
    log = logging.getLogger()

    arguments.func(log, arguments)


if __name__ == "__main__":
    sys.exit(main())
