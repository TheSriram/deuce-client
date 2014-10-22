"""
Tests - Deuce Client - Support
"""
import datetime
import hashlib
import os
import random
import time
import uuid
from time import sleep as slowsleep

import deuceclient.auth.base


# NOTE(TheSriram): Let's monkey patch sleep to get tests
# to run faster
def fastsleep(seconds):
    speed_factor = 0.01
    slowsleep(seconds * speed_factor)

time.sleep = fastsleep


def create_vault_name():
    return 'vault_{0}'.format(str(uuid.uuid4()))


def create_project_name():
    return 'project_{0}'.format(str(uuid.uuid4()))


def get_base_path():
    return '/v1.0'


def get_vault_path(vault_name):
    return '{0}/vaults/{1}'.format(get_base_path(), vault_name)


def get_blocks_path(vault_name):
    return '{0}/blocks'.format(get_vault_path(vault_name))


def get_block_path(vault_name, block_id):
    return '{0}/{1}'.format(get_blocks_path(vault_name), block_id)


def get_storage_path(vault_name):
    return '{0}/storage'.format(get_vault_path(vault_name))


def get_storage_blocks_path(vault_name):
    return '{0}/blocks'.format(get_storage_path(vault_name))


def get_storage_block_path(vault_name, block_id):
    return '{0}/{1}'.format(get_storage_blocks_path(vault_name), block_id)


def get_files_path(vault_name):
    return '{0}/files'.format(get_vault_path(vault_name))


def get_file_path(vault_name, file_id):
    return '{0}/{1}'.format(get_files_path(vault_name), file_id)


def get_fileblocks_path(vault_name, file_id):
    return '{0}/blocks'.format(get_file_path(vault_name, file_id))


def get_fileblock_path(vault_name, file_id, block_id):
    return '{0}/{1}'.format(get_fileblocks_path(vault_name, file_id), block_id)


def get_deuce_url(apihost):
    return 'https://{0}{1}'.format(apihost, get_base_path())


def get_vault_url(apihost, vault):
    return 'https://{0}{1}'.format(apihost, get_vault_path(vault))


def get_blocks_url(apihost, vault):
    return 'https://{0}{1}'.format(apihost, get_blocks_path(vault))


def get_block_url(apihost, vault, block_id):
    return 'https://{0}{1}'.format(apihost, get_block_path(vault, block_id))


def get_files_url(apihost, vault):
    return 'https://{0}{1}'.format(apihost, get_files_path(vault))


def get_file_url(apihost, vault, file_id):
    return 'https://{0}{1}'.format(apihost, get_file_path(vault, file_id))


def get_file_blocks_url(apihost, vault, file_id):
    return 'https://{0}{1}'.format(apihost,
                                   get_fileblocks_path(vault, file_id))


def get_file_block_url(apihost, vault, file_id, block_id):
    return 'https://{0}{1}'.format(apihost,
                                   get_file_block_path(vault,
                                                       file_id,
                                                       block_id))


def get_block_id(data):
    blockid_generator = hashlib.sha1()
    blockid_generator.update(data)
    return blockid_generator.hexdigest()


def create_block(block_size=100):
    block_data = os.urandom(block_size)
    block_id = get_block_id(block_data)
    return (block_id, block_data, block_size)


def create_blocks(block_count=1, block_size=100, uniform_sizes=False,
                  min_size=1, max_size=2000):
    block_sizes = []
    if uniform_sizes:
        block_sizes = [blocksize for _ in range(block_count)]
    else:
        block_sizes = [random.randrange(min_size, max_size)
                       for block_size in range(block_count)]

    return [create_block(block_size) for block_size in block_sizes]


def create_file():
    return '{0}'.format(str(uuid.uuid4()))


def create_storage_block():
    return '{0}'.format(str(uuid.uuid4()))


class FakeAuthenticator(deuceclient.auth.base.AuthenticationBase):

    def __init__(self, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        self.__tenantid = "tid_{0}".format(str(uuid.uuid4()))

        self.__token_data = {}
        self.__token_data['token'] = None
        self.__token_data['expires'] = datetime.datetime.utcnow()
        self.__token_data['lifetime_seconds'] = 3600  # 1 Hour

    @staticmethod
    def MakeToken(expires_in_seconds):
        expires_at = (datetime.datetime.utcnow() +
                      datetime.timedelta(seconds=expires_in_seconds))
        token = "token_{0}".format(str(uuid.uuid4()))
        return (expires_at, token)

    def GetToken(self, retry=5):
        expires, token = FakeAuthenticator.MakeToken(
            self.__token_data['lifetime_seconds'])
        if expires <= datetime.datetime.utcnow():
            if retry:
                return self.GetToken(retry - 1)
            else:
                return None
        else:
            self.__token_data['token'] = token
            self.__token_data['expires'] = expires
            return token

    def IsExpired(self, fuzz=0):
        test_time = (self.AuthExpirationTime() +
                    datetime.timedelta(seconds=fuzz))
        return (test_time >= datetime.datetime.utcnow())

    def AuthExpirationTime(self):
        return self.__token_data['expires']

    def _AuthToken(self):
        if self.IsExpired():
            return self.GetToken()

        elif self.IsExpired(fuzz=2):
            time.sleep(3)
            return self.GetToken()

        else:
            return self.__access.auth_token

    def _AuthTenantId(self):
        return self.__tenantid
