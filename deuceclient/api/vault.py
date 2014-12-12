"""
Deuce Client - Vault API
"""
import json

from stoplight import validate

from deuceclient.api.afile import File
from deuceclient.api.files import Files
from deuceclient.api.blocks import Blocks
from deuceclient.api.storageblocks import StorageBlocks
from deuceclient.common.validation import *


class Vault(object):

    @validate(project_id=ProjectIdRule, vault_id=VaultIdRule)
    def __init__(self, project_id, vault_id):
        self.__properties = {
            'project_id': project_id,
            'vault_id': vault_id,
            'status': None,
            'statistics': None,
            'blocks': Blocks(project_id=project_id,
                             vault_id=vault_id),
            'storageblocks': StorageBlocks(project_id=project_id,
                                           vault_id=vault_id),
            'files': Files(project_id=project_id,
                           vault_id=vault_id)
        }

    def to_json(self):
        return json.dumps({
            'project_id': self.project_id,
            'vault_id': self.vault_id,
            'blocks': self.blocks.to_json(),
            'storage_blocks': self.storageblocks.to_json(),
            'files': self.files.to_json()
        })

    @staticmethod
    def from_json(serialized_data):
        json_data = json.loads(serialized_data)

        vault = Vault(json_data['project_id'],
                      json_data['vault_id'])
        vault.blocks.from_json(json_data['blocks'])
        vault.storageblocks.from_json(json_data['storage_blocks'])
        vault.files.from_json(json_data['files'])
        return vault

    @property
    def vault_id(self):
        return self.__properties['vault_id']

    @property
    def project_id(self):
        return self.__properties['project_id']

    @property
    def storageblocks(self):
        return self.__properties['storageblocks']

    @property
    def status(self):
        """Returns Vault Status

        By default the status is 'unknown'. This will get updated if the Vault
        is Created, Deleted, or Statistics are retrieved."""
        if self.__properties['status'] is None:
            return 'unknown'
        else:
            return self.__properties['status']

    @status.setter
    def status(self, value):
        try:
            if value is None:
                self.__properties['status'] = 'unknown'
            elif value.lower() in ('unknown', 'created', 'deleted',
                                   'valid', 'invalid'):
                self.__properties['status'] = value.lower()
            else:
                raise ValueError(
                    'Invalid Vault Status Value: {0}'.format(value))
        except AttributeError:
                raise ValueError(
                    'Invalid Vault Status Value: {0}'.format(value))

    @property
    def statistics(self):
        """Return cached Vault Statistics"""
        return self.__properties['statistics']

    @statistics.setter
    def statistics(self, value):
        self.__properties['statistics'] = value

    @property
    def blocks(self):
        return self.__properties['blocks']

    @property
    def files(self):
        return self.__properties['files']

    @validate(file_id=FileIdRule)
    def add_file(self, file_id, file_url=None):
        self.files[file_id] = File(self.project_id,
                                   self.vault_id,
                                   file_id=file_id,
                                   url=file_url)
