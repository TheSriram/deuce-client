"""
Deuce Client - Storage Blocks API
"""
import json

from stoplight import validate

from deuceclient.api.block import Block
from deuceclient.common.validation import *


class StorageBlocks(dict):
    """
    A collection of storage blocks
    """

    @validate(project_id=ProjectIdRule, vault_id=VaultIdRule)
    def __init__(self, project_id, vault_id):
        super(self.__class__, self).__init__()
        self.__properties = {
            'marker': None,
            'project_id': project_id,
            'vault_id': vault_id
        }

    def to_json(self):
        return json.dumps({
            'marker': self.marker,
            'project_id': self.project_id,
            'vault_id': self.vault_id,
            'blocks': {
                block_id: self[block_id].to_json()
                for block_id in self.keys()
            }
        })

    def from_json(self, serialized_data):
        json_data = json.loads(serialized_data)
        self.marker = json_data['marker']
        self.__properties['project_id'] = json_data['project_id']
        self.__properties['vault_id'] = json_data['vault_id']
        self.update({
            k: Block.from_json(v)
            for k, v in json_data['blocks']
        })

    @validate(key=StorageBlockIdRule)
    def __getitem__(self, key):
        return dict.__getitem__(self, key)

    @validate(key=StorageBlockIdRule, val=StorageBlockType)
    def __setitem__(self, key, val):
        if isinstance(val, Block):
            return dict.__setitem__(self, key, val)
        else:
            raise TypeError(
                '{0} can only contain Blocks'.format(self.__class__))

    @property
    def project_id(self):
        return self.__properties['project_id']

    @property
    def vault_id(self):
        return self.__properties['vault_id']

    @property
    def marker(self):
        return self.__properties['marker']

    @marker.setter
    @validate(value=StorageBlockIdRuleNoneOkay)
    def marker(self, value):
        # Note: We could force that "marker" is in the dict;
        #   but then that would also unnecessarily force
        #   order-of-operations on how to use the object
        self.__properties['marker'] = value

    def __repr__(self):
        return '{0}: {1}'.format(type(self).__name__,
                                 dict.__repr__(self))

    def update(self, *args, **kwargs):
        # For use of StorageBlocks.__setitem__ in order
        # to get validation of each entry in the incoming dictionary
        for k, v in dict(*args, **kwargs).items():
            self[k] = v
