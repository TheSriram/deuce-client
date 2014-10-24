"""
Deuce Client - Block API
"""
from stoplight import validate

from deuceclient.common.validation import *


class Block(object):

    # TODO: Add a validator for data, ref_count, ref_modified
    @validate(project_id=ProjectIdRule,
              vault_id=VaultIdRule,
              block_id=MetadataBlockIdRuleNoneOkay,
              storage_id=StorageBlockIdRuleNoneOkay)
    def __init__(self, project_id, vault_id, block_id=None,
                 storage_id=None, data=None,
                 ref_count=None, ref_modified=None, block_size=None,
                 block_orphaned='indeterminate'):
        if block_id is None and storage_id is None:
            raise ValueError("Both storage_id and block_id cannot be None")
        self.__properties = {
            'project_id': project_id,
            'vault_id': vault_id,
            'block_id': block_id,
            'storage_id': storage_id,
            'data': data,
            'references': {
                'count': ref_count,
                'modified': ref_modified,
            },
            'block_size': block_size,
            'block_orphaned': block_orphaned
        }

    @property
    def project_id(self):
        return self.__properties['project_id']

    @property
    def vault_id(self):
        return self.__properties['vault_id']

    @property
    def block_id(self):
        return self.__properties['block_id']

    @block_id.setter
    @validate(value=MetadataBlockIdRule)
    def block_id(self, value):
        self.__properties['block_id'] = value

    @property
    def storage_id(self):
        return self.__properties['storage_id']

    @storage_id.setter
    @validate(value=StorageBlockIdRule)
    def storage_id(self, value):
        self.__properties['storage_id'] = value

    @property
    def data(self):
        return self.__properties['data']

    # TODO: Add a validator
    @data.setter
    def data(self, value):
        self.__properties['data'] = value

    @property
    def block_size(self):
        return self.__properties['block_size']

    @block_size.setter
    def block_size(self, value):
        self.__properties['block_size'] = value

    @property
    def block_orphaned(self):
        return self.__properties['block_orphaned']

    @block_orphaned.setter
    @validate(value=BoolRule)
    def block_orphaned(self, value):
        self.__properties['block_orphaned'] = value

    @property
    def ref_count(self):
        return self.__properties['references']['count']

    # TODO: Add a validator
    @ref_count.setter
    def ref_count(self, value):
        self.__properties['references']['count'] = value

    @property
    def ref_modified(self):
        return self.__properties['references']['modified']

    # TODO: Add a validator
    @ref_modified.setter
    def ref_modified(self, value):
        self.__properties['references']['modified'] = value
