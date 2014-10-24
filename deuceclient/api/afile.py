"""
Deuce Client - File API
"""
from stoplight import validate

from deuceclient.api.blocks import Blocks
from deuceclient.common.validation import *


class File(object):

    @validate(project_id=ProjectIdRule,
              vault_id=VaultIdRule,
              file_id=FileIdRuleNoneOkay)
    def __init__(self, project_id, vault_id, file_id=None):
        self.__properties = {
            'project_id': project_id,
            'vault_id': vault_id,
            'file_id': file_id,
            'blocks': Blocks(),
            'offsets': {}
        }

    @property
    def project_id(self):
        return self.__properties['project_id']

    @property
    def vault_id(self):
        return self.__properties['vault_id']

    @property
    def file_id(self):
        return self.__properties['file_id']

    @file_id.setter
    @validate(value=FileIdRule)
    def file_id(self, value):
        self.__properties['file_id'] = value

    @property
    def blocks(self):
        return self.__properties['blocks']

    @property
    def offsets(self):
        return self.__properties['offsets']

    @validate(block_id=MetadataBlockIdRule, offset=FileBlockOffsetRule)
    def assign_block(self, block_id, offset):
        if block_id in self.blocks:
            self.offsets[str(offset)] = block_id
        else:
            raise errors.InvalidBlocks(
                'Unable to find block id {0} for file {1}'.format(
                    self.file_id, block_id))

    @validate(offset=FileBlockOffsetRule)
    def get_block_for_offset(self, offset):
        return self.__properties['offsets'][str(offset)]

    @validate(block_id=MetadataBlockIdRule)
    def get_offsets_for_block(self, block_id):
        offsets = []
        for offset, block in self.offsets.items():
            if block == block_id:
                offsets.append(int(offset))
        return offsets
