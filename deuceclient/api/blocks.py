"""
Deuce Client - Blocks API
"""
from stoplight import validate

from deuceclient.api.block import Block
from deuceclient.common.validation import *


class Blocks(dict):
    """
    A collection of blocks
    """

    def __init__(self):
        super(self.__class__, self).__init__()

    @validate(key=MetadataBlockIdRule)
    def __getitem__(self, key):
        return dict.__getitem__(self, key)

    @validate(key=MetadataBlockIdRule)
    def __setitem__(self, key, val):
        if isinstance(val, Block):
            return dict.__setitem__(self, key, val)
        else:
            raise TypeError(
                '{0} can only contain Blocks'.format(self.__class__))

    def __repr__(self):
        return '{0}: {1}'.format(type(self).__name__,
                                 dict.__repr__(self))

    def update(self, *args, **kwargs):
        # For use of Blocks.__setitem__ in order
        # to get validation of each entry in the incoming dictionary
        for k, v in dict(*args, **kwargs).items():
            self[k] = v


class StorageBlocks(dict):
    """
    A collection of storage blocks
    """

    def __init__(self):
        super(self.__class__, self).__init__()

    @validate(key=StorageBlockIdRule)
    def __getitem__(self, key):
        return dict.__getitem__(self, key)

    @validate(key=StorageBlockIdRule)
    def __setitem__(self, key, val):
        if isinstance(val, Block):
            return dict.__setitem__(self, key, val)
        else:
            raise TypeError(
                '{0} can only contain Blocks'.format(self.__class__))

    def __repr__(self):
        return '{0}: {1}'.format(type(self).__name__,
                                 dict.__repr__(self))

    def update(self, *args, **kwargs):
        # For use of StorageBlocks.__setitem__ in order
        # to get validation of each entry in the incoming dictionary
        for k, v in dict(*args, **kwargs).items():
            self[k] = v
