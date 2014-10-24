"""
Deuce Client - Files API
"""
from stoplight import validate

from deuceclient.api.afile import File
from deuceclient.common.validation import *


class Files(dict):
    """
    A collection of files
    """

    def __init__(self):
        super(self.__class__, self).__init__()

    @validate(key=FileIdRule)
    def __getitem__(self, key):
        return dict.__getitem__(self, key)

    @validate(key=FileIdRule)
    def __setitem__(self, key, val):
        if isinstance(val, File):
            return dict.__setitem__(self, key, val)
        else:
            raise TypeError(
                '{0} can only contain Files'.format(self.__class__))

    def __repr__(self):
        return '{0}: {1}'.format(type(self).__name__,
                                 dict.__repr__(self))

    def update(self, *args, **kwargs):
        # For use of Files.__setitem__ in order
        # to get validation of each entry in the incoming dictionary
        for k, v in dict(*args, **kwargs).items():
            self[k] = v
