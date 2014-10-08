"""
Deuce Client - Project API
"""
from stoplight import validate

from deuceclient.api.vault import Vault
from deuceclient.common.validation import *


class Project(dict):
    """
    Collection for a given project_id that contains the vaults associated
    with that project_id
    """

    @validate(project_id=ProjectIdRule)
    def __init__(self, project_id):
        super(self.__class__, self).__init__()

        if not isinstance(project_id, str):
            raise TypeError('project_id must be type str()')

        self.__project_id = project_id

    @property
    def project_id(self):
        return self.__project_id

    @validate(key=VaultIdRule)
    def __getitem___(self, key):
        return dict.__getitem__(self, key)

    @validate(key=VaultIdRule)
    def __setitem__(self, key, val):
        if isinstance(val, Vault):
            return dict.__setitem__(self, key, val)
        else:
            raise TypeError(
                '{0} can only contain Vaults'.format(self.__class__))

    def __repr__(self):
        return '{1} ({0})'.format(type(self).__name__, dict.__repr__(self))

    def update(self, *args, **kwargs):
        for k, v in dict(*args, **kwargs).iteritems():
            self[k] = v
