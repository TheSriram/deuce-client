"""
Deuce Client - Vault API
"""
from stoplight import validate

import deuceclient.api
from deuceclient.common.validation import *


class Vault(object):

    @validate(project_id=ProjectIdRule, vault_id=VaultIdRule)
    def __init__(self, project_id, vault_id):
        self.__project_id = project_id
        self.__vault_id = vault_id
        self.__files = dict()
        self.__blocks = deuceclient.api.Blocks()
        self.__properties = {
            'status': None,
            'statistics': None
        }

    @property
    def vault_id(self):
        return self.__vault_id

    @property
    def project_id(self):
        return self.__project_id

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
