"""
Deuce Client - Vault API
"""
from stoplight import validate

from deuceclient.common.validation import *


class Vault(object):

    @validate(project_id=ProjectIdRule, vault_id=VaultIdRule)
    def __init__(self, project_id, vault_id):
        self.__project_id = project_id
        self.__vault_id = vault_id
        self.__files = dict()
        self.__blocks = {
            'metadata': dict(),
            'storage': dict()
        }

    @property
    def vault_id(self):
        return self.__vault_id

    @property
    def project_id(self):
        return self.__project_id
