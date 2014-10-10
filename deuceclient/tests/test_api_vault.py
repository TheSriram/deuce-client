"""
Testing - Deuce Client - API - Vault
"""
from unittest import TestCase

import deuceclient.api.vault as v
from deuceclient.tests import *


class VaultTest(TestCase):

    def setUp(self):
        super(self.__class__, self).setUp()

        self.vault_id = create_vault_name()
        self.project_id = create_project_name()

    def test_vault_creation(self):
        vault = v.Vault(self.project_id, self.vault_id)

        self.assertEqual(vault.vault_id, self.vault_id)
        self.assertEqual(vault.project_id, self.project_id)
