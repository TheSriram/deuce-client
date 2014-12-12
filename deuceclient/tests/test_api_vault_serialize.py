"""
Tests - Deuce Client - API - Vault - Serialize
"""
from unittest import TestCase

import deuceclient.api as api
import deuceclient.common.errors as errors
import deuceclient.common.validation as val
from deuceclient.tests import *


class VaultTest(TestCase):

    def setUp(self):
        super(VaultTest, self).setUp()

        self.vault_id = create_vault_name()
        self.project_id = create_project_name()

    def test_serialize(self):
        vault = api.Vault(self.project_id,
                          self.vault_id)

        json_data = vault.to_json()

        new_vault = api.Vault.from_json(json_data)

    def test_serialize(self):
        vault = api.Vault(self.project_id,
                          self.vault_id)

        vault.blocks.update({
            block[0]: api.Block(self.project_id,
                                self.vault_id,
                                block_id=block[0],
                                data=block[1],
                                block_size=block[2])
            for block in create_blocks(block_count=10)
        })
        vault.files.update({
            file_id: api.File(self.project_id,
                              self.vault_id,
                              file_id)
            for file_id in [create_file() for _ in range(10)]
        })

        json_data = vault.to_json()

        new_vault = api.Vault.from_json(json_data)
