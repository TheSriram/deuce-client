"""
Testing - Deuce Client - API Storage Blocks
"""
from unittest import TestCase

import deuceclient.api.storageblocks as bs
import deuceclient.api.block as b
import deuceclient.common.errors as errors
import deuceclient.common.validation as val
from deuceclient.tests import *


class StorageBlocksTest(TestCase):

    def setUp(self):
        super(self.__class__, self).setUp()

        self.project_id = create_project_name()
        self.vault_id = create_vault_name()
        self.storageblock = create_storage_block()

    def test_create_blocks(self):
        blocks = bs.StorageBlocks()

    def test_add_block(self):
        block = b.Block(self.project_id,
                        self.vault_id,
                        storage_id=self.storageblock)

        blocks = bs.StorageBlocks()
        blocks[block.storage_id] = block

        self.assertEqual(block,
                         blocks[block.storage_id])

    def test_add_invalid_block(self):
        block = b.Block(self.project_id,
                        self.vault_id,
                        storage_id=self.storageblock)

        blocks = bs.StorageBlocks()
        with self.assertRaises(errors.InvalidStorageBlocks):
            blocks['alfonso'] = block

        with self.assertRaises(TypeError):
            blocks[block.storage_id] = 'what\'s up doc?'

    def test_repr(self):
        blocks = bs.StorageBlocks()

        serialized_blocks = repr(blocks)

    def test_repr_with_data(self):
        block = b.Block(self.project_id,
                        self.vault_id,
                        storage_id=self.storageblock)

        blocks = bs.StorageBlocks()
        blocks[block.storage_id] = block

        serialized_blocks = repr(blocks)

    def test_update(self):
        block = b.Block(self.project_id,
                        self.vault_id,
                        storage_id=self.storageblock)

        blocks = bs.StorageBlocks()
        blocks.update({
            block.storage_id: block
        })

        self.assertEqual(block,
                         blocks[block.storage_id])

    def test_update_invalid(self):
        block = b.Block(self.project_id,
                        self.vault_id,
                        storage_id=self.storageblock)

        blocks = bs.StorageBlocks()
        with self.assertRaises(TypeError):
            blocks.update({
                self.storageblock: 'be vewy, vewy quiet'
            })

        with self.assertRaises(errors.InvalidStorageBlocks):
            blocks.update({
                'alfonso': block
            })