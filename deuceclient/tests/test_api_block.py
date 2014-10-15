"""
Testing - Deuce Client - API Block
"""
import datetime
from unittest import TestCase

import deuceclient.api.block as b
import deuceclient.common.errors as errors
import deuceclient.common.validation as val
from deuceclient.tests import *


class BlockTest(TestCase):

    def setUp(self):
        super(self.__class__, self).setUp()

        self.project_id = create_project_name()
        self.vault_id = create_vault_name()
        self.block = create_block()
        self.storage_id = create_storage_block()

    def test_create_block(self):
        block = b.Block(self.project_id,
                        self.vault_id,
                        self.block[0])
        self.assertEqual(self.project_id,
                         block.project_id)
        self.assertEqual(self.vault_id,
                         block.vault_id)
        self.assertEqual(self.block[0],
                         block.block_id)
        self.assertIsNone(block.storage_id)
        self.assertIsNone(block.data)
        self.assertIsNone(block.ref_count)
        self.assertIsNone(block.ref_modified)

    def test_create_block_with_data(self):
        block = b.Block(self.project_id,
                        self.vault_id,
                        self.block[0],
                        storage_id=None,
                        data=self.block[1])
        self.assertEqual(self.project_id,
                         block.project_id)
        self.assertEqual(self.vault_id,
                         block.vault_id)
        self.assertEqual(self.block[0],
                         block.block_id)
        self.assertIsNone(block.storage_id)
        self.assertEqual(self.block[1],
                         block.data)
        self.assertIsNone(block.ref_count)
        self.assertIsNone(block.ref_modified)

    def test_create_block_with_storage_id(self):
        block = b.Block(self.project_id,
                        self.vault_id,
                        self.block[0],
                        storage_id=self.storage_id,
                        data=self.block[1])
        self.assertEqual(self.project_id,
                         block.project_id)
        self.assertEqual(self.vault_id,
                         block.vault_id)
        self.assertEqual(self.block[0],
                         block.block_id)
        self.assertIsNotNone(block.storage_id)
        self.assertEqual(self.storage_id,
                         block.storage_id)
        self.assertEqual(self.block[1],
                         block.data)
        self.assertIsNone(block.ref_count)
        self.assertIsNone(block.ref_modified)

    def test_create_block_with_invalid_storage_id(self):
        storage_id = 'Say, have you seen a wabbit wun by here?'

        with self.assertRaises(errors.InvalidStorageBlocks):
            block = b.Block(self.project_id,
                            self.vault_id,
                            self.block[0],
                            storage_id=storage_id,
                            data=self.block[1])

    def test_create_block_with_ref_count(self):
        ref_count = 5
        block = b.Block(self.project_id,
                        self.vault_id,
                        self.block[0],
                        storage_id=self.storage_id,
                        ref_count=ref_count)
        self.assertEqual(self.project_id,
                         block.project_id)
        self.assertEqual(self.vault_id,
                         block.vault_id)
        self.assertEqual(self.block[0],
                         block.block_id)
        self.assertIsNotNone(block.storage_id)
        self.assertEqual(self.storage_id,
                         block.storage_id)
        self.assertIsNone(block.data)
        self.assertEqual(ref_count, block.ref_count)
        self.assertIsNone(block.ref_modified)

    def test_create_block_with_ref_modified(self):
        ref_modified = int(datetime.datetime.utcnow().timestamp())
        block = b.Block(self.project_id,
                        self.vault_id,
                        self.block[0],
                        storage_id=self.storage_id,
                        ref_modified=ref_modified)
        self.assertEqual(self.project_id,
                         block.project_id)
        self.assertEqual(self.vault_id,
                         block.vault_id)
        self.assertEqual(self.block[0],
                         block.block_id)
        self.assertIsNotNone(block.storage_id)
        self.assertEqual(self.storage_id,
                         block.storage_id)
        self.assertIsNone(block.data)
        self.assertIsNone(block.ref_count)
        self.assertEqual(ref_modified, block.ref_modified)

    def test_update_block_storage_id(self):
        block = b.Block(self.project_id,
                        self.vault_id,
                        self.block[0])

        block.storage_id = self.storage_id
        self.assertEqual(self.storage_id,
                         block.storage_id)

    def test_update_block_storage_id_invalid(self):
        block = b.Block(self.project_id,
                        self.vault_id,
                        self.block[0])

        storage_id = 'that wasically wabbit'
        with self.assertRaises(errors.InvalidStorageBlocks):
            block.storage_id = storage_id

    def test_update_block_data(self):
        block = b.Block(self.project_id,
                        self.vault_id,
                        self.block[0])

        block.data = self.block[1]
        self.assertEqual(self.block[1],
                         block.data)

    def test_reset_block_data(self):
        block = b.Block(self.project_id,
                        self.vault_id,
                        self.block[0],
                        data=self.block[1])

        self.assertIsNotNone(block.data)
        self.assertEqual(self.block[1],
                         block.data)

        block.data = None

        self.assertIsNone(block.data)
        self.assertNotEqual(self.block[1],
                            block.data)

    def test_update_ref_count(self):
        block = b.Block(self.project_id,
                        self.vault_id,
                        self.block[0])

        self.assertIsNone(block.ref_count)

        ref_count = 10
        block.ref_count = ref_count

        self.assertIsNotNone(block.ref_count)
        self.assertEqual(ref_count,
                         block.ref_count)

    def test_reset_ref_count(self):
        ref_count = 10
        block = b.Block(self.project_id,
                        self.vault_id,
                        self.block[0],
                        ref_count=ref_count)

        self.assertIsNotNone(block.ref_count)
        self.assertEqual(ref_count,
                         block.ref_count)

        block.ref_count = None

        self.assertIsNone(block.ref_count)
        self.assertNotEqual(ref_count,
                            block.ref_count)

    def test_update_ref_modified(self):
        block = b.Block(self.project_id,
                        self.vault_id,
                        self.block[0])

        self.assertIsNone(block.ref_modified)

        ref_modified = datetime.datetime.utcnow().toordinal()
        block.ref_modified = ref_modified

        self.assertIsNotNone(block.ref_modified)
        self.assertEqual(ref_modified,
                         block.ref_modified)

    def test_reset_ref_modified(self):
        ref_modified = datetime.datetime.utcnow().toordinal()
        block = b.Block(self.project_id,
                        self.vault_id,
                        self.block[0],
                        ref_modified=ref_modified)

        self.assertIsNotNone(block.ref_modified)
        self.assertEqual(ref_modified,
                         block.ref_modified)

        block.ref_modified = None

        self.assertIsNone(block.ref_modified)
        self.assertNotEqual(ref_modified,
                            block.ref_modified)
