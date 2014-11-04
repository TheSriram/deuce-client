"""
Tests - Deuce Client - API File
"""
from unittest import TestCase

import deuceclient.api as api
import deuceclient.common.errors as errors
import deuceclient.common.validation as val
from deuceclient.tests import *


class FileTest(TestCase):

    def setUp(self):
        super(FileTest, self).setUp()

        self.project_id = create_project_name()
        self.vault_id = create_vault_name()
        self.file_id = create_file()
        self.block_data = [
            (create_block(), create_storage_block()) for x in range(10)
        ]

    def test_create_file(self):
        a_file = api.File(self.project_id, self.vault_id, self.file_id)

        self.assertEqual(a_file.project_id, self.project_id)
        self.assertEqual(a_file.vault_id, self.vault_id)
        self.assertEqual(a_file.file_id, self.file_id)
        self.assertEqual(a_file.offsets, {})
        self.assertEqual(a_file.blocks, {})
        self.assertIsNone(a_file.url)

    def test_create_file_with_url(self):
        test_url = 'magic.example.com/open/sesame'
        a_file = api.File(self.project_id,
                          self.vault_id,
                          self.file_id,
                          url=test_url)

        self.assertEqual(a_file.project_id, self.project_id)
        self.assertEqual(a_file.vault_id, self.vault_id)
        self.assertEqual(a_file.file_id, self.file_id)
        self.assertEqual(a_file.offsets, {})
        self.assertEqual(a_file.blocks, {})
        self.assertEqual(a_file.url, test_url)

    def test_create_file_no_file_id(self):
        a_file = api.File(self.project_id, self.vault_id)

        self.assertEqual(a_file.project_id, self.project_id)
        self.assertEqual(a_file.vault_id, self.vault_id)
        self.assertIsNone(a_file.file_id)
        self.assertEqual(a_file.offsets, {})
        self.assertEqual(a_file.blocks, {})
        self.assertIsNone(a_file.url)

    def test_create_file_no_file_id_alternate(self):
        a_file = api.File(self.project_id, self.vault_id, file_id=None)

        self.assertEqual(a_file.project_id, self.project_id)
        self.assertEqual(a_file.vault_id, self.vault_id)
        self.assertIsNone(a_file.file_id)
        self.assertEqual(a_file.offsets, {})
        self.assertEqual(a_file.blocks, {})
        self.assertIsNone(a_file.url)

    def test_create_file_update_file_id(self):
        a_file = api.File(self.project_id, self.vault_id)

        self.assertEqual(a_file.project_id, self.project_id)
        self.assertEqual(a_file.vault_id, self.vault_id)
        self.assertIsNone(a_file.file_id)
        self.assertEqual(a_file.offsets, {})
        self.assertEqual(a_file.blocks, {})

        a_file.file_id = self.file_id
        self.assertEqual(a_file.file_id, self.file_id)

    def test_assign_block(self):
        a_file = api.File(self.project_id, self.vault_id, self.file_id)

        self.assertEqual(a_file.project_id, self.project_id)
        self.assertEqual(a_file.vault_id, self.vault_id)
        self.assertEqual(a_file.file_id, self.file_id)
        self.assertEqual(a_file.offsets, {})
        self.assertEqual(a_file.blocks, {})

        offset = 0
        offsets = {}
        for block_data in self.block_data:
            sha1, data, size = block_data[0]
            with self.assertRaises(errors.InvalidBlocks):
                a_file.assign_block(sha1, offset)
            offset = offset + size

        offset = 0
        offsets = {}
        for block_data in self.block_data:
            sha1, data, size = block_data[0]
            block = api.Block(self.project_id, self.vault_id, sha1, data=data)
            a_file.blocks[sha1] = block
            a_file.assign_block(sha1, offset)
            offsets[offset] = sha1

        for k, v in offsets.items():
            self.assertEqual(a_file.get_block_for_offset(k), v)

    def test_get_block_offsets(self):
        a_file = api.File(self.project_id, self.vault_id, self.file_id)

        self.assertEqual(a_file.project_id, self.project_id)
        self.assertEqual(a_file.vault_id, self.vault_id)
        self.assertEqual(a_file.file_id, self.file_id)
        self.assertEqual(a_file.offsets, {})
        self.assertEqual(a_file.blocks, {})

        offset = 0
        offsets = {}
        for block_data in self.block_data:
            sha1, data, size = block_data[0]
            block = api.Block(self.project_id, self.vault_id, sha1, data=data)
            a_file.blocks[sha1] = block
            a_file.assign_block(sha1, offset)
            offsets[offset] = sha1

        ub_sha1, ub_data, ub_size = create_block()
        self.assertEqual(a_file.get_offsets_for_block(ub_sha1), [])

        for k, v in offsets.items():
            x = [k]
            self.assertEqual(a_file.get_offsets_for_block(v), x)

    def test_invalid_offsets(self):
        a_file = api.File(self.project_id, self.vault_id, self.file_id)
        sha1, data, size = self.block_data[0][0]

        with self.assertRaises(errors.ParameterConstraintError):
            a_file.assign_block(sha1, 'howdy')

        with self.assertRaises(errors.ParameterConstraintError):
            a_file.assign_block(sha1, -1)
