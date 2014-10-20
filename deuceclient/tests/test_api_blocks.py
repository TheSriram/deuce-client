"""
Testing - Deuce Client - API Blocks
"""
from unittest import TestCase

import deuceclient.api as api
import deuceclient.common.errors as errors
import deuceclient.common.validation as val
from deuceclient.tests import *


class BlocksTest(TestCase):

    def setUp(self):
        super(self.__class__, self).setUp()

        self.project_id = create_project_name()
        self.vault_id = create_vault_name()
        self.block = create_block()

    def test_create_blocks(self):
        blocks = api.Blocks()

    def test_add_block(self):
        block = api.Block(self.project_id,
                          self.vault_id,
                          self.block[0])

        blocks = api.Blocks()
        blocks[block.block_id] = block

        self.assertEqual(block,
                         blocks[block.block_id])

    def test_add_invalid_block(self):
        block = api.Block(self.project_id,
                          self.vault_id,
                          self.block[0])

        blocks = api.Blocks()
        with self.assertRaises(errors.InvalidBlocks):
            blocks['alfonso'] = block

        with self.assertRaises(TypeError):
            blocks[block.block_id] = 'what\'s up doc?'

    def test_repr(self):
        blocks = api.Blocks()

        serialized_blocks = repr(blocks)

    def test_repr_with_data(self):
        block = api.Block(self.project_id,
                          self.vault_id,
                          self.block[0])

        blocks = api.Blocks()
        blocks[block.block_id] = block

        serialized_blocks = repr(blocks)

    def test_update(self):
        block = api.Block(self.project_id,
                          self.vault_id,
                         self.block[0])

        blocks = api.Blocks()
        blocks.update({
            block.block_id: block
        })

        self.assertEqual(block,
                         blocks[block.block_id])

    def test_update_invalid(self):
        block = api.Block(self.project_id,
                          self.vault_id,
                          self.block[0])

        blocks = api.Blocks()
        with self.assertRaises(TypeError):
            blocks.update({
                self.block[0]: 'be vewy, vewy quiet'
            })

        with self.assertRaises(errors.InvalidBlocks):
            blocks.update({
                'alfonso': block
            })
