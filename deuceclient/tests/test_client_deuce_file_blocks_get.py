"""
Tests - Deuce Client - Client - Deuce - File - Blocks - Get
"""
import json

import httpretty

import deuceclient.api as api
import deuceclient.client.deuce
from deuceclient.tests import *


class ClientDeuceFileGetBlocksTests(ClientTestBase):

    def setUp(self):
        super(ClientDeuceFileGetBlocksTests, self).setUp()

    def tearDown(self):
        super(ClientDeuceFileGetBlocksTests, self).tearDown()

    @httpretty.activate
    def test_file_blocks_get(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        file_id = create_file()
        self.vault.files[file_id] = api.File(project_id=self.vault.project_id,
                                             vault_id=self.vault.vault_id,
                                             file_id=file_id)

        data = []
        return_data = []

        running_offset = 0
        for block_id, block_data, block_size in create_blocks(5):
            data.append((block_id, running_offset))
            return_data.append(block_id)

            running_offset = running_offset + block_size

        httpretty.register_uri(httpretty.GET,
                               get_file_blocks_url(self.apihost,
                                                   self.vault.vault_id,
                                                   file_id),
                               body=json.dumps(data),
                               status=200)

        self.assertEqual(return_data,
                         client.GetFileBlockList(self.vault, file_id))

    def test_file_blocks_get_bad_vault(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        file_id = create_file()
        self.vault.files[file_id] = api.File(project_id=self.vault.project_id,
                                             vault_id=self.vault.vault_id,
                                             file_id=file_id)

        with self.assertRaises(TypeError):
            client.GetFileBlockList(self.vault.vault_id, file_id)

    def test_file_blocks_get_bad_fileid(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        file_id = create_file()

        with self.assertRaises(KeyError):
            client.GetFileBlockList(self.vault, file_id)

    @httpretty.activate
    def test_file_blocks_get_with_marker(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        block_id, block_data, block_size = create_block()
        file_id = create_file()
        self.vault.files[file_id] = api.File(project_id=self.vault.project_id,
                                             vault_id=self.vault.vault_id,
                                             file_id=file_id)
        data = []
        return_data = []

        running_offset = 0
        for block_id, block_data, block_size in create_blocks(5):
            data.append((block_id, running_offset))
            return_data.append(block_id)

            running_offset = running_offset + block_size

        httpretty.register_uri(httpretty.GET,
                               get_file_blocks_url(self.apihost,
                                                   self.vault.vault_id,
                                                   file_id),
                               body=json.dumps(data),
                               status=200)

        self.assertEqual(return_data,
                         client.GetFileBlockList(self.vault,
                                                file_id,
                                                marker=block_id))

    @httpretty.activate
    def test_file_blocks_get_with_marker_and_limit(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        block_id, block_data, block_size = create_block()
        file_id = create_file()
        self.vault.files[file_id] = api.File(project_id=self.vault.project_id,
                                             vault_id=self.vault.vault_id,
                                             file_id=file_id)
        data = []
        return_data = []

        running_offset = 0
        for block_id, block_data, block_size in create_blocks(5):
            data.append((block_id, running_offset))
            return_data.append(block_id)

            running_offset = running_offset + block_size

        httpretty.register_uri(httpretty.GET,
                               get_file_blocks_url(self.apihost,
                                                   self.vault.vault_id,
                                                   file_id),
                               body=json.dumps(data),
                               status=200)

        self.assertEqual(return_data,
                         client.GetFileBlockList(self.vault,
                                                file_id,
                                                marker=block_id,
                                                limit=5))

    @httpretty.activate
    def test_file_blocks_get_with_limit_only(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        block_id, block_data, block_size = create_block()
        file_id = create_file()
        self.vault.files[file_id] = api.File(project_id=self.vault.project_id,
                                             vault_id=self.vault.vault_id,
                                             file_id=file_id)
        data = []
        return_data = []

        running_offset = 0
        for block_id, block_data, block_size in create_blocks(5):
            data.append((block_id, running_offset))
            return_data.append(block_id)

            running_offset = running_offset + block_size

        httpretty.register_uri(httpretty.GET,
                               get_file_blocks_url(self.apihost,
                                                   self.vault.vault_id,
                                                   file_id),
                               body=json.dumps(data),
                               status=200)

        self.assertEqual(return_data,
                         client.GetFileBlockList(self.vault,
                                                file_id,
                                                limit=5))

    @httpretty.activate
    def test_file_blocks_get_failed(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        file_id = create_file()
        self.vault.files[file_id] = api.File(project_id=self.vault.project_id,
                                             vault_id=self.vault.vault_id,
                                             file_id=file_id)
        data = []

        running_offset = 0
        for block_id, block_data, block_size in create_blocks(5):
            data.append((running_offset, block_id))

            running_offset = running_offset + block_size

        httpretty.register_uri(httpretty.GET,
                               get_file_blocks_url(self.apihost,
                                                   self.vault.vault_id,
                                                   file_id),
                               body=json.dumps(data),
                               status=404)

        with self.assertRaises(RuntimeError) as stats_error:
            client.GetFileBlockList(self.vault, file_id)
