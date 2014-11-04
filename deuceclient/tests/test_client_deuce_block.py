"""
Tests - Deuce Client - Client - Deuce - Block
"""
import json

import httpretty

import deuceclient.client.deuce
import deuceclient.api as api
from deuceclient.tests import *


class ClientDeuceBlockTests(ClientTestBase):

    def setUp(self):
        super(ClientDeuceBlockTests, self).setUp()

    def tearDown(self):
        super(ClientDeuceBlockTests, self).tearDown()

    @httpretty.activate
    def test_block_list(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        data = [block[0] for block in create_blocks(block_count=1)]
        expected_data = json.dumps(data)

        httpretty.register_uri(httpretty.GET,
                               get_blocks_url(self.apihost,
                                              self.vault.vault_id),
                               content_type='application/json',
                               body=expected_data,
                               status=200)

        self.assertTrue(client.GetBlockList(self.vault))
        for block_id in data:
            self.assertIn(block_id, self.vault.blocks)

    @httpretty.activate
    def test_block_list_with_marker(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        block_id, block_data, block_size = create_block()

        data = [block[0] for block in create_blocks(block_count=1)]
        expected_data = json.dumps(data)

        httpretty.register_uri(httpretty.GET,
                               get_blocks_url(self.apihost,
                                              self.vault.vault_id),
                               content_type='application/json',
                               body=expected_data,
                               status=200)

        self.assertTrue(client.GetBlockList(self.vault, marker=block_id))
        for block_id in data:
            self.assertIn(block_id, self.vault.blocks)

    @httpretty.activate
    def test_block_list_with_marker_and_limit(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        block_id, block_data, block_size = create_block()

        data = [block[0] for block in create_blocks(block_count=5)]
        expected_data = json.dumps(data)

        httpretty.register_uri(httpretty.GET,
                               get_blocks_url(self.apihost,
                                              self.vault.vault_id),
                               content_type='application/json',
                               body=expected_data,
                               status=200)

        self.assertTrue(client.GetBlockList(self.vault,
                                            marker=block_id,
                                            limit=5))
        self.assertEqual(len(data), len(self.vault.blocks))
        for block_id in data:
            self.assertIn(block_id, self.vault.blocks)

    @httpretty.activate
    def test_block_list_with_limit(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        data = [block[0] for block in create_blocks(block_count=5)]
        expected_data = json.dumps(data)

        httpretty.register_uri(httpretty.GET,
                               get_blocks_url(self.apihost,
                                              self.vault.vault_id),
                               content_type='application/json',
                               body=expected_data,
                               status=200)

        self.assertTrue(client.GetBlockList(self.vault, limit=5))
        self.assertEqual(5, len(self.vault.blocks))
        self.assertEqual(len(data), len(self.vault.blocks))
        for block_id in data:
            self.assertIn(block_id, self.vault.blocks)

    def test_block_list_bad_vault(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        data = [block[0] for block in create_blocks(block_count=1)]
        expected_data = json.dumps(data)

        with self.assertRaises(TypeError):
            client.GetBlockList(self.vault.vault_id)

    @httpretty.activate
    def test_block_list_failure(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        httpretty.register_uri(httpretty.GET,
                               get_blocks_url(self.apihost,
                                              self.vault.vault_id),
                               content_type='text/plain',
                               body="mock failure",
                               status=404)

        with self.assertRaises(RuntimeError) as stats_error:
            client.GetBlockList(self.vault)

    @httpretty.activate
    def test_block_upload(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        blockid, blockdata, block_size = create_block()
        block = api.Block(project_id=self.vault.project_id,
                          vault_id=self.vault.vault_id,
                          block_id=blockid,
                          data=blockdata)

        httpretty.register_uri(httpretty.PUT,
                               get_block_url(self.apihost,
                                             self.vault.vault_id,
                                             blockid),
                               status=201)

        self.assertTrue(client.UploadBlock(self.vault, block))

    def test_block_upload_bad_vault(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        blockid, blockdata, block_size = create_block()
        block = api.Block(project_id=self.vault.project_id,
                          vault_id=self.vault.vault_id,
                          block_id=blockid,
                          data=blockdata)

        with self.assertRaises(TypeError):
            client.UploadBlock(self.vault.vault_id, block)

    def test_block_upload_bad_block(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        blockid, blockdata, block_size = create_block()
        block = api.Block(project_id=self.vault.project_id,
                          vault_id=self.vault.vault_id,
                          block_id=blockid,
                          data=blockdata)

        with self.assertRaises(TypeError):
            client.UploadBlock(self.vault, block.block_id)

    @httpretty.activate
    def test_block_upload_failed(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        blockid, blockdata, block_size = create_block()
        block = api.Block(project_id=self.vault.project_id,
                          vault_id=self.vault.vault_id,
                          block_id=blockid,
                          data=blockdata)

        httpretty.register_uri(httpretty.PUT,
                               get_block_url(self.apihost,
                                             self.vault.vault_id,
                                             blockid),
                               status=404)

        with self.assertRaises(RuntimeError) as upload_error:
            client.UploadBlock(self.vault, block)

    @httpretty.activate
    def test_block_deletion(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        blockid, blockdata, block_size = create_block()
        block = api.Block(project_id=self.vault.project_id,
                          vault_id=self.vault.vault_id,
                          block_id=blockid,
                          data=blockdata)

        httpretty.register_uri(httpretty.DELETE,
                               get_block_url(self.apihost,
                                             self.vault.vault_id,
                                             blockid),
                               status=204)

        self.assertTrue(client.DeleteBlock(self.vault, block))

    def test_block_deletion_bad_vault(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        blockid, blockdata, block_size = create_block()
        block = api.Block(project_id=self.vault.project_id,
                          vault_id=self.vault.vault_id,
                          block_id=blockid,
                          data=blockdata)

        with self.assertRaises(TypeError):
            client.DeleteBlock(self.vault.vault_id, block)

    def test_block_deletion_bad_block(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        blockid, blockdata, block_size = create_block()
        block = api.Block(project_id=self.vault.project_id,
                          vault_id=self.vault.vault_id,
                          block_id=blockid,
                          data=blockdata)

        with self.assertRaises(TypeError):
            client.DeleteBlock(self.vault, block.block_id)

    @httpretty.activate
    def test_block_deletion_failed(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        blockid, blockdata, block_size = create_block()
        block = api.Block(project_id=self.vault.project_id,
                          vault_id=self.vault.vault_id,
                          block_id=blockid,
                          data=blockdata)

        httpretty.register_uri(httpretty.DELETE,
                               get_block_url(self.apihost,
                                             self.vault.vault_id,
                                             blockid),
                               content_type='text/plain',
                               body="mock failure",
                               status=404)

        with self.assertRaises(RuntimeError) as deletion_error:
            client.DeleteBlock(self.vault, block)

    @httpretty.activate
    def test_block_download(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        blockid, blockdata, block_size = create_block()
        block = api.Block(project_id=self.vault.project_id,
                          vault_id=self.vault.vault_id,
                          block_id=blockid)

        httpretty.register_uri(httpretty.GET,
                               get_block_url(self.apihost,
                                             self.vault.vault_id,
                                             blockid),
                               content_type='text/plain',
                               body=blockdata,
                               status=200)

        self.assertTrue(client.DownloadBlock(self.vault, block))
        self.assertEqual(block.data, blockdata)

    def test_block_download_bad_vault(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        blockid, blockdata, block_size = create_block()
        block = api.Block(project_id=self.vault.project_id,
                          vault_id=self.vault.vault_id,
                          block_id=blockid)

        with self.assertRaises(TypeError):
            client.DownloadBlock(self.vault.vault_id, block)

    def test_block_download_bad_block(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        blockid, blockdata, block_size = create_block()
        block = api.Block(project_id=self.vault.project_id,
                          vault_id=self.vault.vault_id,
                          block_id=blockid)

        with self.assertRaises(TypeError):
            client.DownloadBlock(self.vault, block.block_id)

    @httpretty.activate
    def test_block_download_failed(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        blockid, blockdata, block_size = create_block()
        block = api.Block(project_id=self.vault.project_id,
                          vault_id=self.vault.vault_id,
                          block_id=blockid)

        httpretty.register_uri(httpretty.GET,
                               get_block_url(self.apihost,
                                             self.vault.vault_id,
                                             blockid),
                               content_type='text/plain',
                               body="mock failure",
                               status=404)

        with self.assertRaises(RuntimeError) as deletion_error:
            client.DownloadBlock(self.vault, block)
