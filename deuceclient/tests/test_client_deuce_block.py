"""
Tests - Deuce Client - Client - Deuce - Block
"""
import json
import urllib.parse

import httpretty

import deuceclient.client.deuce
import deuceclient.api as api
from deuceclient.tests import *


class ClientDeuceBlockTests(ClientTestBase):

    def setUp(self):
        super(ClientDeuceBlockTests, self).setUp()

        self.client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                           self.apihost,
                                                           sslenabled=True)

    def tearDown(self):
        super(ClientDeuceBlockTests, self).tearDown()

    @httpretty.activate
    def test_block_list(self):
        data = [block[0] for block in create_blocks(block_count=1)]
        expected_data = json.dumps(data)

        httpretty.register_uri(httpretty.GET,
                               get_blocks_url(self.apihost,
                                              self.vault.vault_id),
                               content_type='application/json',
                               body=expected_data,
                               status=200)

        self.assertTrue(self.client.GetBlockList(self.vault))
        self.assertIsNone(self.vault.blocks.marker)
        for block_id in data:
            self.assertIn(block_id, self.vault.blocks)

    @httpretty.activate
    def test_block_list_with_next_batch(self):
        data = [block[0] for block in create_blocks(block_count=1)]
        expected_data = json.dumps(data)

        url = get_blocks_url(self.apihost, self.vault.vault_id)
        url_params = urllib.parse.urlencode({'marker': data[0]})
        next_batch = '{0}?{1}'.format(url, url_params)
        httpretty.register_uri(httpretty.GET,
                               url,
                               content_type='application/json',
                               adding_headers={
                                   'x-next-batch': next_batch
                               },
                               body=expected_data,
                               status=200)

        self.assertTrue(self.client.GetBlockList(self.vault))
        self.assertIsNotNone(self.vault.blocks.marker)
        self.assertEqual(self.vault.blocks.marker, data[0])
        for block_id in data:
            self.assertIn(block_id, self.vault.blocks)

    @httpretty.activate
    def test_block_list_with_marker(self):
        block_id, block_data, block_size = create_block()

        data = [block[0] for block in create_blocks(block_count=1)]
        expected_data = json.dumps(data)

        httpretty.register_uri(httpretty.GET,
                               get_blocks_url(self.apihost,
                                              self.vault.vault_id),
                               content_type='application/json',
                               body=expected_data,
                               status=200)

        self.assertTrue(self.client.GetBlockList(self.vault, marker=block_id))
        self.assertIsNone(self.vault.blocks.marker)
        for block_id in data:
            self.assertIn(block_id, self.vault.blocks)

    @httpretty.activate
    def test_block_list_with_marker_and_limit(self):
        block_id, block_data, block_size = create_block()

        data = [block[0] for block in create_blocks(block_count=5)]
        expected_data = json.dumps(data)

        httpretty.register_uri(httpretty.GET,
                               get_blocks_url(self.apihost,
                                              self.vault.vault_id),
                               content_type='application/json',
                               body=expected_data,
                               status=200)

        self.assertTrue(self.client.GetBlockList(self.vault,
                                                 marker=block_id,
                                                 limit=5))
        self.assertEqual(len(data), len(self.vault.blocks))
        self.assertIsNone(self.vault.blocks.marker)
        for block_id in data:
            self.assertIn(block_id, self.vault.blocks)

    @httpretty.activate
    def test_block_list_with_limit(self):
        data = [block[0] for block in create_blocks(block_count=5)]
        expected_data = json.dumps(data)

        httpretty.register_uri(httpretty.GET,
                               get_blocks_url(self.apihost,
                                              self.vault.vault_id),
                               content_type='application/json',
                               body=expected_data,
                               status=200)

        self.assertTrue(self.client.GetBlockList(self.vault, limit=5))
        self.assertIsNone(self.vault.blocks.marker)
        self.assertEqual(5, len(self.vault.blocks))
        self.assertEqual(len(data), len(self.vault.blocks))
        for block_id in data:
            self.assertIn(block_id, self.vault.blocks)

    def test_block_list_bad_vault(self):
        data = [block[0] for block in create_blocks(block_count=1)]
        expected_data = json.dumps(data)

        with self.assertRaises(TypeError):
            self.client.GetBlockList(self.vault.vault_id)

    @httpretty.activate
    def test_block_list_failure(self):
        httpretty.register_uri(httpretty.GET,
                               get_blocks_url(self.apihost,
                                              self.vault.vault_id),
                               content_type='text/plain',
                               body="mock failure",
                               status=404)

        with self.assertRaises(RuntimeError) as stats_error:
            self.client.GetBlockList(self.vault)

    @httpretty.activate
    def test_blocks_upload(self):
        blocks = []
        for block_id, blockdata, block_size in [create_block()
                                                for _ in range(5)]:
            blocks.append(block_id)
            a_block = api.Block(project_id=self.vault.project_id,
                                vault_id=self.vault.vault_id,
                                block_id=block_id,
                                data=blockdata)
            self.vault.blocks[block_id] = a_block

        httpretty.register_uri(httpretty.POST,
                               get_blocks_url(self.apihost,
                                              self.vault.vault_id),
                               status=201)

        self.assertTrue(self.client.UploadBlocks(self.vault, blocks))

    def test_blocks_upload_no_blocks_in_vault(self):
        blocks = []
        for block_id, blockdata, block_size in [create_block()
                                                for _ in range(5)]:
            blocks.append(block_id)

        with self.assertRaises(KeyError):
            self.client.UploadBlocks(self.vault, blocks)

    @httpretty.activate
    def test_blocks_upload_failed(self):
        blocks = []
        for block_id, blockdata, block_size in [create_block()
                                                for _ in range(5)]:
            blocks.append(block_id)
            a_block = api.Block(project_id=self.vault.project_id,
                                vault_id=self.vault.vault_id,
                                block_id=block_id,
                                data=blockdata)
            self.vault.blocks[block_id] = a_block

        httpretty.register_uri(httpretty.POST,
                               get_blocks_url(self.apihost,
                                              self.vault.vault_id),
                               status=404)

        with self.assertRaises(RuntimeError):
            self.client.UploadBlocks(self.vault, blocks)

    @httpretty.activate
    def test_block_upload(self):
        block_id, blockdata, block_size = create_block()
        block = api.Block(project_id=self.vault.project_id,
                          vault_id=self.vault.vault_id,
                          block_id=block_id,
                          data=blockdata)

        httpretty.register_uri(httpretty.PUT,
                               get_block_url(self.apihost,
                                             self.vault.vault_id,
                                             block_id),
                               status=201)

        self.assertTrue(self.client.UploadBlock(self.vault, block))

    def test_block_upload_bad_vault(self):
        blockid, blockdata, block_size = create_block()
        block = api.Block(project_id=self.vault.project_id,
                          vault_id=self.vault.vault_id,
                          block_id=blockid,
                          data=blockdata)

        with self.assertRaises(TypeError):
            self.client.UploadBlock(self.vault.vault_id, block)

    def test_block_upload_bad_block(self):
        blockid, blockdata, block_size = create_block()
        block = api.Block(project_id=self.vault.project_id,
                          vault_id=self.vault.vault_id,
                          block_id=blockid,
                          data=blockdata)

        with self.assertRaises(TypeError):
            self.client.UploadBlock(self.vault, block.block_id)

    @httpretty.activate
    def test_block_upload_failed(self):
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
            self.client.UploadBlock(self.vault, block)

    @httpretty.activate
    def test_block_list_deletion(self):
        count = 5
        for blockid, blockdata, block_size in [create_block()
                                               for _ in range(count)]:
            self.vault.blocks.add(api.Block(project_id=self.vault.project_id,
                                            vault_id=self.vault.vault_id,
                                            block_id=blockid,
                                            data=blockdata))
        self.assertEqual(len(self.vault.blocks), count)
        self.assertEqual(len(self.vault.blocks.keys()), count)

        [httpretty.register_uri(httpretty.DELETE,
                                get_block_url(self.apihost,
                                              self.vault.vault_id,
                                              block_id),
                                status=204) for block_id in
            self.vault.blocks.keys()]

        results = self.client.DeleteBlocks(self.vault,
                                           self.vault.blocks.keys())
        self.assertEqual(len(results), count)
        for block_id, r in results:
            self.assertTrue(r)

    @httpretty.activate
    def test_block_list_deletion_failed(self):
        count = 5
        for blockid, blockdata, block_size in [create_block()
                                               for _ in range(count)]:
            self.vault.blocks.add(api.Block(project_id=self.vault.project_id,
                                            vault_id=self.vault.vault_id,
                                            block_id=blockid,
                                            data=blockdata))
        self.assertEqual(len(self.vault.blocks), count)
        self.assertEqual(len(self.vault.blocks.keys()), count)

        [httpretty.register_uri(httpretty.DELETE,
                                get_block_url(self.apihost,
                                              self.vault.vault_id,
                                              block_id),
                                status=404) for block_id in
            self.vault.blocks.keys()]

        results = self.client.DeleteBlocks(self.vault,
                                           self.vault.blocks.keys())
        self.assertEqual(len(results), count)
        for block_id, r in results:
            self.assertFalse(r)

    @httpretty.activate
    def test_block_list_deletion_mixed(self):
        count = 5
        for blockid, blockdata, block_size in [create_block()
                                               for _ in range(count)]:
            self.vault.blocks.add(api.Block(project_id=self.vault.project_id,
                                            vault_id=self.vault.vault_id,
                                            block_id=blockid,
                                            data=blockdata))
        self.assertEqual(len(self.vault.blocks), count)
        self.assertEqual(len(self.vault.blocks.keys()), count)

        expected_results = {}
        count = 0
        for block_id in self.vault.blocks.keys():
            if count % 2 == 0:
                httpretty.register_uri(httpretty.DELETE,
                                    get_block_url(self.apihost,
                                                  self.vault.vault_id,
                                                  block_id),
                                    status=204)
                expected_results[block_id] = True
            else:
                httpretty.register_uri(httpretty.DELETE,
                                    get_block_url(self.apihost,
                                                  self.vault.vault_id,
                                                  block_id),
                                    status=404)
                expected_results[block_id] = False
            count = count + 1

        results = self.client.DeleteBlocks(self.vault,
                                           self.vault.blocks.keys())
        self.assertEqual(len(results), count)
        for block_id, r in results:
            self.assertEqual(r, expected_results[block_id])

    @httpretty.activate
    def test_block_deletion(self):
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

        self.assertTrue(self.client.DeleteBlock(self.vault, block))

    def test_block_deletion_bad_vault(self):
        blockid, blockdata, block_size = create_block()
        block = api.Block(project_id=self.vault.project_id,
                          vault_id=self.vault.vault_id,
                          block_id=blockid,
                          data=blockdata)

        with self.assertRaises(TypeError):
            self.client.DeleteBlock(self.vault.vault_id, block)

    def test_block_deletion_bad_block(self):
        blockid, blockdata, block_size = create_block()
        block = api.Block(project_id=self.vault.project_id,
                          vault_id=self.vault.vault_id,
                          block_id=blockid,
                          data=blockdata)

        with self.assertRaises(TypeError):
            self.client.DeleteBlock(self.vault, block.block_id)

    @httpretty.activate
    def test_block_deletion_failed(self):
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
            self.client.DeleteBlock(self.vault, block)

    @httpretty.activate
    def test_block_download(self):
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

        self.assertTrue(self.client.DownloadBlock(self.vault, block))
        self.assertEqual(block.data, blockdata)

    def test_block_download_bad_vault(self):
        blockid, blockdata, block_size = create_block()
        block = api.Block(project_id=self.vault.project_id,
                          vault_id=self.vault.vault_id,
                          block_id=blockid)

        with self.assertRaises(TypeError):
            self.client.DownloadBlock(self.vault.vault_id, block)

    def test_block_download_bad_block(self):
        blockid, blockdata, block_size = create_block()
        block = api.Block(project_id=self.vault.project_id,
                          vault_id=self.vault.vault_id,
                          block_id=blockid)

        with self.assertRaises(TypeError):
            self.client.DownloadBlock(self.vault, block.block_id)

    @httpretty.activate
    def test_block_download_failed(self):
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
            self.client.DownloadBlock(self.vault, block)
