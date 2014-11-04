"""
Tests - Deuce Client - Client - Deuce - Storage Block
"""
import json

import httpretty

import deuceclient.client.deuce
import deuceclient.api as api
from deuceclient.tests import *


class ClientDeuceStorageBlockTests(ClientTestBase):

    def setUp(self):
        super(self.__class__, self).setUp()

    def tearDown(self):
        super(self.__class__, self).tearDown()

    @httpretty.activate
    def test_storage_block_download(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        storage_blockid = create_storage_block()
        blockid = hashlib.sha1(b'mock').hexdigest()
        httpretty.register_uri(httpretty.GET,
                               get_storage_block_url(self.apihost,
                                                     self.vault.vault_id,
                                                     storage_blockid),
                               content_type='application/octet-stream',
                               body="mock",
                               adding_headers={
                                   'x-block-reference-count': 2,
                                   'x-ref-modified': datetime.datetime.max,
                                   'x-storage-id': storage_blockid,
                                   'x-block-id': blockid,
                               },
                               status=200)
        block_before = api.Block(project_id=create_project_name(),
                                 vault_id=create_vault_name(),
                                 storage_id=storage_blockid,
                                 block_type='storage')
        block = client.DownloadBlockStorageData(
            self.vault,
            block_before)
        self.assertEqual(block.data, b"mock")
        self.assertEqual(block.ref_count, '2')
        self.assertEqual(block.ref_modified, str(datetime.datetime.max))
        self.assertEqual(block.storage_id, storage_blockid)
        self.assertEqual(block.block_id, blockid)

    @httpretty.activate
    def test_non_existent_storage_block_download(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        storage_blockid = create_storage_block()

        httpretty.register_uri(httpretty.GET,
                               get_storage_block_url(self.apihost,
                                                     self.vault.vault_id,
                                                     storage_blockid),
                               content_type='application/octet-stream',
                               body="mock",
                               status=404)
        block = api.Block(project_id=create_project_name(),
                          vault_id=create_vault_name(),
                          storage_id=storage_blockid,
                          block_type='storage')
        with self.assertRaises(RuntimeError) as deletion_error:
            client.DownloadBlockStorageData(self.vault, block)

    @httpretty.activate
    def test_storage_block_list(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        data = [create_storage_block() for _ in range(10)]
        expected_data = json.dumps(data)
        httpretty.register_uri(httpretty.GET,
                               get_storage_blocks_url(self.apihost,
                                                      self.vault.vault_id),
                               content_type='application/octet-stream',
                               body=expected_data,
                               status=200)
        blocks = client.GetBlockStorageList(self.vault)
        self.assertEqual(set(blocks.keys()), set(data))

    @httpretty.activate
    def test_storage_block_list_error(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        httpretty.register_uri(httpretty.GET,
                               get_storage_blocks_url(self.apihost,
                                                      self.vault.vault_id),
                               status=500)

        with self.assertRaises(RuntimeError):
            client.GetBlockStorageList(self.vault)

    @httpretty.activate
    def test_storage_block_list_with_marker(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)
        block = create_block()
        data = sorted([create_storage_block(block[0])
                       for _ in range(3)])
        expected_data = json.dumps(data)
        httpretty.register_uri(httpretty.GET,
                               get_storage_blocks_url(self.apihost,
                                                      self.vault.vault_id),
                               content_type='application/octet-stream',
                               body=expected_data,
                               status=200)
        blocks = client.GetBlockStorageList(self.vault,
            marker=data[0])
        self.assertEqual(set(blocks.keys()), set(data))

    @httpretty.activate
    def test_storage_block_list_with_limit(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)
        block = create_block()
        data = sorted([create_storage_block(block[0])
                       for _ in range(5)])
        expected_data = json.dumps(data)
        httpretty.register_uri(httpretty.GET,
                               get_storage_blocks_url(self.apihost,
                                                      self.vault.vault_id),
                               content_type='application/octet-stream',
                               body=expected_data,
                               status=200)

        blocks = client.GetBlockStorageList(self.vault,
                                            limit=5)
        self.assertEqual(set(blocks.keys()), set(data))

    @httpretty.activate
    def test_storage_block_list_with_limit_and_marker(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)
        block = create_block()
        data = sorted([create_storage_block(block[0])
                       for _ in range(3)])
        expected_data = json.dumps(data)
        httpretty.register_uri(httpretty.GET,
                               get_storage_blocks_url(self.apihost,
                                                      self.vault.vault_id),
                               content_type='application/octet-stream',
                               body=expected_data,
                               status=200)

        blocks = client.GetBlockStorageList(self.vault,
            limit=3,
            marker=data[0])
        self.assertEqual(set(blocks.keys()), set(data))

    @httpretty.activate
    def test_head_storage_block_non_existent(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        storage_blockid = create_storage_block()
        httpretty.register_uri(httpretty.HEAD,
                               get_storage_block_url(self.apihost,
                                                     self.vault.vault_id,
                                                     storage_blockid),
                               status=404)
        block = api.Block(project_id=create_project_name(),
                          vault_id=create_vault_name(),
                          storage_id=storage_blockid,
                          block_type='storage')
        with self.assertRaises(RuntimeError):
            client.HeadBlockStorage(self.vault, block)

    @httpretty.activate
    def test_head_storage_block(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        storage_blockid = create_storage_block()
        blockid = hashlib.sha1(b'mock').hexdigest()
        httpretty.register_uri(httpretty.HEAD,
                               get_storage_block_url(self.apihost,
                                                     self.vault.vault_id,
                                                     storage_blockid),
                               content_type='application/octet-stream',
                               adding_headers={
                                   'x-block-reference-count': 2,
                                   'x-ref-modified': datetime.datetime.max,
                                   'x-storage-id': storage_blockid,
                                   'x-block-id': blockid,
                                   'x-block-size': 200,
                                   'x-block-orphaned': True
                               },
                               status=204)
        block_before = api.Block(project_id=create_project_name(),
                                 vault_id=create_vault_name(),
                                 storage_id=storage_blockid,
                                 block_type='storage')
        block = client.HeadBlockStorage(self.vault, block_before)
        self.assertEqual(block.ref_count, '2')
        self.assertEqual(block.ref_modified, str(datetime.datetime.max))
        self.assertEqual(block.storage_id, storage_blockid)
        self.assertEqual(block.block_id, blockid)
        self.assertEqual(block.block_size, '200')
        self.assertTrue(block.block_orphaned)

    @httpretty.activate
    def test_delete_storage_block(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        storage_blockid = create_storage_block()
        httpretty.register_uri(httpretty.DELETE,
                               get_storage_block_url(self.apihost,
                                                     self.vault.vault_id,
                                                     storage_blockid),
                               status=204)
        block = api.Block(project_id=create_project_name(),
                          vault_id=create_vault_name(),
                          storage_id=storage_blockid,
                          block_type='storage')
        self.assertTrue(True, client.DeleteBlockStorage(self.vault,
                                                        block))

    @httpretty.activate
    def test_delete_storage_block_non_existent(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        storage_blockid = create_storage_block()
        httpretty.register_uri(httpretty.DELETE,
                               get_storage_block_url(self.apihost,
                                                     self.vault.vault_id,
                                                     storage_blockid),
                               status=404)
        block = api.Block(project_id=create_project_name(),
                          vault_id=create_vault_name(),
                          storage_id=storage_blockid,
                          block_type='storage')
        with self.assertRaises(RuntimeError):
            client.DeleteBlockStorage(self.vault, block)
