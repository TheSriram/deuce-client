"""
Tests - Deuce Client - Client - Deuce
"""
import hashlib
import json
import os
import random
from unittest import TestCase
import uuid

import httpretty
import mock

import deuceclient
import deuceclient.api as api
import deuceclient.api.vault as api_vault
import deuceclient.client.deuce
from deuceclient.common import errors
from deuceclient.tests import *


class ClientTest(TestCase):

    def setUp(self):
        super(self.__class__, self).setUp()
        self.deuceclient_version = deuceclient.version()
        self.apihost = 'deuce-api-test'
        self.uripath = '/'
        self.expected_agent = 'Deuce-Client/{0:}'.format(
            self.deuceclient_version)
        self.expected_uri = "https://" + self.apihost + self.uripath
        self.authenticator = FakeAuthenticator(userid='cheshirecat',
                                               usertype='username',
                                               credentials='alice',
                                               auth_method='password',
                                               datacenter='wonderland',
                                               auth_url='down.the.rabbit.hole')

        self.vault = api_vault.Vault(create_project_name(),
                                     create_vault_name())

    @property
    def vault_name(self):
        return self.vault.vault_id

    def tearDown(self):
        super(self.__class__, self).tearDown()

    def test_init_ssl_correct_uri(self):

        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)
        self.assertIn('X-Deuce-User-Agent', client.headers)
        self.assertIn('User-Agent', client.headers)
        self.assertEqual(self.expected_agent,
                         client.headers['X-Deuce-User-Agent'])
        self.assertEqual(self.expected_agent,
                         client.headers['User-Agent'])
        self.assertEqual(client.headers['X-Deuce-User-Agent'],
                         client.headers['User-Agent'])

        self.assertEqual(self.expected_uri,
                         client.uri)
        self.assertEqual(self.authenticator.AuthTenantId,
                         client.ProjectId)

    @httpretty.activate
    def test_create_vault(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        httpretty.register_uri(httpretty.PUT,
                               get_vault_url(
                                   self.apihost,
                                   self.vault.vault_id),
                               status=201)

        self.assertTrue(client.CreateVault(self.vault.vault_id))

    @httpretty.activate
    def test_create_vault_with_api_vault(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        httpretty.register_uri(httpretty.PUT,
                               get_vault_url(
                                   self.apihost,
                                   self.vault.vault_id),
                               status=201)

        self.assertTrue(client.CreateVault(self.vault))

    @httpretty.activate
    def test_create_vault_failed(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        httpretty.register_uri(httpretty.PUT,
                               get_vault_url(
                                   self.apihost,
                                   self.vault.vault_id),
                               content_type='text/plain',
                               body="mock failure",
                               status=404)

        with self.assertRaises(RuntimeError) as creation_error:
            client.CreateVault(self.vault)

    @httpretty.activate
    def test_delete_vault(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        httpretty.register_uri(httpretty.DELETE,
                               get_vault_url(self.apihost,
                                             self.vault.vault_id),
                               status=204)
        self.assertTrue(client.DeleteVault(self.vault))

    @httpretty.activate
    def test_delete_vault_failed(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        httpretty.register_uri(httpretty.DELETE,
                               get_vault_url(self.apihost,
                                             self.vault.vault_id),
                               content_type='text/plain',
                               body="mock failure",
                               status=404)

        with self.assertRaises(RuntimeError) as deletion_error:
            client.DeleteVault(self.vault)

    @httpretty.activate
    def test_vault_exists(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)
        expected_result = {'status': True}

        httpretty.register_uri(httpretty.HEAD,
                               get_vault_url(self.apihost,
                                             self.vault.vault_id),
                               status=204)

        self.assertTrue(client.VaultExists(self.vault))

    @httpretty.activate
    def test_vault_does_not_exist(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)
        expected_result = {'status': True}

        httpretty.register_uri(httpretty.HEAD,
                               get_vault_url(self.apihost,
                                             self.vault.vault_id),
                               status=404)

        self.assertFalse(client.VaultExists(self.vault))

    @httpretty.activate
    def test_vault_exists_failure(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)
        httpretty.register_uri(httpretty.HEAD,
                               get_vault_url(self.apihost,
                                             self.vault.vault_id),
                               content_type='text/plain',
                               body="mock failure",
                               status=401)

        with self.assertRaises(RuntimeError) as exists_error:
            client.VaultExists(self.vault)

    @httpretty.activate
    def test_vault_statistics(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        data = {'status': True}
        expected_body = json.dumps(data)

        httpretty.register_uri(httpretty.GET,
                               get_vault_url(self.apihost,
                                             self.vault.vault_id),
                               content_type='application/json',
                               body=expected_body,
                               status=200)

        self.assertEqual(data,
                         client.GetVaultStatistics(self.vault))

    @httpretty.activate
    def test_vault_statistics_failure(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        httpretty.register_uri(httpretty.GET,
                               get_vault_url(self.apihost,
                                             self.vault.vault_id),
                               content_type='text/plain',
                               body="mock failure",
                               status=404)

        with self.assertRaises(RuntimeError) as stats_error:
            client.GetVaultStatistics(self.vault)

    @httpretty.activate
    def test_block_list(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        data = {'list': 'my list'}
        expected_data = json.dumps(data)

        httpretty.register_uri(httpretty.GET,
                               get_blocks_url(self.apihost,
                                              self.vault.vault_id),
                               content_type='application/json',
                               body=expected_data,
                               status=200)

        self.assertEqual(data,
                         client.GetBlockList(self.vault))

    @httpretty.activate
    def test_block_list_with_marker(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        block_id, block_data, block_size = create_block()

        data = {'list': 'my list'}
        expected_data = json.dumps(data)

        httpretty.register_uri(httpretty.GET,
                               get_blocks_url(self.apihost,
                                              self.vault.vault_id),
                               content_type='application/json',
                               body=expected_data,
                               status=200)

        self.assertEqual(data,
                         client.GetBlockList(self.vault, marker=block_id))

    @httpretty.activate
    def test_block_list_with_marker_and_limit(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        block_id, block_data, block_size = create_block()

        data = {'list': 'my list'}
        expected_data = json.dumps(data)

        httpretty.register_uri(httpretty.GET,
                               get_blocks_url(self.apihost,
                                              self.vault.vault_id),
                               content_type='application/json',
                               body=expected_data,
                               status=200)

        self.assertEqual(data,
                         client.GetBlockList(self.vault,
                                             marker=block_id,
                                             limit=5))

    @httpretty.activate
    def test_block_list_with_limit(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        data = {'list': 'my list'}
        expected_data = json.dumps(data)

        httpretty.register_uri(httpretty.GET,
                               get_blocks_url(self.apihost,
                                              self.vault.vault_id),
                               content_type='application/json',
                               body=expected_data,
                               status=200)

        self.assertEqual(data,
                         client.GetBlockList(self.vault, limit=5))

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

        httpretty.register_uri(httpretty.PUT,
                               get_block_url(self.apihost,
                                             self.vault.vault_id,
                                             blockid),
                               status=201)

        self.assertTrue(client.UploadBlock(self.vault,
                                           blockid,
                                           blockdata,
                                           block_size))

    @httpretty.activate
    def test_block_upload_failed(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        blockid, blockdata, block_size = create_block()

        httpretty.register_uri(httpretty.PUT,
                               get_block_url(self.apihost,
                                             self.vault.vault_id,
                                             blockid),
                               status=404)

        with self.assertRaises(RuntimeError) as upload_error:
            client.UploadBlock(self.vault, blockid, blockdata, block_size)

    @httpretty.activate
    def test_block_deletion(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        blockid, blockdata, block_size = create_block()

        httpretty.register_uri(httpretty.DELETE,
                               get_block_url(self.apihost,
                                             self.vault.vault_id,
                                             blockid),
                               status=204)

        self.assertTrue(client.DeleteBlock(self.vault, blockid))

    @httpretty.activate
    def test_block_deletion_failed(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        blockid, blockdata, block_size = create_block()

        httpretty.register_uri(httpretty.DELETE,
                               get_block_url(self.apihost,
                                             self.vault.vault_id,
                                             blockid),
                               content_type='text/plain',
                               body="mock failure",
                               status=404)

        with self.assertRaises(RuntimeError) as deletion_error:
            client.DeleteBlock(self.vault, blockid)

    @httpretty.activate
    def test_block_download(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        blockid, blockdata, block_size = create_block()

        httpretty.register_uri(httpretty.GET,
                               get_block_url(self.apihost,
                                             self.vault.vault_id,
                                             blockid),
                               content_type='text/plain',
                               body=blockdata,
                               status=200)

        data = client.GetBlockData(self.vault, blockid)
        self.assertEqual(data, blockdata)

    @httpretty.activate
    def test_block_download_failed(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        blockid, blockdata, block_size = create_block()

        httpretty.register_uri(httpretty.GET,
                               get_block_url(self.apihost,
                                             self.vault.vault_id,
                                             blockid),
                               content_type='text/plain',
                               body="mock failure",
                               status=404)

        with self.assertRaises(RuntimeError) as deletion_error:
            client.GetBlockData(self.vault.vault_id, blockid)

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

        block = client.DownloadBlockStorageData(
            self.vault,
            storage_blockid)
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

        with self.assertRaises(RuntimeError) as deletion_error:
            client.DownloadBlockStorageData(self.vault, storage_blockid)

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
        uuids = ['09f074cd-36db-4a1f-9d09-ecf64dbe4fdc',
                 '09f074cd-36db-4a1f-9d09-ecf64dbe4fdd',
                 '09f074cd-36db-4a1f-9d09-ecf64dbe4fde']
        data = [block[0] + '_' + uuid for uuid in uuids]
        expected_data = json.dumps(data)
        httpretty.register_uri(httpretty.GET,
                               get_storage_blocks_url(self.apihost,
                                                      self.vault.vault_id),
                               content_type='application/octet-stream',
                               body=expected_data,
                               status=200)
        blocks = client.GetBlockStorageList(self.vault,
            marker=block[0] + '_' + '09f074cd-36db-4a1f-9d09-ecf64dbe4fdc')
        self.assertEqual(set(blocks.keys()), set(data))

    @httpretty.activate
    def test_storage_block_list_with_limit(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)
        block = create_block()
        uuids = ['09f074cd-36db-4a1f-9d09-ecf64dbe4fdc',
                 '09f074cd-36db-4a1f-9d09-ecf64dbe4fdd',
                 '09f074cd-36db-4a1f-9d09-ecf64dbe4fde',
                 '09f074cd-36db-4a1f-9d09-ecf64dbe4fdf',
                 '09f074cd-36db-4a1f-9d09-ecf64dbe4fe0']
        data = [block[0] + '_' + uuid for uuid in uuids]
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
        uuids = ['09f074cd-36db-4a1f-9d09-ecf64dbe4fde',
                '09f074cd-36db-4a1f-9d09-ecf64dbe4fdf',
                '09f074cd-36db-4a1f-9d09-ecf64dbe4fe0']
        data = [block[0] + '_' + uuid for uuid in uuids]
        expected_data = json.dumps(data)
        httpretty.register_uri(httpretty.GET,
                               get_storage_blocks_url(self.apihost,
                                                      self.vault.vault_id),
                               content_type='application/octet-stream',
                               body=expected_data,
                               status=200)

        blocks = client.GetBlockStorageList(self.vault,
            limit=3,
            marker=block[0] + '_' + '09f074cd-36db-4a1f-9d09-ecf64dbe4fde')
        self.assertEqual(set(blocks.keys()), set(data))

    @httpretty.activate
    def test_head_storage_block_non_existant(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        storage_blockid = create_storage_block()
        httpretty.register_uri(httpretty.HEAD,
                               get_storage_block_url(self.apihost,
                                                     self.vault.vault_id,
                                                     storage_blockid),
                               status=404)
        with self.assertRaises(RuntimeError):
            client.HeadBlockStorage(self.vault, storage_blockid)

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

        block = client.HeadBlockStorage(self.vault, storage_blockid)
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
        self.assertTrue(True, client.DeleteBlockStorage(self.vault,
                                                        storage_blockid))

    @httpretty.activate
    def test_delete_storage_block_non_existant(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        storage_blockid = create_storage_block()
        httpretty.register_uri(httpretty.DELETE,
                               get_storage_block_url(self.apihost,
                                                     self.vault.vault_id,
                                                     storage_blockid),
                               status=404)
        with self.assertRaises(RuntimeError):
            client.DeleteBlockStorage(self.vault, storage_blockid)

    @httpretty.activate
    def test_file_creation(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        file_id = create_file()
        file_url = get_file_url(self.apihost, self.vault.vault_id, file_id)

        httpretty.register_uri(httpretty.POST,
                               get_files_url(self.apihost,
                                             self.vault.vault_id),
                               adding_headers={
                                   'location': file_url
                               },
                               status=201)

        self.assertEqual(file_url, client.CreateFile(self.vault))

    @httpretty.activate
    def test_file_creation_missing_location_header(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        file_id = create_file()
        file_url = get_file_url(self.apihost, self.vault.vault_id, file_id)

        httpretty.register_uri(httpretty.POST,
                               get_files_url(self.apihost,
                                             self.vault.vault_id),
                               status=201)

        with self.assertRaises(KeyError) as creation_error:
            client.CreateFile(self.vault)

    @httpretty.activate
    def test_file_creation_failed(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        file_id = create_file()
        file_url = get_file_url(self.apihost, self.vault.vault_id, file_id)

        httpretty.register_uri(httpretty.POST,
                               get_files_url(self.apihost,
                                             self.vault.vault_id),
                               status=404)

        with self.assertRaises(RuntimeError) as creation_error:
            client.CreateFile(self.vault)

    @httpretty.activate
    def test_file_blocks_get(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        file_id = create_file()
        data = {'list': 'my list'}

        httpretty.register_uri(httpretty.GET,
                               get_file_blocks_url(self.apihost,
                                                   self.vault.vault_id,
                                                   file_id),
                               body=json.dumps(data),
                               status=200)

        self.assertEqual(data,
                         client.GetFileBlockList(self.vault, file_id))

    @httpretty.activate
    def test_file_blocks_get_with_marker(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        block_id, block_data, block_size = create_block()
        file_id = create_file()
        data = {'list': 'my list'}

        httpretty.register_uri(httpretty.GET,
                               get_file_blocks_url(self.apihost,
                                                   self.vault.vault_id,
                                                   file_id),
                               body=json.dumps(data),
                               status=200)

        self.assertEqual(data,
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
        data = {'list': 'my list'}

        httpretty.register_uri(httpretty.GET,
                               get_file_blocks_url(self.apihost,
                                                   self.vault.vault_id,
                                                   file_id),
                               body=json.dumps(data),
                               status=200)

        self.assertEqual(data,
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
        data = {'list': 'my list'}

        httpretty.register_uri(httpretty.GET,
                               get_file_blocks_url(self.apihost,
                                                   self.vault.vault_id,
                                                   file_id),
                               body=json.dumps(data),
                               status=200)

        self.assertEqual(data,
                         client.GetFileBlockList(self.vault,
                                                 file_id,
                                                 limit=5))

    @httpretty.activate
    def test_file_blocks_get_filed(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        file_id = create_file()
        data = {'list': 'my list'}

        httpretty.register_uri(httpretty.GET,
                               get_file_blocks_url(self.apihost,
                                                   self.vault.vault_id,
                                                   file_id),
                               body=json.dumps(data),
                               status=404)

        with self.assertRaises(RuntimeError) as stats_error:
            client.GetFileBlockList(self.vault, file_id)

    @httpretty.activate
    def test_file_assign_blocks(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        file_id = create_file()

        block_list = {'blocks': []}

        running_offset = 0
        for block_id, block_data, block_size in \
                create_blocks(5):
            block_list['blocks'].append({
                'id': block_id,
                'size': block_size,
                'offset': running_offset
            })
            running_offset = running_offset + block_size

        data = {'status': True}

        httpretty.register_uri(httpretty.POST,
                               get_file_url(self.apihost,
                                            self.vault.vault_id,
                                            file_id),
                               body=json.dumps(data),
                               status=200)

        self.assertEqual(data,
                         client.AssignBlocksToFile(self.vault,
                                                   file_id,
                                                   block_list))

    @httpretty.activate
    def test_file_assign_blocks_failed(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        file_id = create_file()

        block_list = {'blocks': []}

        running_offset = 0
        for block_id, block_data, block_size in create_blocks(5):
            block_list['blocks'].append({
                'id': block_id,
                'size': block_size,
                'offset': running_offset
            })
            running_offset = running_offset + block_size

        data = {'status': True}

        httpretty.register_uri(httpretty.POST,
                               get_file_url(self.apihost,
                                            self.vault.vault_id,
                                            file_id),
                               content_type='text/plain',
                               body="mock failure",
                               status=404)

        with self.assertRaises(RuntimeError) as stats_error:
            client.AssignBlocksToFile(self.vault, file_id, block_list)
