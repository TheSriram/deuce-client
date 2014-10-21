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
                         client.project_id)

    @httpretty.activate
    def test_create_vault(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        httpretty.register_uri(httpretty.PUT,
                            get_vault_url(self.apihost, self.vault.vault_id),
                            status=201)

        vault = client.CreateVault(self.vault.vault_id)
        self.assertEqual(vault.vault_id, self.vault.vault_id)
        self.assertEqual(vault.status, "created")

    def test_create_vault_invalid_parameter(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        with self.assertRaises(TypeError) as creation_error:
            client.CreateVault(self.vault)

    @httpretty.activate
    def test_create_vault_failed(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        httpretty.register_uri(httpretty.PUT,
                            get_vault_url(self.apihost, self.vault.vault_id),
                            content_type='text/plain',
                            body="mock failure",
                            status=404)

        with self.assertRaises(RuntimeError) as creation_error:
            client.CreateVault(self.vault.vault_id)

    @httpretty.activate
    def test_get_vault(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        httpretty.register_uri(httpretty.HEAD,
                               get_vault_url(self.apihost,
                                             self.vault.vault_id),
                               status=204)
        vault = client.GetVault(self.vault.vault_id)
        self.assertEqual(vault.vault_id,
                         self.vault.vault_id)
        self.assertEqual(vault.status, "valid")

    def test_get_vault_failed_bad_parameter(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        with self.assertRaises(TypeError):
            client.GetVault(self.vault)

    @httpretty.activate
    def test_get_vault_failed(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        httpretty.register_uri(httpretty.HEAD,
                               get_vault_url(self.apihost,
                                             self.vault.vault_id),
                               status=404)
        with self.assertRaises(RuntimeError):
            client.GetVault(self.vault.vault_id)

    @httpretty.activate
    def test_delete_vault_by_api_vault(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        httpretty.register_uri(httpretty.DELETE,
                               get_vault_url(self.apihost,
                                             self.vault.vault_id),
                               status=204)
        self.assertTrue(client.DeleteVault(self.vault))

    @httpretty.activate
    def test_delete_vault_by_vault_name(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        with self.assertRaises(TypeError):
            client.DeleteVault(self.vault.vault_id)

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
    def test_vault_exists_by_api_vault(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)
        httpretty.register_uri(httpretty.HEAD,
                               get_vault_url(self.apihost,
                                             self.vault.vault_id),
                               status=204)

        self.assertTrue(client.VaultExists(self.vault))

    @httpretty.activate
    def test_vault_exists_by_vault_name(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)
        httpretty.register_uri(httpretty.HEAD,
                               get_vault_url(self.apihost,
                                             self.vault.vault_id),
                               status=204)

        self.assertTrue(client.VaultExists(self.vault.vault_id))

    @httpretty.activate
    def test_vault_does_not_exist(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)
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
    def test_vault_statistics_by_api_vault(self):
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

        self.assertTrue(client.GetVaultStatistics(self.vault))
        self.assertEqual(data, self.vault.statistics)

    def test_vault_statistics_by_vault_name(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        with self.assertRaises(TypeError):
            client.GetVaultStatistics(self.vault.vault_id)

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
                                   'location': file_url,
                                   'x-file-id': file_id
                               },
                               status=201)

        file_id = client.CreateFile(self.vault)
        self.assertIn(file_id, self.vault.files)
        self.assertEqual(file_url, self.vault.files[file_id].url)

    def test_file_creation_bad_vault(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        file_id = create_file()
        file_url = get_file_url(self.apihost, self.vault.vault_id, file_id)

        with self.assertRaises(TypeError):
            client.CreateFile(self.vault.vault_id)

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

    def test_file_blocks_get_bad_vault(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        file_id = create_file()

        with self.assertRaises(TypeError):
            client.GetFileBlockList(self.vault.vault_id, file_id)

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
    def test_file_blocks_get_failed(self):
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
        self.vault.files[file_id] = api.File(project_id=self.vault.project_id,
                                             vault_id=self.vault.vault_id,
                                             file_id=file_id)

        block_list = []

        running_offset = 0
        for block_id, block_data, block_size in \
                create_blocks(5):
            block = api.Block(project_id=self.vault.project_id,
                              vault_id=self.vault.vault_id,
                              block_id=block_id,
                              data=block_data)
            self.vault.blocks[block_id] = block
            self.vault.files[file_id].blocks[block_id] = block
            self.vault.files[file_id].offsets[running_offset] = block_id

            block_list.append((block_id, running_offset))

            running_offset = running_offset + block_size

        data = ['status'] 

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

    def test_file_assign_blocks_bad_vault(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        file_id = create_file()
        self.vault.files[file_id] = api.File(project_id=self.vault.project_id,
                                             vault_id=self.vault.vault_id,
                                             file_id=file_id)

        block_list = []

        running_offset = 0
        for block_id, block_data, block_size in \
                create_blocks(5):
            block = api.Block(project_id=self.vault.project_id,
                              vault_id=self.vault.vault_id,
                              block_id=block_id,
                              data=block_data)
            self.vault.blocks[block_id] = block
            self.vault.files[file_id].blocks[block_id] = block
            self.vault.files[file_id].offsets[running_offset] = block_id

            block_list.append((block_id, running_offset))

            running_offset = running_offset + block_size

        with self.assertRaises(TypeError):
            client.AssignBlocksToFile(self.vault.vault_id,
                                      file_id,
                                      block_list)

    @httpretty.activate
    def test_file_assign_blocks_failed(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        file_id = create_file()
        self.vault.files[file_id] = api.File(project_id=self.vault.project_id,
                                             vault_id=self.vault.vault_id,
                                             file_id=file_id)

        block_list = []

        running_offset = 0
        for block_id, block_data, block_size in create_blocks(5):
            block = api.Block(project_id=self.vault.project_id,
                              vault_id=self.vault.vault_id,
                              block_id=block_id,
                              data=block_data)
            self.vault.blocks[block_id] = block
            self.vault.files[file_id].blocks[block_id] = block
            self.vault.files[file_id].offsets[running_offset] = block_id

            block_list.append((block_id, running_offset))

            running_offset = running_offset + block_size

        httpretty.register_uri(httpretty.POST,
                               get_file_url(self.apihost,
                                            self.vault.vault_id,
                                            file_id),
                               content_type='text/plain',
                               body="mock failure",
                               status=404)

        with self.assertRaises(RuntimeError) as stats_error:
            client.AssignBlocksToFile(self.vault, file_id, block_list)
