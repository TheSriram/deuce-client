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

        data = [block[0] for block in create_blocks(block_count=1)]
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
        for block_id in data:
            self.assertIn(block_id, self.vault.blocks)

    @httpretty.activate
    def test_block_list_with_limit(self):
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

        self.assertTrue(client.GetBlockList(self.vault, limit=5))
        for block_id in data:
            self.assertIn(block_id, self.vault.blocks)

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
            client.GetBlockData(self.vault, blockid)

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
