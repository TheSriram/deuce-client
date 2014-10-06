"""
Deuce Client Tests
"""
import hashlib
import json
import os
from unittest import TestCase
import uuid

import httpretty
import mock

import deuceclient
import deuceclient.client.deuce
from deuceclient.tests import FakeAuthenticator


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

        self.vault_name = '{0}'.format(str(uuid.uuid4()))

    def get_deuce_url(self):
        return 'https://{0}/v1.0'.format(self.apihost)

    def get_vault_url(self, vault):
        return '{0}/{1}'.format(self.get_deuce_url(), vault)

    def get_blocks_url(self, vault):
        return '{0}/blocks'.format(self.get_vault_url(vault))

    def get_block_url(self, vault, block):
        return '{0}/{1}'.format(self.get_blocks_url(vault), block)

    @staticmethod
    def get_block_id(data):
        blockid_generator = hashlib.sha1()
        blockid_generator.update(data)
        return blockid_generator.hexdigest()

    @staticmethod
    def create_block(block_size=100):
        block_data = os.urandom(block_size)
        block_id = ClientTest.get_block_id(block_data)
        return (block_id, block_data)

    @staticmethod
    def create_blocks(block_count=1, block_size=100, uniform_sizes=False,
                      min_size=1, max_size=2000):
        block_sizes = []
        if uniform_sizes:
            block_sizes = [blocksize for _ in range(block_count)]
        else:
            block_sizes = [randrange(min_size, max_size)
                           for block_size in range(block_count)]

        blocks = [ClientTest.create_block(block_size)
                  for block_size in block_sizes]
        return blocks

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
                            self.get_vault_url(self.vault_name),
                            status=201)

        self.assertTrue(client.CreateVault(self.vault_name))

    @httpretty.activate
    def test_create_vault_failed(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        httpretty.register_uri(httpretty.PUT,
                            self.get_vault_url(self.vault_name),
                            body="mock failure",
                            status=404)

        with self.assertRaises(RuntimeError) as creation_error:
            client.CreateVault(self.vault_name)

    @httpretty.activate
    def test_delete_vault(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        httpretty.register_uri(httpretty.DELETE,
                               self.get_vault_url(self.vault_name),
                               status=204)
        self.assertTrue(client.DeleteVault(self.vault_name))

    @httpretty.activate
    def test_delete_vault_failed(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        httpretty.register_uri(httpretty.DELETE,
                               self.get_vault_url(self.vault_name),
                               body="mock failure",
                               status=404)

        with self.assertRaises(RuntimeError) as deletion_error:
            client.DeleteVault(self.vault_name)

    @httpretty.activate
    def test_vault_exists(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)
        expected_result = {'status': True}

        httpretty.register_uri(httpretty.HEAD,
                               self.get_vault_url(self.vault_name),
                               status=204)

        self.assertTrue(client.VaultExists(self.vault_name))

    @httpretty.activate
    def test_vault_does_not_exist(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)
        expected_result = {'status': True}

        httpretty.register_uri(httpretty.HEAD,
                               self.get_vault_url(self.vault_name),
                               status=404)

        self.assertFalse(client.VaultExists(self.vault_name))

    @httpretty.activate
    def test_vault_exists_failure(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)
        httpretty.register_uri(httpretty.HEAD,
                               self.get_vault_url(self.vault_name),
                               body="mock failure",
                               status=401)

        with self.assertRaises(RuntimeError) as exists_error:
            client.VaultExists(self.vault_name)

    @httpretty.activate
    def test_vault_statistics(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        data = {'status': True}
        expected_body = json.dumps(data)

        httpretty.register_uri(httpretty.GET,
                               self.get_vault_url(self.vault_name),
                               body=expected_body,
                               status=200)

        self.assertEqual(data,
                         client.GetVaultStatistics(self.vault_name))

    @httpretty.activate
    def test_vault_statistics_failure(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        httpretty.register_uri(httpretty.GET,
                               self.get_vault_url(self.vault_name),
                               body="mock failure",
                               status=404)

        with self.assertRaises(RuntimeError) as stats_error:
            client.GetVaultStatistics(self.vault_name)

    @httpretty.activate
    def test_block_list(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        data = {'list': 'my list'}
        expected_data = json.dumps(data)

        httpretty.register_uri(httpretty.GET,
                               self.get_blocks_url(self.vault_name),
                               body=expected_data,
                               status=200)

        self.assertEqual(data,
                         client.GetBlockList(self.vault_name))

    @httpretty.activate
    def test_block_list_with_marker(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        block_id, block_data = self.__class__.create_block()

        data = {'list': 'my list'}
        expected_data = json.dumps(data)

        httpretty.register_uri(httpretty.GET,
                               self.get_blocks_url(self.vault_name),
                               body=expected_data,
                               status=200)

        self.assertEqual(data,
                         client.GetBlockList(self.vault_name, marker=block_id))

    @httpretty.activate
    def test_block_list_with_marker_and_limit(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        block_id, block_data = self.__class__.create_block()

        data = {'list': 'my list'}
        expected_data = json.dumps(data)

        httpretty.register_uri(httpretty.GET,
                               self.get_blocks_url(self.vault_name),
                               body=expected_data,
                               status=200)

        self.assertEqual(data,
                         client.GetBlockList(self.vault_name,
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
                               self.get_blocks_url(self.vault_name),
                               body=expected_data,
                               status=200)

        self.assertEqual(data,
                         client.GetBlockList(self.vault_name, limit=5))

    @httpretty.activate
    def test_block_list_failure(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        httpretty.register_uri(httpretty.GET,
                               self.get_blocks_url(self.vault_name),
                               body="mock failure",
                               status=404)

        with self.assertRaises(RuntimeError) as stats_error:
            client.GetBlockList(self.vault_name)

    @httpretty.activate
    def test_block_upload(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        blockid, blockdata = self.__class__.create_block()

        httpretty.register_uri(httpretty.PUT,
                               self.get_block_url(self.vault_name, blockid),
                               status=201)

        self.assertTrue(client.UploadBlock(self.vault_name,
                                           blockid,
                                           blockdata))

    @httpretty.activate
    def test_block_upload_failed(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        blockid, blockdata = self.__class__.create_block()

        httpretty.register_uri(httpretty.PUT,
                               self.get_block_url(self.vault_name, blockid),
                               status=404)

        with self.assertRaises(RuntimeError) as upload_error:
            client.UploadBlock(self.vault_name, blockid, blockdata)

    @httpretty.activate
    def test_block_deletion(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        blockid, blockdata = self.__class__.create_block()

        httpretty.register_uri(httpretty.DELETE,
                               self.get_block_url(self.vault_name, blockid),
                               status=204)

        self.assertTrue(client.DeleteBlock(self.vault_name, blockid))

    @httpretty.activate
    def test_block_deletion_failed(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        blockid, blockdata = self.__class__.create_block()

        httpretty.register_uri(httpretty.DELETE,
                               self.get_block_url(self.vault_name, blockid),
                               body="mock failure",
                               status=404)

        with self.assertRaises(RuntimeError) as deletion_error:
            client.DeleteBlock(self.vault_name, blockid)

    @httpretty.activate
    def test_block_download(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        blockid, blockdata = self.__class__.create_block()

        httpretty.register_uri(httpretty.GET,
                               self.get_block_url(self.vault_name, blockid),
                               body=blockdata,
                               status=200)

        data = client.GetBlockData(self.vault_name, blockid)
        self.assertEqual(data, blockdata)

    @httpretty.activate
    def test_block_download_failed(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        blockid, blockdata = self.__class__.create_block()

        httpretty.register_uri(httpretty.GET,
                               self.get_block_url(self.vault_name, blockid),
                               body="mock failure",
                               status=404)

        with self.assertRaises(RuntimeError) as deletion_error:
            client.GetBlockData(self.vault_name, blockid)
