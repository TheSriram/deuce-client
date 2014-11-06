"""
Tests - Deuce Client - Client - Deuce - Vault
"""
import json

import httpretty

import deuceclient.client.deuce
from deuceclient.tests import *


class ClientDeuceVaultTests(ClientTestBase):

    def setUp(self):
        super(ClientDeuceVaultTests, self).setUp()

        self.client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                           self.apihost,
                                                           sslenabled=True)

    def tearDown(self):
        super(ClientDeuceVaultTests, self).tearDown()

    @httpretty.activate
    def test_create_vault(self):
        httpretty.register_uri(httpretty.PUT,
                               get_vault_url(
                                   self.apihost,
                                   self.vault.vault_id),
                               status=201)

        vault = self.client.CreateVault(self.vault.vault_id)
        self.assertEqual(vault.vault_id, self.vault.vault_id)
        self.assertEqual(vault.status, "created")

    def test_create_vault_invalid_parameter(self):
        with self.assertRaises(TypeError) as creation_error:
            self.client.CreateVault(self.vault)

    @httpretty.activate
    def test_create_vault_failed(self):
        httpretty.register_uri(httpretty.PUT,
                               get_vault_url(
                                   self.apihost,
                                   self.vault.vault_id),
                               content_type='text/plain',
                               body="mock failure",
                               status=404)

        with self.assertRaises(RuntimeError) as creation_error:
            self.client.CreateVault(self.vault.vault_id)

    @httpretty.activate
    def test_get_vault(self):
        httpretty.register_uri(httpretty.HEAD,
                               get_vault_url(self.apihost,
                                             self.vault.vault_id),
                               status=204)
        vault = self.client.GetVault(self.vault.vault_id)
        self.assertEqual(vault.vault_id,
                         self.vault.vault_id)
        self.assertEqual(vault.status, "valid")

    def test_get_vault_failed_bad_parameter(self):
        with self.assertRaises(TypeError):
            self.client.GetVault(self.vault)

    @httpretty.activate
    def test_get_vault_failed(self):
        httpretty.register_uri(httpretty.HEAD,
                               get_vault_url(self.apihost,
                                             self.vault.vault_id),
                               status=404)
        with self.assertRaises(RuntimeError):
            self.client.GetVault(self.vault.vault_id)

    @httpretty.activate
    def test_delete_vault_by_api_vault(self):
        httpretty.register_uri(httpretty.DELETE,
                               get_vault_url(self.apihost,
                                             self.vault.vault_id),
                               status=204)
        self.assertTrue(self.client.DeleteVault(self.vault))

    @httpretty.activate
    def test_delete_vault_by_vault_name(self):
        with self.assertRaises(TypeError):
            self.client.DeleteVault(self.vault.vault_id)

    @httpretty.activate
    def test_delete_vault_failed(self):
        httpretty.register_uri(httpretty.DELETE,
                               get_vault_url(self.apihost,
                                             self.vault.vault_id),
                               content_type='text/plain',
                               body="mock failure",
                               status=404)

        with self.assertRaises(RuntimeError) as deletion_error:
            self.client.DeleteVault(self.vault)

    @httpretty.activate
    def test_vault_exists_by_api_vault(self):
        httpretty.register_uri(httpretty.HEAD,
                               get_vault_url(self.apihost,
                                             self.vault.vault_id),
                               status=204)

        self.assertTrue(self.client.VaultExists(self.vault))

    @httpretty.activate
    def test_vault_exists_by_vault_name(self):
        httpretty.register_uri(httpretty.HEAD,
                               get_vault_url(self.apihost,
                                             self.vault.vault_id),
                               status=204)

        self.assertTrue(self.client.VaultExists(self.vault.vault_id))

    @httpretty.activate
    def test_vault_does_not_exist(self):
        httpretty.register_uri(httpretty.HEAD,
                               get_vault_url(self.apihost,
                                             self.vault.vault_id),
                               status=404)

        self.assertFalse(self.client.VaultExists(self.vault))

    @httpretty.activate
    def test_vault_exists_failure(self):
        httpretty.register_uri(httpretty.HEAD,
                               get_vault_url(self.apihost,
                                             self.vault.vault_id),
                               content_type='text/plain',
                               body="mock failure",
                               status=401)

        with self.assertRaises(RuntimeError) as exists_error:
            self.client.VaultExists(self.vault)

    @httpretty.activate
    def test_vault_statistics_by_api_vault(self):
        data = {'status': True}
        expected_body = json.dumps(data)

        httpretty.register_uri(httpretty.GET,
                               get_vault_url(self.apihost,
                                             self.vault.vault_id),
                               content_type='application/json',
                               body=expected_body,
                               status=200)

        self.assertTrue(self.client.GetVaultStatistics(self.vault))
        self.assertEqual(data, self.vault.statistics)

    def test_vault_statistics_by_vault_name(self):
        with self.assertRaises(TypeError):
            self.client.GetVaultStatistics(self.vault.vault_id)

    @httpretty.activate
    def test_vault_statistics_failure(self):
        httpretty.register_uri(httpretty.GET,
                               get_vault_url(self.apihost,
                                             self.vault.vault_id),
                               content_type='text/plain',
                               body="mock failure",
                               status=404)

        with self.assertRaises(RuntimeError) as stats_error:
            self.client.GetVaultStatistics(self.vault)
