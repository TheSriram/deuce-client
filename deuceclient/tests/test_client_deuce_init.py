"""
Tests - Deuce Client - Client - Deuce - Init
"""
import deuceclient.client.deuce
from deuceclient.tests import *


class ClientDeuceInitTests(ClientTestBase):

    def setUp(self):
        super(ClientDeuceInitTests, self).setUp()

    def tearDown(self):
        super(ClientDeuceInitTests, self).tearDown()

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
