"""
Tests - Deuce Client - Client - Deuce - Init
"""
import json

import httpretty
import requests

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

    def test_log_request(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)
        client._DeuceClient__log_request_data()

        client._DeuceClient__log_request_data(fn=None)

        client._DeuceClient__log_request_data(fn=None,
                                              headers=None)

        client._DeuceClient__log_request_data(fn='howdy',
                                              headers=None)

        client._DeuceClient__log_request_data(fn='doody',
                                              headers={'X-Car': 'humvee'})

        client._DeuceClient__log_request_data(headers={'X-Rug': 'bearskin'})

    @httpretty.activate
    def test_log_response(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        resp_uri = 'http://log.response/'
        resp_data = {'field': 'value'}

        httpretty.register_uri(httpretty.GET,
                               resp_uri,
                               content_type="application/json",
                               body=json.dumps(resp_data),
                               status=200)
        response = requests.get(resp_uri)

        client._DeuceClient__log_response_data(response)
        client._DeuceClient__log_response_data(response,
                                               jsondata=True)
        client._DeuceClient__log_response_data(response,
                                               fn='transformers')
        client._DeuceClient__log_response_data(response,
                                               jsondata=True,
                                               fn='strawberry')
        client._DeuceClient__log_response_data(response,
                                               jsondata=False,
                                               fn='shortcake')
