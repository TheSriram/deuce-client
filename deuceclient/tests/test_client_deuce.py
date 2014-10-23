"""
Tests - Deuce Client - Client - Deuce
"""
import json

import httpretty

import deuceclient.api as api
import deuceclient.api.vault as api_vault
from deuceclient.tests import *


class DeuceClientTests(ClientTestBase):

    def setUp(self):
        super(self.__class__, self).setUp()

    def tearDown(self):
        super(self.__class__, self).tearDown()

    """
    Keeping temporarily for merge purposes
    To be Removed
    """
