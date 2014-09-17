"""
Deuce Client OpenStack Authentication Tests
"""
import uuid
from unittest import TestCase

import contextlib
import keystoneclient.exceptions
import mock

import deuceclient.auth
import deuceclient.auth.openstackauth as openstackauth
import deuceclient.tests.test_auth


class FakeAccess(object):
    """Fake Keystone Access Object for testing
    """
    raise_until = 0
    raise_counter = 0
    raise_error = False

    def __init__(self):
        pass

    @classmethod
    def raise_error(cls, raise_or_not):
        cls.raise_error = raise_or_not

    @classmethod
    def raise_reset(cls, raise_until=0, raise_error=False):
        cls.raise_until = raise_until
        cls.raise_error = raise_error
        cls.raise_counter = 0

    @property
    def auth_token(self):
        if self.__class__.raise_until > 0:
            self.__class__.raise_error = (
                self.__class__.raise_counter < self.__class__.raise_until)

            if self.__class__.raise_error:
                self.__class__.raise_counter = self.__class__.raise_counter + 1

        if self.__class__.raise_error is True:
            raise keystoneclient.exceptions.AuthorizationFailure('mocking')
        else:
            return 'token_{0:}'.format(str(uuid.uuid4()))


class FakeClient(object):
    """Fake Keystone Client Object for testing
    """
    def __init__(self, *args, **kwargs):
        pass

    def get_raw_token_from_identity_service(self, *args, **kwargs):
        return FakeAccess()


class OpenStackAuthTest(TestCase,
                        deuceclient.tests.test_auth.AuthenticationBaseTest):

    def create_authengine(self, userid=None, usertype=None,
                          credentials=None, auth_method=None,
                          datacenter=None, auth_url=None):
        return openstackauth.OpenStackAuthentication(userid=userid,
                                                     usertype=usertype,
                                                     credentials=credentials,
                                                     auth_method=auth_method,
                                                     datacenter=datacenter,
                                                     auth_url=auth_url)

    def test_parameter_no_authurl(self):
        userid = self.create_userid()
        apikey = self.create_apikey()

        with self.assertRaises(deuceclient.auth.AuthenticationError) \
                as auth_error:

            authengine = self.create_authengine(userid=userid,
                                                usertype='user_id',
                                                credentials=apikey,
                                                auth_method=None,
                                                datacenter='test',
                                                auth_url=None)

    def test_keystone_parameters(self):
        userid = self.create_userid()
        username = self.create_username()
        tenantid = self.create_tenant_id()
        tenantname = self.create_tenant_name()
        apikey = self.create_apikey()
        password = self.create_password()
        token = self.create_token()

        datacenter = 'test'
        auth_url = __name__

        usertype = 'user_id'
        auth_method = 'apikey'
        with mock.patch('keystoneclient.client.Client') as keystone_mock:
            keystone_mock.return_value = True

            authengine = self.create_authengine(userid=userid,
                                                usertype=usertype,
                                                credentials=apikey,
                                                auth_method=auth_method,
                                                datacenter=datacenter,
                                                auth_url=auth_url)

            client = authengine.get_client()

            kargs, kwargs = keystone_mock.call_args
            self.assertIn('username', kwargs)
            self.assertEqual(kwargs['username'], userid)

            self.assertIn('password', kwargs)
            self.assertEqual(kwargs['password'], apikey)

            self.assertIn('auth_url', kwargs)
            self.assertEqual(kwargs['auth_url'], auth_url)

        usertype = 'user_name'
        auth_method = 'password'
        with mock.patch('keystoneclient.client.Client') as keystone_mock:
            keystone_mock.return_value = True

            authengine = self.create_authengine(userid=username,
                                                usertype=usertype,
                                                credentials=password,
                                                auth_method=auth_method,
                                                datacenter=datacenter,
                                                auth_url=auth_url)

            client = authengine.get_client()

            kargs, kwargs = keystone_mock.call_args
            self.assertIn('username', kwargs)
            self.assertEqual(kwargs['username'], username)

            self.assertIn('password', kwargs)
            self.assertEqual(kwargs['password'], password)

            self.assertIn('auth_url', kwargs)
            self.assertEqual(kwargs['auth_url'], auth_url)

        usertype = 'tenant_name'
        auth_method = 'token'
        with mock.patch('keystoneclient.client.Client') as keystone_mock:
            keystone_mock.return_value = True

            authengine = self.create_authengine(userid=tenantname,
                                                usertype=usertype,
                                                credentials=token,
                                                auth_method=auth_method,
                                                datacenter=datacenter,
                                                auth_url=auth_url)

            client = authengine.get_client()

            kargs, kwargs = keystone_mock.call_args
            self.assertIn('tenant_name', kwargs)
            self.assertEqual(kwargs['tenant_name'], tenantname)

            self.assertIn('token', kwargs)
            self.assertEqual(kwargs['token'], token)

            self.assertIn('auth_url', kwargs)
            self.assertEqual(kwargs['auth_url'], auth_url)

        usertype = 'tenant_id'
        auth_method = 'token'
        with mock.patch('keystoneclient.client.Client') as keystone_mock:
            keystone_mock.return_value = True

            authengine = self.create_authengine(userid=tenantid,
                                                usertype=usertype,
                                                credentials=token,
                                                auth_method=auth_method,
                                                datacenter=datacenter,
                                                auth_url=auth_url)

            client = authengine.get_client()

            kargs, kwargs = keystone_mock.call_args
            self.assertIn('tenant_id', kwargs)
            self.assertEqual(kwargs['tenant_id'], tenantid)

            self.assertIn('token', kwargs)
            self.assertEqual(kwargs['token'], token)

            self.assertIn('auth_url', kwargs)
            self.assertEqual(kwargs['auth_url'], auth_url)

        usertype = 'bison'
        with mock.patch('keystoneclient.client.Client') as keystone_mock:
            keystone_mock.return_value = False

            authengine = self.create_authengine(userid=tenantid,
                                                usertype=usertype,
                                                credentials=token,
                                                auth_method=auth_method,
                                                datacenter=datacenter,
                                                auth_url=auth_url)

            with self.assertRaises(deuceclient.auth.base.AuthenticationError) \
                    as auth_error:

                client = authengine.get_client()

                self.assertIn('usertype', str(auth_error))

        usertype = 'tenant_id'
        auth_method = 'yak'
        with mock.patch('keystoneclient.client.Client') as keystone_mock:
            keystone_mock.return_value = False

            authengine = self.create_authengine(userid=tenantid,
                                                usertype=usertype,
                                                credentials=token,
                                                auth_method=auth_method,
                                                datacenter=datacenter,
                                                auth_url=auth_url)

            with self.assertRaises(deuceclient.auth.base.AuthenticationError) \
                    as auth_error:

                client = authengine.get_client()

                self.assertIn('auth_method', str(auth_error))

    def test_get_token_failed(self):
        usertype = 'user_name'
        username = self.create_username()

        apikey = self.create_apikey()
        auth_method = 'apikey'

        datacenter = 'test'
        # auth_url = __name__
        auth_url = 'http://identity.api.rackspacecloud.com'

        # Because the mock strings are so long, we're going to store them
        # in variables here to keep the mocking statements short
        mok_ky_base = 'keystoneclient'

        mok_ky_httpclient = '{0:}.httpclient.HTTPClient'.format(mok_ky_base)
        mok_ky_auth = '{0:}.authenticate'.format(mok_ky_httpclient)

        mok_ky_v2_client = '{0:}.v2_0.client.Client'.format(mok_ky_base)
        mok_ky_v2_rawtoken = '{0:}.get_raw_token_from_identity_service'\
            .format(mok_ky_v2_client)

        # mok_ky_session = '{0:}.session'.format(mok_ky_base)
        # mok_ky_session_obj = '{0:}.Session'.format(mok_ky_session)
        # mok_ky_session_construct = '{0:}.construct'\
        #    .format(mok_ky_session_obj)

        mok_ky_discover = '{0:}.discover'.format(mok_ky_base)
        mok_ky_discovery = '{0:}.Discover'.format(mok_ky_discover)
        mok_ky_discovery_init = '{0:}.__init__'.format(mok_ky_discovery)
        mok_ky_discover_client = '{0:}.create_client'.format(mok_ky_discovery)

        mok_ky_discover_int = '{0:}._discover'.format(mok_ky_base)
        mok_ky_discover_version = '{0:}.get_version_data'\
            .format(mok_ky_discover_int)

        with mock.patch(mok_ky_auth) as keystone_auth_mock,\
                mock.patch(mok_ky_v2_rawtoken) as keystone_raw_token_mock,\
                mock.patch(mok_ky_discover_version) as keystone_discover_ver,\
                mock.patch(mok_ky_discover_client) as keystone_discover_cli:
                # mock.patch(mok_ky_discovery_init) as keystone_discovery, \
                # mock.patch(mok_ky_discover_client) as keystone_find_client:

            keystone_auth_mock.return_value = True
            # keystone_discovery.return_value = None
            # keystone_find_client.return_value = True

            keystone_discover_ver.return_value = [
                {
                    "id": "v1.0",
                    "links": [
                        {
                            "href": "https://identity.api.rackspacecloud.com/"
                            "v1.0",
                            "rel": "self"
                        }
                    ],
                    "status": "DEPRECATED",
                    "updated": "2011-07-19T22:30:00Z"
                },
                {
                    "id": "v1.1",
                    "links": [
                        {
                            "href": "http://docs.rackspacecloud.com/"
                            "auth/api/v1.1/auth.wadl",
                            "rel": "describedby",
                            "type": "application/vnd.sun.wadl+xml"
                        }
                    ],
                    "status": "CURRENT",
                    "updated": "2012-01-19T22:30:00.25Z"
                },
                {
                    "id": "v2.0",
                    "links": [
                        {
                            "href":
                                "http://docs.rackspacecloud.com/"
                                "auth/api/v2.0/auth.wadl",
                            "rel": "describedby",
                            "type": "application/vnd.sun.wadl+xml"
                        }
                    ],
                    "status": "CURRENT",
                    "updated": "2012-01-19T22:30:00.25Z"
                }
            ]
            keystone_discover_cli.return_value = FakeClient()

            FakeAccess.raise_until = 4
            FakeAccess.raise_counter = 0
            keystone_raw_token_mock.return_value = FakeAccess()

            authengine = self.create_authengine(userid=username,
                                                usertype=usertype,
                                                credentials=apikey,
                                                auth_method=auth_method,
                                                datacenter=datacenter,
                                                auth_url=auth_url)

            with self.assertRaises(deuceclient.auth.AuthenticationError) \
                    as auth_error:
                authengine.GetToken(retry=FakeAccess.raise_until - 1)

    def test_get_token_success(self):
        usertype = 'user_name'
        username = self.create_username()

        apikey = self.create_apikey()
        auth_method = 'apikey'

        datacenter = 'test'
        # auth_url = __name__
        auth_url = 'http://identity.api.rackspacecloud.com'

        # Because the mock strings are so long, we're going to store them
        # in variables here to keep the mocking statements short
        mok_ky_base = 'keystoneclient'

        mok_ky_httpclient = '{0:}.httpclient.HTTPClient'.format(mok_ky_base)
        mok_ky_auth = '{0:}.authenticate'.format(mok_ky_httpclient)

        mok_ky_v2_client = '{0:}.v2_0.client.Client'.format(mok_ky_base)
        mok_ky_v2_rawtoken = '{0:}.get_raw_token_from_identity_service'\
            .format(mok_ky_v2_client)

        # mok_ky_session = '{0:}.session'.format(mok_ky_base)
        # mok_ky_session_obj = '{0:}.Session'.format(mok_ky_session)
        # mok_ky_session_construct = '{0:}.construct'\
        #    .format(mok_ky_session_obj)

        mok_ky_discover = '{0:}.discover'.format(mok_ky_base)
        mok_ky_discovery = '{0:}.Discover'.format(mok_ky_discover)
        mok_ky_discovery_init = '{0:}.__init__'.format(mok_ky_discovery)
        mok_ky_discover_client = '{0:}.create_client'.format(mok_ky_discovery)

        mok_ky_discover_int = '{0:}._discover'.format(mok_ky_base)
        mok_ky_discover_version = '{0:}.get_version_data'\
            .format(mok_ky_discover_int)

        with mock.patch(mok_ky_auth) as keystone_auth_mock,\
                mock.patch(mok_ky_v2_rawtoken) as keystone_raw_token_mock,\
                mock.patch(mok_ky_discover_version) as keystone_discover_ver,\
                mock.patch(mok_ky_discover_client) as keystone_discover_cli:
                # mock.patch(mok_ky_discovery_init) as keystone_discovery, \
                # mock.patch(mok_ky_discover_client) as keystone_find_client:

            keystone_auth_mock.return_value = True
            # keystone_discovery.return_value = None
            # keystone_find_client.return_value = True

            keystone_discover_ver.return_value = [
                {
                    "id": "v1.0",
                    "links": [
                        {
                            "href": "https://identity.api.rackspacecloud.com/"
                            "v1.0",
                            "rel": "self"
                        }
                    ],
                    "status": "DEPRECATED",
                    "updated": "2011-07-19T22:30:00Z"
                },
                {
                    "id": "v1.1",
                    "links": [
                        {
                            "href": "http://docs.rackspacecloud.com/"
                            "auth/api/v1.1/auth.wadl",
                            "rel": "describedby",
                            "type": "application/vnd.sun.wadl+xml"
                        }
                    ],
                    "status": "CURRENT",
                    "updated": "2012-01-19T22:30:00.25Z"
                },
                {
                    "id": "v2.0",
                    "links": [
                        {
                            "href":
                                "http://docs.rackspacecloud.com/"
                                "auth/api/v2.0/auth.wadl",
                            "rel": "describedby",
                            "type": "application/vnd.sun.wadl+xml"
                        }
                    ],
                    "status": "CURRENT",
                    "updated": "2012-01-19T22:30:00.25Z"
                }
            ]
            keystone_discover_cli.return_value = FakeClient()

            FakeAccess.raise_until = 4
            FakeAccess.raise_counter = 0
            keystone_raw_token_mock.return_value = FakeAccess()

            authengine = self.create_authengine(userid=username,
                                                usertype=usertype,
                                                credentials=apikey,
                                                auth_method=auth_method,
                                                datacenter=datacenter,
                                                auth_url=auth_url)

            token = authengine.GetToken(retry=FakeAccess.raise_until)
