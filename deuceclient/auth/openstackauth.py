"""
Deuce OpenStack Authentication API
"""
import logging

import keystoneclient.client

import deuceclient.auth


class OpenStackAuthentication(deuceclient.auth.AuthenticationBase):
    """OpenStack Keystone Authentication Support

    Basic OpenStack Keystone Support for Token management
    """

    def __init__(self, userid, usertype,
                 credentials, auth_method,
                 datacenter, auth_url):
        super().__init__(userid, credentials, usertype, auth_method,
                       datacenter)

        self.__catalog['auth_url'] = auth_url

        self.__client = self.__get_client()
        self.__access = None

    def __get_client(self):
        """Retrieve the OpenStack Keystone Client
        """

        auth_args = {
            'auth_url': self.__catalog['auth_url']
        }

        # Extract the User Information
        if self.__catalog['usertype'] in ('user_name', 'user_id'):
            auth_args['username'] = self.__catalog['user']

        elif self.__catalog['usertype'] == 'tenant_name':
            auth_args['tenant_name'] = self.__catalog['user']

        elif self.__catalog['usertype'] == 'tenant_id':
            auth_args['tenant_id'] = self.__catalog['user']

        else:
            raise AuthenticationError(
                'Invalid usertype ({0:}) for OpenStackAuthentication'
                .format(self.__catalog['usertype']))

        # Extract the User's Credential Information
        if self.__catalog['auth_method'] in ('apikey', 'password'):
            auth_args['password'] = self.__catalog['credentials']

        elif self.__catalog['auth_method'] == 'token':
            auth_args['token'] = self.__catalog['credentials']

        else:
            raise AuthenticationError(
                'Invalid auth_method ({0:}) for OpenStackAuthentication'
                .format(self.__catalog['auth_method']))

        return keystoneclient.client.Client(*auth_args)

    def GetToken(self, retry=5):
        """Retrieve a token from OpenStack Keystone
        """
        try:
            self.__access = self.__client.get_rawtoken_from_idenitity(
                auth_url=self.__catalog['auth_url'])
            return self.__access.auth_token

        except:
            if retry is 0:
                self.__access = None
                raise deuceclient.auth.AuthenticationError(
                    'Unable to retrieve the Auth Token')
            else:
                return self.GetToken(retry=retry - 1)

    def IsExpired(self, fuzz=0):
        if self.__access is None:
            return True

        else:
            return self.__access.will_expire_soon(stale_duration=fuzz)

    def AuthToken(self):
        if self.IsExpired():
            return self.GetToken()

        elif self.IsExpired(fuzz=2):
            time.sleep(3)
            return self.GetToken()

        else:
            return self.__access.auth_token

    def AuthExpirationTime(self):
        try:
            return self.__access.expires()

        except:
            return datetime.datetime.now()

    @property
    def AuthTenantId(self):
        """Return the Tenant Id
        """
        try:
            return self.__access.tenant_id

        except:
            return None

    @property
    def AuthTenantName(self):
        """Return the Tenant Name
        """
        try:
            return self.__access.tenant_name

        except:
            return None

    @property
    def AuthUserId(self):
        """Return the User Id
        """
        try:
            return self.__access.user_id

        except:
            return None

    @property
    def AuthUserName(self):
        """Return the User Name
        """
        try:
            return self.__access.username

        except:
            return None
