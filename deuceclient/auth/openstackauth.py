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

    def __init__(self, userid=None, usertype=None,
                 credentials=None, auth_method=None,
                 datacenter=None, auth_url=None):
        if auth_url is None:
            raise deuceclient.auth.AuthenticationError(
                'Required Parameter, auth_url, not specified.')

        super().__init__(userid=userid, usertype=usertype,
                         credentials=credentials, auth_method=auth_method,
                         datacenter=datacenter, auth_url=auth_url)

        self.__client = None
        self.__access = None

    def get_client(self):
        """Retrieve the OpenStack Keystone Client
        """

        auth_args = {
            'auth_url': self.authurl
        }

        # Extract the User Information
        if self.usertype in ('user_name', 'user_id'):
            auth_args['username'] = self.userid

        elif self.usertype == 'tenant_name':
            auth_args['tenant_name'] = self.userid

        elif self.usertype == 'tenant_id':
            auth_args['tenant_id'] = self.userid

        else:
            raise deuceclient.auth.AuthenticationError(
                'Invalid usertype ({0:}) for OpenStackAuthentication'
                .format(self.usertype))

        # Extract the User's Credential Information
        if self.authmethod in ('apikey', 'password'):
            auth_args['password'] = self.credentials

        elif self.authmethod == 'token':
            auth_args['token'] = self.credentials

        else:
            raise deuceclient.auth.AuthenticationError(
                'Invalid auth_method ({0:}) for OpenStackAuthentication'
                .format(self.authmethod))

        return keystoneclient.client.Client(**auth_args)

    def GetToken(self, retry=5):
        """Retrieve a token from OpenStack Keystone
        """
        if self.__client is None:
            self.__client = self.get_client()

        try:
            self.__access = self.__client.get_raw_token_from_identity_service(
                auth_url=self.authurl)
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
