"""
Deuce Rackspace Authentication API
"""
import logging

import keystoneclient.v2_0.client as client_v2

import deuceclient.auth
import deuceclient.auth.openstackauth


def get_identity_apihost(datacenter):
    if datacenter in ('us', 'uk', 'lon', 'iad', 'dfw', 'ord'):
        return 'https://identity.api.rackspacecloud.com/v2.0'
    elif datacenter in ('hkg', 'syd'):
        return'https://{0:}.identity.api.rackspacecloud.com/v2.0'.\
            format(datacenter)
    else:
        raise deuceclient.auth.AuthenticationError(
            'Unknown Data Center: {0:}'.format(datacenter))


class RackspaceAuthentication(
        deuceclient.auth.openstackauth.OpenStackAuthentication):
    """Rackspace Identity Authentication Support

    Only difference between this and OpenStackAuthentication is that this
    can know the servers without one being specified.
    """

    def __init__(self, userid=None, usertype=None,
                 credentials=None, auth_method=None,
                 datacenter=None, auth_url=None):

        # If an authentication url is not provided then create one using
        # Rackspace's Identity Service for the specified datacenter
        if auth_url is None:
            if datacenter is None:
                raise deuceclient.auth.AuthenticationError(
                    'Required Parameter, datacenter, not specified.')

            auth_url = get_identity_apihost(datacenter)
            log = logging.getLogger(__name__)
            log.debug('No AuthURL specified. Using {0:}'.format(auth_url))

        super().__init__(userid=userid, usertype=usertype,
                         credentials=credentials, auth_method=auth_method,
                         datacenter=datacenter, auth_url=auth_url)

    @staticmethod
    def _management_url(*args, **kwargs):
        return 'https://identity.api.rackspacecloud.com/v2.0'

    @staticmethod
    def patch_management_url(self):
        from keystoneclient.service_catalog import ServiceCatalog
        ServiceCatalog.url_for = RackspaceAuthentication._management_url

    def get_client(self):
        """Retrieve the Rackspace Client
        """

        auth_args = {
            'auth_url': self.authurl,
            'region_name': self.datacenter
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
                'Invalid usertype ({0:}) for RackspaceAuthentication'
                .format(self.usertype))

        # Extract the User's Credential Information
        if self.authmethod in ('apikey', 'password'):
            auth_args['password'] = self.credentials

        elif self.authmethod == 'token':
            auth_args['token'] = self.credentials

        else:
            raise deuceclient.auth.AuthenticationError(
                'Invalid auth_method ({0:}) for RackspaceAuthentication'
                .format(self.authmethod))

        RackspaceAuthentication.patch_management_url()

        return client_v2.Client(**auth_args)
