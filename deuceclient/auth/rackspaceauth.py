"""
Deuce Rackspace Authentication API
"""
import logging

import deuceclient.auth


def get_identity_apihost(datacenter):
    if datacenter in ('us', 'uk', 'lon', 'iad', 'dfw', 'ord'):
        return 'identity.api.rackspacecloud.com'
    elif datacenter in ('hkg', 'syd'):
        return'{0:}.identity.api.rackspacecloud.com'.format(datacenter)
    else:
        raise AuthenticationError(
            'Unknown Data Center: {0:}'.format(datacenter))


class RackspaceAuthentication(deuceclient.auth.OpenStackAuthentication):
    """Rackspace Identity Authentication Support

    Only difference between this and OpenStackAuthentication is that this
    can know the servers without one being specified.
    """

    def __init__(self, userid, usertype,
                 credentials, auth_method,
                 datacenter, auth_url=None):

        # If an authentication url is not provided then create one using
        # Rackspace's Identity Service for the specified datacenter
        if auth_url is None:
            auth_url = get_identity_apihost(datacenter)
            log = logging.getLogger(__name__)
            log.debug('No AuthURL specified. Using {0:}'.format(auth_url))

        super().__init__(userid, credentials, usertype, auth_method,
                       datacenter)
