"""
Deuce Authentication
"""

from deuceclient.auth.auth import AuthenticationBase
from deuceclient.auth.auth import AuthenticationError
from deuceclient.auth.auth import AuthCredentialsErrors
from deuceclient.auth.auth import AuthExpirationError

# Basic OpenStack Keystone Authentication, AuthURL unknown
from deuceclient.auth.openstackauth import OpenStackAuthentication

# OpenStack Keystone Authentication against Rackspace Identity
from deuceclient.auth.rackspaceauth import RackspaceAuthentication
