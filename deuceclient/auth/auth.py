"""
Deuce Authentication API
"""
import abc
import json
import requests
import logging
import datetime
import time

from deuceclient.common.command import Command


# TODO: Add a base Auth class
# TODO: Convert below to use KeystoneClient

class AuthenticationError(Exception):
    pass


class AuthCredentialsErrors(AuthenticationError):
    pass


class AuthExpirationError(AuthenticationError):
    pass


class AuthenticationBase(object, metaclass=abc.ABCMeta):
    """
    Authentication Interface Class
    """

    def __init__(self, userid, usertype,
                 credentials, auth_method,
                 datacenter, auth_url=None):
        """
        :param userid:  string - User Identifier, e.g username, userid, etc.)
        :param usertype: string - Type of User Identifier
                         values: user_id, user_name, userid, tenant_name,
                                 tenant_id
        :param credentials: string - User Credentials for Authentication
                                     e.g. password
        :param auth_method: string - Type of User Credentials
                                     values: apikey, password, token

        :param auth_url: string - Authentication Service URL to use
        :param datacenter: string - Datacenter to autheniticate in
                                    e.g. identity.rackspace.com
        """
        self.__catalog = {}
        self.__catalog['user'] = userid
        self.__catalog['credentials'] = credentials
        self.__catalog['usertype'] = usertype
        self.__catalog['auth_method'] = method
        self.__catalog['datacenter'] = datacenter

    @property
    def userid(self):
        """Return the User Identifier used for authentication
        """
        return self.__catalog['user']

    @property
    def credentials(self):
        """Return the User Crentidals used for authentication
        """
        return self.__catalog['credentials']

    @property
    def usertype(self):
        """Return the type of user credentials used for authencation
        """
        return self.__catalog['usertype']

    @property
    def authmethod(self):
        """Return the Authentication Method used for authentication
        """
        return self.__catalog['auth_method']

    @property
    def datacenter(self):
        """Return the Datacenter (region) authentication was performed against
        """
        return self.__catalog['datacenter']

    @abc.abstractmethod
    def GetToken(self, retry=5):
        """Retrieve an Authentication Token

        :param retry: integer - number of times to retry getting a token
        :returns: string - authentication token or None if failure
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def IsExpired(self, fuzz=0):
        """Has the token expired
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def AuthToken(self):
        """Access the current AuthToken, retrieving it if necessary

        :returns: string - authentication token
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def AuthExpirationTime(self):
        """Retrieve the time at which the token will expire

        :returns: datetime.datetime - date/time the token expires at
        """
        raise NotImplementedError()
