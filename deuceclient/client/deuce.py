"""
Deuce API
"""
from __future__ import print_function
import json
import requests
import logging

from deuceclient.common.command import Command


class DeuceVault(Command):
    """
    Deuce Vault Functionality
    """

    def __init__(self, sslenabled, authenticator, apihost):
        """
        Initialize the Deuce Client access
            sslenabled - True if using HTTPS; otherwise false
            authenticator - instance of deuceclient.auth.Authentication to use
            apihost - server to use for API calls
        """
        super(self.__class__, self).__init__(sslenabled, apihost, '/')
        self.log = logging.getLogger(__name__)
        self.sslenabled = sslenabled
        self.authenticator = authenticator


class DeuceClient(Command):
    """
    Object defining HTTP REST API calls for interacting with Deuce.
    """

    def __init__(self, authenticator, apihost, sslenabled=False):
        """
        Initialize the Deuce Client access
            authenticator - instance of deuceclient.auth.Authentication to use
            apihost - server to use for API calls
            sslenabled - True if using HTTPS; otherwise false
        """
        super(self.__class__, self).__init__(apihost,
                                             '/',
                                             sslenabled=sslenabled)
        self.log = logging.getLogger(__name__)
        self.sslenabled = sslenabled
        self.authenticator = authenticator

    def __update_headers(self):
        """
        Update common headers
        """
        self.headers['X-Auth-Token'] = self.authenticator.AuthToken
        self.headers['X-Project-ID'] = self.ProjectId

    def __log_request_data(self):
        """
        Log the information about the request
        """
        self.log.debug('host: %s', self.apihost)
        self.log.debug('body: %s', self.Body)
        self.log.debug('headers: %s', self.Headers)
        self.log.debug('uri: %s', self.Uri)

    @property
    def ProjectId(self):
        """
        Return the project id to use
        """
        return self.authenticator.AuthTenantId

    def CreateVault(self, vaultname):
        """
        Create a Vault
            vaultname - name of vault to be created
        """
        self.ReInit(self.sslenabled, '/v1.0/{0:}'.format(vaultname))
        self.__update_headers()
        self.__log_request_data()
        res = requests.put(self.Uri, headers=self.Headers)

        if res.status_code == 201:
            return True
        else:
            raise RuntimeError(
                'Failed to create Vault. '
                'Error ({0:}): {1:}'.format(res.status_code, res.text))

    def DeleteVault(self, vaultname):
        """
        Delete a Vault
            vaultname - name of vault to be deleted
        """
        self.ReInit(self.sslenabled, '/v1.0/{0:}'.format(vaultname))
        self.__update_headers()
        self.__log_request_data()
        res = requests.delete(self.Uri, headers=self.Headers)

        if res.status_code == 204:
            return True
        else:
            raise RuntimeError(
                'Failed to delete Vault. '
                'Error ({0:}): {1:}'.format(res.status_code, res.text))

    def VaultExists(self, vaultname):
        """
        Return the statistics on a Vault
            vaultname - name of vault to be deleted
        """
        self.ReInit(self.sslenabled, '/v1.0/{0:}'.format(vaultname))
        self.__update_headers()
        self.__log_request_data()
        res = requests.head(self.Uri, headers=self.Headers)

        if res.status_code == 204:
            return True
        elif res.status_code == 404:
            return False
        else:
            raise RuntimeError(
                'Failed to determine if Vault exists. '
                'Error ({0:}): {1:}'.format(res.status_code, res.text))

    def GetVaultStatistics(self, vaultname):
        """
        Return the statistics on a Vault
            vaultname - name of vault to be deleted
        """
        self.ReInit(self.sslenabled, '/v1.0/{0:}'.format(vaultname))
        self.__update_headers()
        self.__log_request_data()
        res = requests.get(self.Uri, headers=self.Headers)

        if res.status_code == 200:
            return res.json()
        else:
            raise RuntimeError(
                'Failed to get Vault statistics. '
                'Error ({0:}): {1:}'.format(res.status_code, res.text))

    def GetBlockList(self, vaultname, marker=None, limit=None):
        """
        Return the list of blocks in the vault
        """
        url = '/v1.0/{0:}/blocks'.format(vaultname)
        if marker is not None or limit is not None:
            # add the separator between the URL and the parameters
            url = url + '?'

            # Apply the marker
            if marker is not None:
                url = '{0:}marker={1:}'.format(url, marker)
                # Apply a comma if the next item is not none
                if limit is not None:
                    url = url + ','

            # Apply the limit
            if limit is not None:
                url = '{0:}limit={1:}'.format(url, limit)

        self.ReInit(self.sslenabled, url)
        self.__update_headers()
        self.__log_request_data()
        res = requests.get(self.Uri, headers=self.Headers)

        if res.status_code == 200:
            return res.json()
        else:
            raise RuntimeError(
                'Failed to get Block list for Vault . '
                'Error ({0:}): {1:}'.format(res.status_code, res.text))

    def UploadBlock(self, vaultname, blockid, blockcontent, blocksize):
        """
        Upload a block to the vault specified.
            vaultname - name of the vault to be created
            blockid - the id (SHA-1) of the block to be uploaded
                      f.e 74bdda817d796333e9fe359e283d5643ee1a1397
            blockcontent - data present in the block to uploaded
        """
        url = '/v1.0/{0:}/blocks/{1:}'.format(vaultname, blockid)
        print('url{0:}'.format(url))
        self.ReInit(self.sslenabled, url)
        self.__update_headers()
        self.__log_request_data()
        headers = {}
        headers.update(self.Headers)
        headers['content-type'] = 'application/octet-stream'
        headers['content-length'] = blocksize
        res = requests.put(self.Uri, headers=self.Headers, data=blockcontent)
        if res.status_code == 201:
            return True
        else:
            raise RuntimeError(
                'Failed to upload Block. '
                'Error ({0:}): {1:}'.format(res.status_code, res.text))

    def DeleteBlock(self, vaultname, blockid):
        """
        Delete the block from the vault.
        This funciton has not been tested
        """
        url = '/v1.0/{0:}/blocks/{1:}'.format(vaultname, blockid)
        self.ReInit(self.sslenabled, url)
        self.__update_headers()
        self.__log_request_data()
        res = requests.delete(self.Uri, headers=self.Headers)
        if res.status_code == 204:
            return True
        else:
            raise RuntimeError(
                'Failed to delete Vault. '
                'Error ({0:}): {1:}'.format(res.status_code, res.text))

    def GetBlockData(self, vaultname, blockid):
        """
        Gets the data associated with the block id provided
        vaultname - exisiting vault, eg 'v1'
        block id - sha1 of block, eg - 74bdda817d796333e9fe359e283d5643ee1a1397
        """
        url = '/v1.0/{0:}/blocks/{1:}'.format(vaultname, blockid)
        self.ReInit(self.sslenabled, url)
        self.__update_headers()
        self.__log_request_data()
        res = requests.get(self.Uri, headers=self.Headers)

        if res.status_code == 200:
            return res.content
        else:
            raise RuntimeError(
                'Failed to get Block Content for Block Id . '
                'Error ({0:}): {1:}'.format(res.status_code, res.text))

    def CreateFile(self, vaultname):
        """
        Creates a file in the specified vault, does not post data to it
        Returns the location of the file which gives the file id
        """
        url = '/v1.0/{0:}/files'.format(vaultname)
        self.ReInit(self.sslenabled, url)
        self.__update_headers()
        self.__log_request_data()
        res = requests.post(self.Uri, headers=self.Headers)
        if res.status_code == 201:
            return res.headers['location']
        else:
            raise RuntimeError(
                'Failed to create File. '
                'Error ({0:}): {1:}'.format(res.status_code, res.text))

    def AssignBlocksToFile(self, vaultname, fileid, value):
        """
        Assigns the specified block to a file
        Returns an empty list if the blocks being assigned are already uploaded
        Returns the block id if the block trying to be assigned has not already
            been uploaded
        value - The block id, size and offset of the block to be assigned to
            the file. eg -
                {
                    "blocks": [
                        {
                            "id": "Block Id 1",
                            "size": "Block size 1",
                            "offset": "Block Offset 1"
                        },
                        {
                            "id": "Block Id 2",
                            "size": "Block size 2",
                            "offset":"Block Offset 2"
                        },
                        {
                            "id": "Block Id 3",
                            "size": "Block size 3",
                            "offset": "Block Offset 3"
                        }
                    ]
                }
        Mandatory to supply block size and offset along with the block id
        """
        url = '/v1.0/{0}/files/{1}'.format(vaultname, fileid)
        self.ReInit(self.sslenabled, url)
        self.__update_headers()
        self.__log_request_data()
        res = requests.post(self.Uri,
                            data=json.dumps(value),
                            headers=self.Headers)
        if res.status_code == 200:
            return res.json()
        else:
            raise RuntimeError(
                'Failed to Assign Blocks to the File. '
                'Error ({0:}): {1:}'.format(res.status_code, res.text))

    def GetFileBlockList(self, vaultname, fileid, marker=None, limit=None):
        """
        Return the list of blocks assigned to the file
        This does not finalize the file.
        This function is returning a 404
        """

        url = '/v1.0/{0:}/files/{1:}/blocks'.format(vaultname, fileid)

        if marker is not None or limit is not None:
            # add the separator between the URL and the parameters
            url = url + '?'

            # Apply the marker
            if marker is not None:
                url = '{0:}marker={1:}'.format(url, marker)
                # Apply a comma if the next item is not none
                if limit is not None:
                    url = url + ','

            # Apply the limit
            if limit is not None:
                url = '{0:}limit={1:}'.format(url, limit)

        self.ReInit(self.sslenabled, url)
        self.__update_headers()
        self.__log_request_data()
        res = requests.get(self.Uri, headers=self.Headers)

        if res.status_code == 200:
            return res.json()
        else:
            raise RuntimeError(
                'Failed to get Block list for File . '
                'Error ({0:}): {1:}'.format(res.status_code, res.text))
