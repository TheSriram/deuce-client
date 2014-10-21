"""
Deuce API
"""
import json
import requests
import logging

import deuceclient.api as api
import deuceclient.api.vault as api_vault
import deuceclient.api.v1 as api_v1
from deuceclient.common.command import Command


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
        self.headers['X-Project-ID'] = self.project_id

    def __log_request_data(self):
        """
        Log the information about the request
        """
        self.log.debug('host: %s', self.apihost)
        self.log.debug('body: %s', self.Body)
        self.log.debug('headers: %s', self.Headers)
        self.log.debug('uri: %s', self.Uri)

    @property
    def project_id(self):
        """
        Return the project id to use
        """
        return self.authenticator.AuthTenantId

    def CreateVault(self, vault_name):
        """Create a vault

        :param vault_name: name of the vault
        :returns: deuceclient.api.Vault instance of the new Vault
        """
        if not isinstance(vault_name, str):
            raise TypeError('vault_name must be string')

        path = api_v1.get_vault_path(vault_name)
        self.ReInit(self.sslenabled, path)

        self.__update_headers()
        self.__log_request_data()
        res = requests.put(self.Uri, headers=self.Headers)

        if res.status_code == 201:
            vault = api_vault.Vault(project_id=self.project_id,
                                    vault_id=vault_name)
            vault.status = "created"
            return vault
        else:
            raise RuntimeError(
                'Failed to create Vault. '
                'Error ({0:}): {1:}'.format(res.status_code, res.text))

    def GetVault(self, vault_name):
        """Get an existing vault

        :param vault_name: name of the vault
        :returns: deuceclient.api.Vault instance of the existing Vault
        """
        if not isinstance(vault_name, str):
            raise TypeError('vault_name must be string')

        if self.VaultExists(vault_name):
            vault = api_vault.Vault(project_id=self.project_id,
                                    vault_id=vault_name)
            vault.status = "valid"
            return vault
        else:
            raise RuntimeError('Failed to find a Vault with the name {0:}'
                               .format(vault_name))

    def DeleteVault(self, vault):
        """
        Delete a Vault
            vault - vault or name of the vault to be deleted
        """
        if not isinstance(vault, api_vault.Vault):
            raise TypeError('vault must be deuceclient.api.Vault')

        path = api_v1.get_vault_path(vault.vault_id)
        self.ReInit(self.sslenabled, path)
        self.__update_headers()
        self.__log_request_data()
        res = requests.delete(self.Uri, headers=self.Headers)

        if res.status_code == 204:
            vault.status = 'deleted'
            return True
        else:
            raise RuntimeError(
                'Failed to delete Vault. '
                'Error ({0:}): {1:}'.format(res.status_code, res.text))

    def VaultExists(self, vault):
        """
        Return the statistics on a Vault
            vault - name of vault to be deleted
        """
        # Note: We cannot use GetVault() here b/c it would
        #   end up being self-referential
        vault_id = vault
        if isinstance(vault, api_vault.Vault):
            vault_id = vault.vault_id

        path = api_v1.get_vault_path(vault_id)
        self.ReInit(self.sslenabled, path)
        self.__update_headers()
        self.__log_request_data()
        res = requests.head(self.Uri, headers=self.Headers)

        if res.status_code == 204:
            if isinstance(vault, api_vault.Vault):
                vault.status = 'valid'
            return True
        elif res.status_code == 404:
            if isinstance(vault, api_vault.Vault):
                vault.status = 'invalid'
            return False
        else:
            raise RuntimeError(
                'Failed to determine if Vault exists. '
                'Error ({0:}): {1:}'.format(res.status_code, res.text))

    def GetVaultStatistics(self, vault):
        """
        Return the statistics on a Vault
            vault - name of vault to be deleted
        """
        if not isinstance(vault, api_vault.Vault):
            raise TypeError('vault must be of type deuceclient.api.Vault')

        path = api_v1.get_vault_path(vault.vault_id)
        self.ReInit(self.sslenabled, path)
        self.__update_headers()
        self.__log_request_data()
        res = requests.get(self.Uri, headers=self.Headers)

        if res.status_code == 200:
            vault.statistics = res.json()
            return vault
        else:
            raise RuntimeError(
                'Failed to get Vault statistics. '
                'Error ({0:}): {1:}'.format(res.status_code, res.text))

    def GetBlockList(self, vault, marker=None, limit=None):
        """
        Return the list of blocks in the vault
        """
        url = api_v1.get_blocks_path(vault.vault_id)
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
            for block_entry in res.json():
                vault.blocks[block_entry] = api.Block(vault.project_id,
                                                      vault.vault_id,
                                                      block_entry)
            return True
        else:
            raise RuntimeError(
                'Failed to get Block list for Vault . '
                'Error ({0:}): {1:}'.format(res.status_code, res.text))

    def UploadBlock(self, vault, blockid, blockcontent, blocksize):
        """
        Upload a block to the vault specified.
            vault - name of the vault to be created
            blockid - the id (SHA-1) of the block to be uploaded
                      f.e 74bdda817d796333e9fe359e283d5643ee1a1397
            blockcontent - data present in the block to uploaded
        """
        url = api_v1.get_block_path(vault.vault_id, blockid)
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

    def DeleteBlock(self, vault, blockid):
        """
        Delete the block from the vault.
        This funciton has not been tested
        """
        url = api_v1.get_block_path(vault.vault_id, blockid)
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

    def GetBlockData(self, vault, blockid):
        """
        Gets the data associated with the block id provided
        vault - exisiting vault, eg 'v1'
        block id - sha1 of block, eg - 74bdda817d796333e9fe359e283d5643ee1a1397
        """
        url = api_v1.get_block_path(vault.vault_id, blockid)
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

    def CreateFile(self, vault):
        """
        Creates a file in the specified vault, does not post data to it
        Returns the location of the file which gives the file id
        """
        url = api_v1.get_files_path(vault.vault_id)
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

    def AssignBlocksToFile(self, vault, fileid, value):
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
        url = api_v1.get_file_path(vault.vault_id, fileid)
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

    def GetFileBlockList(self, vault, fileid, marker=None, limit=None):
        """
        Return the list of blocks assigned to the file
        This does not finalize the file.
        This function is returning a 404
        """

        url = api_v1.get_fileblocks_path(vault.vault_id, fileid)

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
