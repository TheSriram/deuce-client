"""
Deuce API
"""
import json
import requests
import logging

import deuceclient.api.vault as api_vault
import deuceclient.api.block as api_block
import deuceclient.api.v1 as api_v1
from deuceclient.common.command import Command


class DeuceClient(Command):
    """
    Object defining HTTP REST API calls for interacting with Deuce.
    """
    def __init__(self, authenticator, apihost, sslenabled=False):
        """Initialize the Deuce Client access

        :param authenticator: instance of deuceclient.auth.Authentication
                              to use for retrieving auth tokens
        :param apihost: server to use for API calls
        :param sslenabled: True if using HTTPS; otherwise false
        """
        super(self.__class__, self).__init__(apihost,
                                             '/',
                                             sslenabled=sslenabled)
        self.log = logging.getLogger(__name__)
        self.sslenabled = sslenabled
        self.authenticator = authenticator

    def __update_headers(self):
        """Update common headers
        """
        self.headers['X-Auth-Token'] = self.authenticator.AuthToken
        self.headers['X-Project-ID'] = self.project_id

    def __log_request_data(self):
        """Log the information about the request
        """
        self.log.debug('host: %s', self.apihost)
        self.log.debug('body: %s', self.Body)
        self.log.debug('headers: %s', self.Headers)
        self.log.debug('uri: %s', self.Uri)

    @property
    def project_id(self):
        """Return the project id to use
        """
        return self.authenticator.AuthTenantId

    def CreateVault(self, vault_name):
        """Create a vault

        :param vault_name: name of the vault
        :returns: deuceclient.api.Vault instance of the new Vault
        :raises: TypeError if vault_name is not a string object
        :raises: RunTimeError on failure
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
        :raises: TypeError if vault_name is not a string object
        :raises: RunTimeError on failure
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
        """Delete a Vault

        :param vault: vault of the vault to be deleted
        :returns: True on success
        :raises: TypeError if vault is not a Vault object
        :raises: RunTimeError on failure
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
        """Return the statistics on a Vault

        :param vault: Vault object for the vault or name of vault to
                      be verified
        :returns: True if the Vault exists; otherwise False
        :raises: RunTimeError on error
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
        """Return the statistics on a Vault

        :param vault: vault to get the statistics for
        :returns: True on success
        :raises: TypeError if vault is not a Vault object
        :raises: RunTimeError on failure
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
            return True
        else:
            raise RuntimeError(
                'Failed to get Vault statistics. '
                'Error ({0:}): {1:}'.format(res.status_code, res.text))

    def GetBlockList(self, vault, marker=None, limit=None):
        """Return the list of blocks in the vault

        :param vault: vault to get the block list for
        :param marker: marker denoting the start of the list
        :param limit: integer denoting the maximum entries to retrieve
        :raises: TypeError if vault is not a Vault object
        :raises: RunTimeError on failure
        """
        if not isinstance(vault, api_vault.Vault):
            raise TypeError('vault must be deuceclient.api.Vault')

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
                vault.blocks[block_entry] = api_block.Block(vault.project_id,
                                                            vault.vault_id,
                                                            block_entry)
            return True
        else:
            raise RuntimeError(
                'Failed to get Block list for Vault . '
                'Error ({0:}): {1:}'.format(res.status_code, res.text))

    def UploadBlock(self, vault, block):
        """Upload a block to the vault specified.

        :param vault: vault to upload the block into
        :param block: block to be uploaded
                      must be deuceclient.api.Block type
        """
        if not isinstance(vault, api_vault.Vault):
            raise TypeError('vault must be deuceclient.api.Vault')
        if not isinstance(block, api_block.Block):
            raise TypeError('block must be deuceclient.api.Block')

        url = api_v1.get_block_path(vault.vault_id, block.block_id)
        self.ReInit(self.sslenabled, url)
        self.__update_headers()
        self.__log_request_data()
        headers = {}
        headers.update(self.Headers)
        headers['content-type'] = 'application/octet-stream'
        headers['content-length'] = len(block)
        res = requests.put(self.Uri, headers=self.Headers, data=block.data)
        if res.status_code == 201:
            return True
        else:
            raise RuntimeError(
                'Failed to upload Block. '
                'Error ({0:}): {1:}'.format(res.status_code, res.text))

    def DeleteBlock(self, vault, blockid):
        """Delete the block from the vault.

        :param vault: vault to delete the block from
        :param blockid: the id (SHA-1) of the block to be deleted

        TODO: change this to Block functionality
        Note: blockid is not removed from the Vault object
        """
        if not isinstance(vault, api_vault.Vault):
            raise TypeError('vault must be deuceclient.api.Vault')

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
        """Gets the data associated with the block id provided

        :param vault: vault to download the block from
        :param blockid: the id (SHA-1) of the block to be downloaded

        TODO: change this to Block functionality
        """
        if not isinstance(vault, api_vault.Vault):
            raise TypeError('vault must be deuceclient.api.Vault')

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
        """Create a file

        :param vault: vault to create the file in
        :returns: the name of the file within the vault

        TODO: Update this to File functionality
        """
        if not isinstance(vault, api_vault.Vault):
            raise TypeError('vault must be deuceclient.api.Vault')

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
        """Assigns the specified block to a file

        :param vault: vault to containing the file
        :param fileid: fileid of the file in the vault that the block
                       will be assigned to
        :param vault: block information being assigned to the file

        TODO: Update this to the File+Blocks functionality

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
        if not isinstance(vault, api_vault.Vault):
            raise TypeError('vault must be deuceclient.api.Vault')

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
        """Return the list of blocks assigned to the file

        :param vault: vault to the file belongs to
        :param fileid: fileid of the file to list the blocks for
        :param marker: blockid within the list to start at
        :param limit: the maximum number of entries to retrieve

        TODO: Update this to File functionality
        """
        if not isinstance(vault, api_vault.Vault):
            raise TypeError('vault must be deuceclient.api.Vault')

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
