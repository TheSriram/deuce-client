"""
Tests - Deuce Client - Client - Deuce - File - Assign Blocks
"""
import json

import httpretty

import deuceclient.api as api
import deuceclient.client.deuce
from deuceclient.tests import *


class ClientDeuceFileTests(ClientTestBase):

    def setUp(self):
        super(ClientDeuceFileTests, self).setUp()

    def tearDown(self):
        super(ClientDeuceFileTests, self).tearDown()

    @httpretty.activate
    def test_file_assign_blocks(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        file_id = create_file()
        self.vault.files[file_id] = api.File(project_id=self.vault.project_id,
                                             vault_id=self.vault.vault_id,
                                             file_id=file_id)

        block_list = []

        running_offset = 0
        for block_id, block_data, block_size in \
                create_blocks(5):
            block = api.Block(project_id=self.vault.project_id,
                              vault_id=self.vault.vault_id,
                              block_id=block_id,
                              data=block_data)
            self.vault.blocks[block_id] = block
            self.vault.files[file_id].blocks[block_id] = block
            self.vault.files[file_id].offsets[running_offset] = block_id

            block_list.append((block_id, running_offset))

            running_offset = running_offset + block_size

        data = ['status']

        httpretty.register_uri(httpretty.POST,
                               get_file_url(self.apihost,
                                            self.vault.vault_id,
                                            file_id),
                               body=json.dumps(data),
                               status=200)

        self.assertEqual(data,
                         client.AssignBlocksToFile(self.vault,
                                                   file_id,
                                                   block_list))

    @httpretty.activate
    def test_file_assign_blocks_alternate(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        file_id = create_file()
        self.vault.files[file_id] = api.File(project_id=self.vault.project_id,
                                             vault_id=self.vault.vault_id,
                                             file_id=file_id)

        running_offset = 0
        for block_id, block_data, block_size in \
                create_blocks(5):
            block = api.Block(project_id=self.vault.project_id,
                              vault_id=self.vault.vault_id,
                              block_id=block_id,
                              data=block_data)
            self.vault.blocks[block_id] = block
            self.vault.files[file_id].blocks[block_id] = block
            self.vault.files[file_id].offsets[running_offset] = block_id

            running_offset = running_offset + block_size

        data = ['status']

        httpretty.register_uri(httpretty.POST,
                               get_file_url(self.apihost,
                                            self.vault.vault_id,
                                            file_id),
                               body=json.dumps(data),
                               status=200)

        self.assertEqual(data,
                         client.AssignBlocksToFile(self.vault,
                                                   file_id))

    def test_file_assign_blocks_bad_vault(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        file_id = create_file()
        self.vault.files[file_id] = api.File(project_id=self.vault.project_id,
                                             vault_id=self.vault.vault_id,
                                             file_id=file_id)

        block_list = []

        with self.assertRaises(TypeError):
            client.AssignBlocksToFile(self.vault.vault_id,
                                      file_id,
                                      block_list)

    def test_file_assign_blocks_bad_fileid(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        block_list = []
        file_id = create_file().encode()

        with self.assertRaises(TypeError):
            client.AssignBlocksToFile(self.vault,
                                      file_id,
                                      block_list)

    def test_file_assign_blocks_fileid_not_in_vault(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        file_id = create_file()

        block_list = []

        with self.assertRaises(KeyError):
            client.AssignBlocksToFile(self.vault,
                                      file_id,
                                      block_list)

    def test_file_assign_blocks_bad_blocklist(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        file_id = create_file()
        self.vault.files[file_id] = api.File(project_id=self.vault.project_id,
                                             vault_id=self.vault.vault_id,
                                             file_id=file_id)

        block_list = []

        with self.assertRaises(ValueError):
            client.AssignBlocksToFile(self.vault,
                                      file_id,
                                      block_list)

    @httpretty.activate
    def test_file_assign_blocks_not_in_vault_blocklist(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        file_id = create_file()
        self.vault.files[file_id] = api.File(project_id=self.vault.project_id,
                                             vault_id=self.vault.vault_id,
                                             file_id=file_id)

        block_list = []

        running_offset = 0
        for block_id, block_data, block_size in \
                create_blocks(5):
            block = api.Block(project_id=self.vault.project_id,
                              vault_id=self.vault.vault_id,
                              block_id=block_id,
                              data=block_data)
            self.vault.files[file_id].blocks[block_id] = block
            self.vault.files[file_id].offsets[running_offset] = block_id

            block_list.append((block_id, running_offset))

            running_offset = running_offset + block_size

        with self.assertRaises(KeyError):
            client.AssignBlocksToFile(self.vault,
                                      file_id,
                                      block_list)

    @httpretty.activate
    def test_file_assign_blocks_not_in_files_blocklist(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        file_id = create_file()
        self.vault.files[file_id] = api.File(project_id=self.vault.project_id,
                                             vault_id=self.vault.vault_id,
                                             file_id=file_id)

        block_list = []

        running_offset = 0
        for block_id, block_data, block_size in \
                create_blocks(5):
            block = api.Block(project_id=self.vault.project_id,
                              vault_id=self.vault.vault_id,
                              block_id=block_id,
                              data=block_data)
            self.vault.blocks[block_id] = block
            self.vault.files[file_id].offsets[running_offset] = block_id

            block_list.append((block_id, running_offset))

            running_offset = running_offset + block_size

        with self.assertRaises(KeyError):
            client.AssignBlocksToFile(self.vault,
                                      file_id,
                                      block_list)

    @httpretty.activate
    def test_file_assign_blocks_not_in_files_offsetlist(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        file_id = create_file()
        self.vault.files[file_id] = api.File(project_id=self.vault.project_id,
                                             vault_id=self.vault.vault_id,
                                             file_id=file_id)

        block_list = []

        running_offset = 0
        for block_id, block_data, block_size in \
                create_blocks(5):
            block = api.Block(project_id=self.vault.project_id,
                              vault_id=self.vault.vault_id,
                              block_id=block_id,
                              data=block_data)
            self.vault.blocks[block_id] = block
            self.vault.files[file_id].blocks[block_id] = block

            block_list.append((block_id, running_offset))

            running_offset = running_offset + block_size

        with self.assertRaises(KeyError):
            client.AssignBlocksToFile(self.vault,
                                      file_id,
                                      block_list)

    @httpretty.activate
    def test_file_assign_blocks_files_offsetlist_not_matching(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        file_id = create_file()
        self.vault.files[file_id] = api.File(project_id=self.vault.project_id,
                                             vault_id=self.vault.vault_id,
                                             file_id=file_id)

        block_list = []

        running_offset = 0
        for block_id, block_data, block_size in \
                create_blocks(5):
            block = api.Block(project_id=self.vault.project_id,
                              vault_id=self.vault.vault_id,
                              block_id=block_id,
                              data=block_data)
            self.vault.blocks[block_id] = block
            self.vault.files[file_id].blocks[block_id] = block
            block_id2, block_data2, block_size2 = create_block()

            self.vault.files[file_id].offsets[running_offset] = block_id2

            block_list.append((block_id, running_offset))

            running_offset = running_offset + block_size

        with self.assertRaises(ValueError):
            client.AssignBlocksToFile(self.vault,
                                      file_id,
                                      block_list)

    @httpretty.activate
    def test_file_assign_blocks_no_blocklist_no_offsets(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        file_id = create_file()
        self.vault.files[file_id] = api.File(project_id=self.vault.project_id,
                                             vault_id=self.vault.vault_id,
                                             file_id=file_id)

        running_offset = 0
        for block_id, block_data, block_size in \
                create_blocks(5):
            block = api.Block(project_id=self.vault.project_id,
                              vault_id=self.vault.vault_id,
                              block_id=block_id,
                              data=block_data)
            self.vault.blocks[block_id] = block
            self.vault.files[file_id].blocks[block_id] = block

            running_offset = running_offset + block_size

        with self.assertRaises(ValueError):
            client.AssignBlocksToFile(self.vault,
                                      file_id)

    @httpretty.activate
    def test_file_assign_blocks_no_blocklist_no_fileblocks(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        file_id = create_file()
        self.vault.files[file_id] = api.File(project_id=self.vault.project_id,
                                             vault_id=self.vault.vault_id,
                                             file_id=file_id)

        running_offset = 0
        for block_id, block_data, block_size in \
                create_blocks(5):
            block = api.Block(project_id=self.vault.project_id,
                              vault_id=self.vault.vault_id,
                              block_id=block_id,
                              data=block_data)
            self.vault.blocks[block_id] = block

            self.vault.files[file_id].offsets[running_offset] = block_id

            running_offset = running_offset + block_size

        with self.assertRaises(ValueError):
            client.AssignBlocksToFile(self.vault,
                                      file_id)

    @httpretty.activate
    def test_file_assign_blocks_no_blocklist_not_in_fileblocks(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        file_id = create_file()
        self.vault.files[file_id] = api.File(project_id=self.vault.project_id,
                                             vault_id=self.vault.vault_id,
                                             file_id=file_id)

        running_offset = 0
        for block_id, block_data, block_size in \
                create_blocks(5):
            block = api.Block(project_id=self.vault.project_id,
                              vault_id=self.vault.vault_id,
                              block_id=block_id,
                              data=block_data)
            self.vault.blocks[block_id] = block
            self.vault.files[file_id].blocks[block_id] = block

            block_id2, block_data2, block_size2 = create_block()

            self.vault.files[file_id].offsets[running_offset] = block_id2

            running_offset = running_offset + block_size

        with self.assertRaises(KeyError):
            client.AssignBlocksToFile(self.vault,
                                      file_id)

    @httpretty.activate
    def test_file_assign_blocks_failed(self):
        client = deuceclient.client.deuce.DeuceClient(self.authenticator,
                                                      self.apihost,
                                                      sslenabled=True)

        file_id = create_file()
        self.vault.files[file_id] = api.File(project_id=self.vault.project_id,
                                             vault_id=self.vault.vault_id,
                                             file_id=file_id)

        block_list = []

        running_offset = 0
        for block_id, block_data, block_size in create_blocks(5):
            block = api.Block(project_id=self.vault.project_id,
                              vault_id=self.vault.vault_id,
                              block_id=block_id,
                              data=block_data)
            self.vault.blocks[block_id] = block
            self.vault.files[file_id].blocks[block_id] = block
            self.vault.files[file_id].offsets[running_offset] = block_id

            block_list.append((block_id, running_offset))

            running_offset = running_offset + block_size

        httpretty.register_uri(httpretty.POST,
                               get_file_url(self.apihost,
                                            self.vault.vault_id,
                                            file_id),
                               content_type='text/plain',
                               body="mock failure",
                               status=404)

        with self.assertRaises(RuntimeError) as stats_error:
            client.AssignBlocksToFile(self.vault, file_id, block_list)
