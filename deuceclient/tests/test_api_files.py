"""
Testing - Deuce Client - API Files
"""
from unittest import TestCase

import deuceclient.api as api
import deuceclient.common.errors as errors
import deuceclient.common.validation as val
from deuceclient.tests import *


class FileTest(TestCase):

    def setUp(self):
        super(self.__class__, self).setUp()

        self.project_id = create_project_name()
        self.vault_id = create_vault_name()
        self.file_id = create_file()

    def test_create_files(self):
        files = api.Files()

    def test_add_file(self):
        a_file = api.File(self.project_id, self.vault_id, self.file_id)

        files = api.Files()
        files[a_file.file_id] = a_file

        self.assertEqual(a_file, files[a_file.file_id])

    def test_add_invalid_file(self):
        a_file = api.File(self.project_id, self.vault_id, self.file_id)

        files = api.Files()

        with self.assertRaises(errors.InvalidFiles):
            files['jessie'] = a_file

        with self.assertRaises(TypeError):
            files[a_file.file_id] = 'james'

    def test_repr(self):
        files = api.Files()

        serialized_files = repr(files)

    def test_repr_with_data(self):
        a_file = api.File(self.project_id, self.vault_id, self.file_id)

        files = api.Files()
        files[a_file.file_id] = a_file

        serialized_files = repr(files)

    def test_update(self):
        a_file = api.File(self.project_id, self.vault_id, self.file_id)

        files = api.Files()
        files.update({
            a_file.file_id: a_file
        })

        self.assertEqual(a_file, files[a_file.file_id])

    def test_update_invalid_file(self):
        a_file = api.File(self.project_id, self.vault_id, self.file_id)

        files = api.Files()

        with self.assertRaises(errors.InvalidFiles):
            files.update({
                'jessie': a_file
            })

        with self.assertRaises(TypeError):
            files.update({
                a_file.file_id: 'james'
            })
