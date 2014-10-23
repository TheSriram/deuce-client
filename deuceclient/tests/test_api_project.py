"""
Testing - Deuce Client - API - Project
"""
from unittest import TestCase

import deuceclient.api as api
import deuceclient.common.errors as errors
import deuceclient.common.validation as val
from deuceclient.tests import *


class ProjectTest(TestCase):

    def setUp(self):
        super(self.__class__, self).setUp()

        self.project_id = create_project_name()

    def test_create_project(self):

        project = api.Project(self.project_id)
        self.assertEqual(project.project_id, self.project_id)

    def test_create_project_bad_type(self):

        with self.assertRaises(TypeError):
            project = api.Project(bytes(self.project_id))

    def test_project_name_invalid(self):

        with self.assertRaises(errors.InvalidProject):
            project = api.Project(self.project_id + '$')

        # Build project name that is too long
        x = self.project_id
        while len(x) < (val.PROJECT_ID_MAX_LEN + 1):
            x = '{0}_{1}'.format(x, self.project_id)

        with self.assertRaises(errors.InvalidProject):
            project = api.Project(x)

    def test_project_add_vault(self):

        project = api.Project(self.project_id)

        vault = api.Vault(self.project_id,
                        create_vault_name())

        project[vault.vault_id] = vault

        self.assertEqual(vault, project[vault.vault_id])

    def test_project_add_vault_invalid(self):
        project = api.Project(self.project_id)

        with self.assertRaises(errors.InvalidVault):
            project[create_vault_name() + '$'] = {}

    def test_project_get_vault_invalid(self):
        project = api.Project(self.project_id)

        with self.assertRaises(errors.InvalidVault):
            v = project[create_vault_name() + '$']

    def test_project_update_vault(self):

        project = api.Project(self.project_id)
        vaults = {
            x: api.Vault(self.project_id, x) for x in [create_vault_name()]
        }

        project.update(vaults)

        for k, vt in vaults.items():
            self.assertEqual(vt, project[k])

    def test_project_update_vault_invalid(self):

        project = api.Project(self.project_id)
        vaults = {
            x: x for x in [create_vault_name()]
        }

        with self.assertRaises(TypeError):
            project.update(vaults)

    def test_repr(self):
        project = api.Project(self.project_id)

        serialized_project = repr(project)
