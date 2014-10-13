"""
Testing - Deuce Client - API - Vault
"""
from unittest import TestCase

import deuceclient.api.project as p
import deuceclient.api.vault as v
import deuceclient.common.errors as errors
from deuceclient.tests import *


class ProjectTest(TestCase):

    def setUp(self):
        super(self.__class__, self).setUp()

        self.project_id = create_project_name()

    def test_create_project(self):

        project = p.Project(self.project_id)
        self.assertEqual(project.project_id, self.project_id)

    def test_create_project_bad_type(self):

        with self.assertRaises(TypeError):
            project = p.Project(bytes(self.project_id))

    def test_project_name_invalid(self):

        with self.assertRaises(errors.InvalidProject):
            project = p.Project(self.project_id + '$')

    def test_project_add_vault(self):

        project = p.Project(self.project_id)

        vault = v.Vault(self.project_id,
                        create_vault_name())

        project[vault.vault_id] = vault

        self.assertEqual(vault, project[vault.vault_id])

    def test_project_add_vault_invalid(self):
        project = p.Project(self.project_id)

        with self.assertRaises(errors.InvalidVault):
            project[create_vault_name() + '$'] = {}

    def test_project_get_vault_invalid(self):
        project = p.Project(self.project_id)

        with self.assertRaises(errors.InvalidVault):
            v = project[create_vault_name() + '$']

    def test_project_update_vault(self):

        project = p.Project(self.project_id)
        vaults = {
            x: v.Vault(self.project_id, x) for x in [create_vault_name()]
        }

        project.update(vaults)

        for k, vt in vaults.items():
            self.assertEqual(vt, project[k])

    def test_project_update_vault_invalid(self):

        project = p.Project(self.project_id)
        vaults = {
            x: x for x in [create_vault_name()]
        }

        with self.assertRaises(TypeError):
            project.update(vaults)

    def test_repr(self):
        project = p.Project(self.project_id)

        serialized_project = repr(project)
