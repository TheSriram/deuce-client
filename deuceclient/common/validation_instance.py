"""
Deuce Client: Instance Validation Functionality
"""
from stoplight import Rule, ValidationFailed, validation_function

import deuceclient.api as api
import deuceclient.common.errors as errors


@validation_function
def val_vault_instance(value):
    if not isinstance(value, api.Vault):
        raise ValidationFailed('vault must be deuceclient.api.Vault')


@validation_function
def val_block_instance(value):
    if not isinstance(value, api.Block):
        raise ValidationFailed('block must be deuceclient.api.Block')


def _abort_instance(error_code):
    abort_errors = {
        # 101: errors.InvalidProject,
        201: errors.InvalidVaultInstance,
        # 301: errors.InvalidFiles,
        # 401: errors.InvalidBlocks,
        402: errors.InvalidBlockInstance,
        # 501: errors.InvalidStorageBlocks,
    }
    raise abort_errors[error_code]

VaultInstanceRule = Rule(val_vault_instance(), lambda: _abort_instance(201))
BlockInstanceRule = Rule(val_block_instance(), lambda: _abort_instance(402))
