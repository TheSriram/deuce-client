"""
Deuce Client: Validation Functionality
"""
import re

from stoplight import Rule, ValidationFailed, validation_function

import deuceclient.common.errors as errors

PROJECT_ID_MAX_LEN = 128
VAULT_ID_MAX_LEN = 128
OPENSTRING_REGEX = re.compile('^[a-zA-Z0-9_\-]+$')
PROJECT_ID_REGEX = OPENSTRING_REGEX
VAULT_ID_REGEX = OPENSTRING_REGEX
METADATA_BLOCK_ID_REGEX = re.compile('\\b[0-9a-f]{40}\\b')
UUID_REGEX = re.compile(
    '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}')
FILE_ID_REGEX = UUID_REGEX
STORAGE_BLOCK_ID_REGEX = UUID_REGEX
OFFSET_REGEX = re.compile(
    '(?<![-.])\\b[0-9]+\\b(?!\\.[0-9])')
LIMIT_REGEX = re.compile(
    '(?<![-.])\\b[0-9]+\\b(?!\\.[0-9])')


@validation_function
def val_project_id(value):
    if not PROJECT_ID_REGEX.match(value):
        raise ValidationFailed('Invalid project id {0}'.format(value))

    if len(value) > PROJECT_ID_MAX_LEN:
        raise ValidationFailed('Project ID exceeded max len {0}'.format(
            VAULT_ID_MAX_LEN))


@validation_function
def val_vault_id(value):
    if not VAULT_ID_REGEX.match(value):
        raise ValidationFailed('Invalid vault id {0}'.format(value))

    if len(value) > VAULT_ID_MAX_LEN:
        raise ValidationFailed('Vault ID exceeded max len {0}'.format(
            VAULT_ID_MAX_LEN))


@validation_function
def val_file_id(value):
    if not FILE_ID_REGEX.match(value):
        raise ValidationFailed('Invalid File ID {0}'.format(value))


@validation_function
def val_metadata_block_id(value):
    if not METADATA_BLOCK_ID_REGEX.match(value):
        raise ValidationFailed('Invalid Block ID {0}'.format(value))


@validation_function
def val_storage_block_id(value):
    if not STORAGE_BLOCK_ID_REGEX.match(value):
        raise ValidationFailed('Invalid Storage Block ID {0}'.format(value))


@validation_function
def val_offset(value):
    if not OFFSET_REGEX.match(value):
        raise ValidationFailed('Invalid offset {0}'.format(value))


@validation_function
def val_limit(value):
    if not LIMIT_REGEX.match(value):
        raise ValidationFailed('Invalid limit {0}'.format(value))


@validation_function
def val_bool(value):
    if not isinstance(value, bool):
        raise ValidationFailed('Invalid type {0} instead of '
                               'Bool'.format(type(value)))


def _abort(error_code):
    abort_errors = {
        100: errors.InvalidProject,
        200: errors.InvalidVault,
        300: errors.InvalidFiles,
        400: errors.InvalidBlocks,
        500: errors.InvalidStorageBlocks,
        600: errors.ParameterConstraintError,
    }
    raise abort_errors[error_code]


# Parameter Rules
ProjectIdRule = Rule(val_project_id(), lambda: _abort(100))
VaultIdRule = Rule(val_vault_id(), lambda: _abort(200))
FileIdRule = Rule(val_file_id(), lambda: _abort(300))
MetadataBlockIdRule = Rule(val_metadata_block_id(), lambda: _abort(400))
MetadataBlockIdRuleNoneOkay = Rule(val_metadata_block_id(none_ok=True),
                                   lambda: _abort(400))
StorageBlockIdRule = Rule(val_storage_block_id(), lambda: _abort(500))
OffsetRule = Rule(val_offset(), lambda: _abort(600))
LimitRule = Rule(val_limit(), lambda: _abort(600))
BoolRule = Rule(val_bool(), lambda: _abort(600))
StorageBlockIdRuleNoneOkay = Rule(val_storage_block_id(none_ok=True),
                                  lambda: _abort(500))
