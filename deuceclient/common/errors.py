"""
Deuce Client: Errors
"""


class DeuceClientExceptions(Exception):
    """Generic Deuce Client Exception
    """
    pass


class InvalidProject(DeuceClientExceptions):
    """Deuce Client Project-ID Exceptions
    """
    pass


class InvalidVault(DeuceClientExceptions):
    """Deuce Client Vault Exceptions
    """
    pass


class InvalidFiles(DeuceClientExceptions):
    """Deuce Client File Exceptions
    """
    pass


class InvalidBlocks(DeuceClientExceptions):
    """Deuce Client (metadata) Block Exceptions
    """
    pass


class InvalidStorageBlocks(InvalidBlocks):
    """Deuce Client Storage Block Exceptions
    """
    pass


class ParameterConstraintError(DeuceClientExceptions):
    """Parameter Constraint Error
    """
    pass


class IterableContentError(DeuceClientExceptions):
    """Iterable Content Exception
    """
    pass


class InvalidApiObjectInstance(TypeError):
    """Parameter Type Error
    """
    pass


class InvalidVaultInstance(InvalidApiObjectInstance):
    """Invalid Vault Object Instance
    """
    pass


class InvalidBlockInstance(InvalidApiObjectInstance):
    """Invalid Block Object Instance
    """
    pass


class InvalidBlockType(InvalidApiObjectInstance):
    """Invalid Block Type
    """
    pass


class InvalidStorageBlockType(InvalidBlockType):
    """Invalid Storage Block Type
    """
    pass

class InvalidMetadataBlockType(InvalidBlockType):
    """Invalid Storage Block Type
    """
    pass
