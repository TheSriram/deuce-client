"""
Deuce Client - (File) Splitter API
"""
import abc

from deuceclient.api import Block, Blocks


class FileSplitterBase(object):
    """
    File Splitter Interface Class
    """
    __metaclass__ = abc.ABCMeta

    def __init__(self, project_id, vault_id, input_io):
        """
        :param input_io: file-like object providing a read function
        """
        self.__project_id = project_id
        self.__vault_id = vault_id
        self.__input_stream = input_io
        self.__state = None

    @property
    def state(self):
        return self.__state

    def _set_state(self, new_state):
        if new_state in (None, 'processing'):
            self.__state = new_state
        else:
            raise ValueError('Invalid state - {0}'.format(new_state))

    @property
    def project_id(self):
        return self.__project_id

    @property
    def vault_id(self):
        return self.__vault_id

    @property
    def input_stream(self):
        return self.__input_stream

    @input_stream.setter
    def input_stream(self, input_io):
        if self.state is None and 'read' in dir(input_io):
            self.__input_stream = input_io
        elif 'read' not in dir(input_io):
            raise TypeError('input_io must have a read method')
        else:
            raise RuntimeError('Invalid state to set new input_stream')

    def get_blocks(self, count):
        """Get a series of blocks

        :returns: api.Blocks containing the data
        """
        blocks = Blocks(self.project_id,
                        self.vault_id)

        for block in [self.get_block() for _ in range(count)]:
            if block is not None:
                blocks[block.block_id] = block

        return blocks

    def _make_block(self, data):
        block_id = Block.make_id(data)
        return Block(self.project_id, self.vault_id, block_id, data=data)

    @abc.abstractmethod
    def configure(self, config):
        """Configure the splitter

        :param config: dict - dictionary of the configuration values
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def get_block(self):
        """Get a block

        :returns: api.Block containing the data
        """
        raise NotImplementedError()
