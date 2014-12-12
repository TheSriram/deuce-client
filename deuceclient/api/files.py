"""
Deuce Client - Files API
"""
import json

from stoplight import validate

from deuceclient.api.afile import File
from deuceclient.common.validation import *


class Files(dict):
    """
    A collection of files
    """

    @validate(project_id=ProjectIdRule, vault_id=VaultIdRule)
    def __init__(self, project_id, vault_id):
        super(Files, self).__init__()
        self.__properties = {
            'marker': None,
            'project_id': project_id,
            'vault_id': vault_id
        }

    def to_json(self):
        return json.dumps({
            'marker': self.marker,
            'project_id': self.project_id,
            'vault_id': self.vault_id,
            'files': {
                file_id: self[file_id].to_json()
                for file_id in self.keys()
            }
        })

    def from_json(self, serialized_data):
        json_data = json.loads(serialized_data)
        self.marker = json_data['marker']
        self.__properties['project_id'] = json_data['project_id']
        self.__properties['vault_id'] = json_data['vault_id']
        self.update({
            k: File.from_json(v)
            for k, v in json_data['files'].items()
        })

    @validate(key=FileIdRule)
    def __getitem__(self, key):
        return dict.__getitem__(self, key)

    @validate(key=FileIdRule)
    def __setitem__(self, key, val):
        if isinstance(val, File):
            return dict.__setitem__(self, key, val)
        else:
            raise TypeError(
                '{0} can only contain Files'.format(self.__class__))

    @property
    def project_id(self):
        return self.__properties['project_id']

    @property
    def vault_id(self):
        return self.__properties['vault_id']

    @property
    def marker(self):
        return self.__properties['marker']

    @marker.setter
    @validate(value=FileIdRuleNoneOkay)
    def marker(self, value):
        # Note: We could force that "marker" is in the dict;
        #   but then that would also unnecessarily force
        #   order-of-operations on how to use the object
        self.__properties['marker'] = value

    def __repr__(self):
        return '{0}: {1}'.format(type(self).__name__,
                                 dict.__repr__(self))

    def update(self, *args, **kwargs):
        # For use of Files.__setitem__ in order
        # to get validation of each entry in the incoming dictionary
        for k, v in dict(*args, **kwargs).items():
            self[k] = v
