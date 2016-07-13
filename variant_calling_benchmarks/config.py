import os
import logging
import string
import json

def load_config(*filenames):
    '''
    Load the specified JSON config files.

    Returns
    ---------
    MergedConfigDicts
    '''
    dicts = []
    substitutions = {}
    for filename in filenames:
        try:
            def object_hook(dictionary):
                return ConfigDict(filename, substitutions, dictionary)

            with open(filename) as fd:
                d = json.load(fd, object_hook=object_hook)
                substitutions.update(d.get("substitutions", {}))
                dicts.append(d)
        except Exception as e:
            logging.warn(
                "Error loading config %s: %s" % (filename, str(e)))
            raise

    return MergedConfigDicts(dicts)

class MergedConfigDicts(dict):
    '''

    '''
    def __init__(self, dicts):
        self.key_to_dict = {}
        for d in dicts:
            self.update(d)
            for key in d.keys():
                self.key_to_dict[key] = d

    def get_substituted(self, key, path=False):
        return self.key_to_dict[key].get_substituted(key, path=path)

class ConfigDict(dict):
    '''
    Thin wrapper over a dict that adds the get_substituted method.
    '''
    def __init__(self, filename, substitutions, dictionary):
        '''
        Parameters
        -------------
        filename : string
            File this dict was loaded from.

        substitutions : dict
            String substitutions that should be used when get_substituted
            is called.

        dictionary : dict
            The dictionary to be wrapped.
        '''
        self.filename = filename
        self.substitutions = substitutions
        self.basedir = os.path.dirname(self.filename)
        self.update(dictionary)

    def get_substituted(self, key, path=False):
        '''
        Return the specified key from the dict after running string
        substitutions.

        Parameters
        -------------
        path : boolean
            If True, then after template expansion on the value, it will
            additionally be treated as a path. If it is relative (does not
            start with /) then the directory of the JSON file the Config was
            loaded from will be preprended to it.

        Returns
        -------------
        string, number, list, or ConfigDict giving the requested value

        '''
        value = self[key]
        result = string.Template(value).substitute(**self.substitutions)
        if path:
            result = os.path.join(self.basedir, result)
        return result

