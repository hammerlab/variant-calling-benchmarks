import os
import logging
import string
import json
import functools

def load_config(*filenames):
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
    def __init__(self, dicts):
        self.key_to_dict = {}
        for d in dicts:
            self.update(d)
            for key in d.keys():
                self.key_to_dict[key] = d

    def get_substituted(self, key, path=False):
        return self.key_to_dict[key].get_substituted(key, path=path)

class ConfigDict(dict):
    def __init__(self, filename, substitutions, dictionary):
        self.filename = filename
        self.substitutions = substitutions
        self.basedir = os.path.dirname(self.filename)
        self.update(dictionary)

    def get_substituted(self, key, path=False):
        value = self[key]
        result = string.Template(value).substitute(**self.substitutions)
        if path:
            result = os.path.join(self.basedir, result)
        return result

