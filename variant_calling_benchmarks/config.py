import os
import logging
import string
import json
import functools
import pprint

def substitute(value, variables, raise_on_keyerror=True):
    """
    Interpolate a string like "Hello $NAME" in the context of the given
    variables (like: {"NAME": "John"}.

    The substition values may themselves have substitutions. The interpolation
    is run repeatedly until a fixed point is reached.

    Parameters
    -----------
    value : string
        The string to be substituted

    variables : dict
        The variables to substitute

    raise_on_keyerror : boolean
        If True, a KeyError is raised if a substitution is not found in
        variables. If False, the substitution is left in the string and no
        error is raised.

    """
    result = None
    original = value
    i = 0
    while True:
        template = string.Template(original)
        if raise_on_keyerror:
            result = template.substitute(**variables)
        else:
            result = template.safe_substitute(**variables)
        if result == original:
            # No changes.
            break
        original = result
        if i > 100:
            raise RuntimeError(
                "Substitution not terminating: %s" % value)
        i += 1
    return result

def recursive_substitute(node, variables, raise_on_keyerror=True):
    """
    Substitute strings recursively in a data structure of dicts, lists, and
    strings.
    """
    return recursive_map(
        node, functools.partial(
            substitute,
            raise_on_keyerror=raise_on_keyerror,
            variables=variables))

def recursive_map(node, function):
    """
    Run the given function on strings in a data structure of dicts, lists,
    and strings, and return the result.
    """
    if isinstance(node, dict):
        return dict(
            (key, recursive_map(value, function))
            for (key, value) in node.items())
    if isinstance(node, list):
        return [recursive_map(value, function) for value in node]
    return function(node)

def load_config(*filenames):
    '''
    Load, merge, and interpolate the specified JSON config files.

    Returns
    ---------
    dict
    '''
    substitutions = {}
    merged = {}
    for filename in filenames:
        try:
            with open(filename) as fd:
                d = json.load(fd)

                # We substitute the special THIS_DIR substitution immediately,
                # since its value depends on the filename.
                d = recursive_substitute(d, {
                    'THIS_DIR': os.path.dirname(os.path.abspath(filename))
                }, raise_on_keyerror=False)
                substitutions.update(d.get("substitutions", {}))
                merged.update(d)
        except Exception as e:
            logging.warn(
                "Error loading config %s: %s" % (filename, str(e)))
            raise

    substituted = recursive_substitute(merged, substitutions)
    logging.info("Loaded config from files %s:\n%s" % (
        " ".join(filenames),
        pprint.pformat(substituted)))

    return substituted
