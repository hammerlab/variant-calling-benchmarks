import os
import tempfile
import logging

TEMPORARY_FILES = []

def tempfile_path(prefix='', suffix='.data', contents=''):
    '''
    Return a path to a new temporary file. The caller is responsible for
    deleting the file when finished.
    '''
    fd = tempfile.NamedTemporaryFile(
            prefix='tmp_variant_calling_benchmarks_' + prefix,
            suffix=suffix,
            mode='w',
            delete=False)
    fd.write(contents)
    fd.close()
    TEMPORARY_FILES.append(fd.name)
    logging.info("Created temporary file: %s" % fd.name)
    return fd.name

def finished(delete=True):
    '''
    Print the names of temporary files and delete them if delete=True.

    Call this when the process is finishing.
    '''
    global TEMPORARY_FILES
    for filename in TEMPORARY_FILES:
        if delete:
            logging.info("Deleting: %s" % filename)
            os.unlink(filename)
        else:
            logging.info("Not deleting: %s" % filename)
        TEMPORARY_FILES = []
