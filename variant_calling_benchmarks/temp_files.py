import os
import tempfile

TEMPORARY_FILES = []

def tempfile_path(prefix='', suffix='.data', contents=''):
    '''
    Return a path to a new temporary file. The caller is responsible for
    deleting the file when finished.
    '''
    fd = tempfile.NamedTemporaryFile(
            prefix='tmp_variant_calling_benchmarks_' + prefix,
            suffix=suffix,
            delete=False)
    fd.write(contents)
    fd.close()
    TEMPORARY_FILES.append(fd.name)
    return fd.name

def finished(delete=True):
    '''
    Print the names of temporary files and delete them if delete=True.

    Call this when the process is finishing.
    '''
    for filename in TEMPORARY_FILES:
        if delete:
            print("Deleting: %s" % filename)
            os.unlink(filename)
        else:
            print("Not deleting: %s" % filename)
