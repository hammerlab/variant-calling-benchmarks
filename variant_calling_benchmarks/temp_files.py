import os
import tempfile

TEMPORARY_FILES = []

def tempfile_path(prefix='', suffix='.data', contents=''):
    fd = tempfile.NamedTemporaryFile(
            prefix='tmp_variant_calling_benchmarks_' + prefix,
            suffix=suffix,
            delete=False)
    fd.write(contents)
    fd.close()
    TEMPORARY_FILES.append(fd.name)
    return fd.name

def finished(delete=True):
    for filename in TEMPORARY_FILES:
        if delete:
            print("Deleting: %s" % filename)
            os.unlink(filename)
        else:
            print("Not deleting: %s" % filename)
