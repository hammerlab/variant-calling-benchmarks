import subprocess
import logging

def copy_to_google_storage_bucket(
        filename,
        bucket,
        no_clobber=True,
        raise_on_error=False):

    cp_args = []
    if no_clobber:
        cp_args.append("-n")
    gsutil_call = ["gsutil", "-m", "cp"] + cp_args + [filename, bucket]
    logging.info("Copying %s to bucket %s: %s" % (
        filename, bucket, str(gsutil_call)))
    try:
        subprocess.check_call(gsutil_call)
    except Exception as e:
        logging.warn("Error copying to bucket: %s %s" % (type(e), str(e)))
        if raise_on_error:
            raise
