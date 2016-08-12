'''
Run guacamole on a benchmark locally.
'''

import sys
import os
import argparse
import subprocess
import logging
import time

from .config import load_config
from . import temp_files
from . import common
from .joint_caller import invoke
from .joint_caller import process_results

parser = argparse.ArgumentParser(description=__doc__)
common.add_common_run_args(parser)

def run(argv=sys.argv[1:]):
    args = parser.parse_args(argv)
    temp_files.TEMP_DIR = args.out_dir
    config = load_config(*args.configs)
    try:
        main(args, config)
    finally:
        temp_files.finished(not args.keep_temp_files)

def main(args, config):
    patients = args.patient if args.patient else sorted(config['patients'])

    patient_to_vcf = {}
    if not os.path.exists(args.out_dir):
        os.mkdir(args.out_dir)
    elapsed = {}
    for patient in patients:
        out_vcf = os.path.join(
            args.out_dir,
            "out.%s.%s.vcf" % (config['benchmark'], patient))
        logging.info("Running on patient %s outputting to %s" % (
            patient, out_vcf))

        invocation = (
            ["java"] +
            config.get("java_arguments") + 
            ["-cp", ":".join([
                args.guacamole_jar,
                args.guacamole_dependencies_jar
            ])] +
            ["org.hammerlab.guacamole.Main"] +
            invoke.make_arguments(
                config, patient, out_vcf, include_filtered=not args.only_passing))

        if args.skip_guacamole:
            logging.info("Skipping guacamole run with arguments %s" % str(
                invocation))
            result_vcf = common.compress_file(out_vcf, dry_run=True)
        else:
            logging.info("Running guacamole with arguments %s" % str(
                invocation))
            start = time.time()
            subprocess.check_call(invocation)
            elapsed[patient] = time.time() - start
            logging.info("Ran in %0.1f seconds." % elapsed[patient])
            result_vcf = common.compress_file(out_vcf)

        patient_to_vcf[patient] = result_vcf

    extra = {
        'guacamole_elapsed_seconds': elapsed,
        'environment_variables': dict(os.environ),
    }
    process_results.write_results(args, config, patient_to_vcf, extra=extra)
