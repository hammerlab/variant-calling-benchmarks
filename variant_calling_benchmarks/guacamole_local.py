'''
Run guacamole on a benchmark locally.
'''

import sys
import os
import argparse
import subprocess
import logging

from .config import load_config
from . import temp_files
from . import common
from .joint_caller import invoke
from .joint_caller import process_results

parser = argparse.ArgumentParser(description=__doc__)
common.add_common_run_args(parser)

def run(argv=sys.argv[1:]):
    args = parser.parse_args(argv)
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
    for patient in patients:
        out_vcf = os.path.join(
            args.out_dir,
            "out.%s.%s.vcf" % (config['benchmark'], patient))
        patient_to_vcf[patient] = out_vcf
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
            invoke.make_arguments(config, patient, out_vcf))

        if args.skip_guacamole:
            logging.info("Skipping guacamole run with arguments %s" % str(
                invocation))
        else:
            logging.info("Running guacamole with arguments %s" % str(
                invocation))
            subprocess.check_call(invocation)

    process_results.write_merged_calls(args, config, patient_to_vcf)
