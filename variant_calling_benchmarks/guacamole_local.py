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
from . import joint_caller

parser = argparse.ArgumentParser(description=__doc__)

parser.add_argument("configs", nargs="+", help="JSON config files")
parser.add_argument("--guacamole-jar", required=True)
parser.add_argument("--patient", nargs="+",
    help="One or more patients to run. Default: all patients are run.")
parser.add_argument("--out-dir", required=True)
parser.add_argument("--keep-temp-files", action="store_true", default=False,
    help="Don't delete temporary files.")
parser.add_argument("--skip-guacamole", action="store_true", default=False,
    help="Don't actually run guacamole")

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
            config.get("java_args") + 
            [
                "-D%s=%s" % (key, value)
                for (key, value)
                in config.get("spark_configuration", {}).items()
            ] + 
            ["-cp", args.guacamole_jar, "org.hammerlab.guacamole.Main"] +
            joint_caller.make_arguments(config, patient, out_vcf))

        if args.skip_guacamole:
            logging.info("Skipping guacamole run with arguments %s" % str(
                invocation))
        else:
            logging.info("Running guacamole with arguments %s" % str(
                invocation))
            subprocess.check_call(invocation)
            temp_files.finished(not args.keep_temp_files)

    guacamole_calls = joint_caller.load_results(patient_to_vcf)
    guacamole_calls_csv = os.path.join(
            args.out_dir,
            "guacamole_calls.%s.csv" % (config['benchmark']))
    guacamole_calls.to_csv(guacamole_calls_csv, index=False)
    print("Wrote: %s" % guacamole_calls_csv)

    merged_calls = joint_caller.merge_calls_with_others(
        config, guacamole_calls)
    merged_calls_csv = os.path.join(
            args.out_dir,
            "merged_calls.%s.csv" % (config['benchmark']))
    merged_calls.to_csv(merged_calls_csv, index=False)
    print("Wrote: %s" % merged_calls_csv)

    summary = joint_caller.summary_stats(
        config, merged_calls)
    summary_csv = os.path.join(
            args.out_dir,
            "summary.%s.csv" % (config['benchmark']))
    summary.to_csv(summary_csv, index=False)
    print("Wrote: %s" % summary_csv)

