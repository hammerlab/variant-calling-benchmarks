'''
Run guacamole on a benchmark locally

'''

import sys
import os
import argparse
import collections
import tempfile
import subprocess
import logging

import pandas

from .config import load_config

parser = argparse.ArgumentParser(description=__doc__)

parser.add_argument("configs", nargs="+")
parser.add_argument("--guacamole-jar", required=True)
parser.add_argument("--patient", nargs="+")
parser.add_argument("--out-dir", required=True)
parser.add_argument("--keep-temp-files", action="store_true", default=False)

TEMPORARY_FILES = []

def extract_loci_string(variant_filenames):
    loci = []
    for filename in variant_filenames:
        df = pandas.read_csv(filename)
        for (i, row) in df.iterrows():
            loci.append("%s:%d-%d" % (
                row["contig"], row["interbase_start"], row["interbase_end"]))
    return ",\n".join(loci)

def make_joint_caller_arguments(config, patient, out_vcf):
    patient_dict = config["patients"][patient]
    reads = collections.OrderedDict(patient_dict["reads"])
    only_somatic = all(
        x['kind'] == 'somatic' for x in patient_dict['variants'].values())
    force_call_loci_string = extract_loci_string([
        x.get_substituted('path', path=True)
        for x in patient_dict['variants'].values()
    ])

    force_call_loci_fd = tempfile.NamedTemporaryFile(
            prefix='tmp_variant_calling_benchmarks_loci_',
            suffix=".txt",
            delete=False)
    TEMPORARY_FILES.append(force_call_loci_fd.name)
    force_call_loci_fd.write(force_call_loci_string)
    force_call_loci_fd.close()

    arguments = ["somatic-joint"]
    arguments.extend(
        x.get_substituted('path', path=True) for x in reads.values())
    arguments.extend(
        ["--tissue-types"] + [x['tissue_type'] for x in reads.values()])
    arguments.extend(
        ["--analytes"] + [x['analyte'] for x in reads.values()])
    arguments.extend(
        ["--sample-names"] + list(reads.keys()))

    arguments.append("--include-filtered")
    if only_somatic:
        arguments.append("--only-somatic")

    arguments.extend(["--force-call-loci-from-file", force_call_loci_fd.name])
    arguments.extend(
        ["--reference-fasta", config.get_substituted("reference", path=True)])
    arguments.extend(config.get("guacamole_arguments", []))

    return arguments

def run(argv=sys.argv[1:]):
    args = parser.parse_args(argv)

    config = load_config(*args.configs)

    print(config)

    patients = args.patient if args.patient else sorted(config['patients'])

    for patient in patients:
        out_vcf = os.path.join(
            args.out_dir,
            "out.%s.%s.vcf" % (config['benchmark'], patient))
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
            make_joint_caller_arguments(config, patient, out_vcf))

        print("***** RUNNING GUACAMOLE WITH THESE ARGUMENTS ****")
        print(invocation)
        try:
            subprocess.check_call(invocation)
        finally:
            for filename in TEMPORARY_FILES:
                if args.keep_temp_files:
                    print("Not deleting: %s" % filename)
                else:
                    print("Deleting: %s" % filename)
                    os.unlink(filename)

