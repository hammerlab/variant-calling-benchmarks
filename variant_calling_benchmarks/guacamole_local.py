'''
Run guacamole on a benchmark locally

'''

import sys
import argparse
import json
import collections

parser = argparse.ArgumentParser(description=__doc__)

parser.add_argument("configs", nargs="+")
parser.add_argument("--guacamole-jar", required=True)

def load_config(config_filenames):
    config = {}
    for filename in config_filenames:
        with open(filename) as fd:
            config.update(json.load(fd))

def extract_loci(variant_filenames):


def make_joint_caller_arguments(config, patient, out_vcf):
    patient_dict = config["patients"][patient]
    reads = collections.OrderedDict(patient_dict["reads"])
    only_somatic = all(
        x['kind'] == 'somatic' for x in patient_dict['variants'].values())
    force_call_loci = extract_loci(
        [x['path'] for x in patient_dict['variants'].values()])
    return (
        ["somatic-joint"] +
        [x['path'] for x in reads.values()] +
        ["--tissue-types"] + [x['path'] for x in reads.values()] +
        ["--analytes"] + [x['analyte'] for x in reads.values()] +
        ["--sample-names"] + list(reads.keys())
        ["--include-filtered"] + 
        (["--only-somatic"] if only_somatic else []) +
        ["--force-call-loci", force_call_loci] + 
        ["--reference-fasta", config["reference"]])

def run(argv=sys.argv[1:]):
    args = parser.parse_args(argv)

    config = load_config(args.configs)

    invocation = (
        ["java"] +
        config.get("java_args") + 
        [
            "D%s=%s" (key, value)
            for (key, value)
            in config.get("spark_configuration", {})
        ] + 
        ["-cp", args.guacamole_jar, org.hammerlab.guacamole.Main] +
        config.get("guacamole_arguments", [])
    
    subprocess.check_call(invocation)



    

