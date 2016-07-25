'''
Utilities for calling the guacamole joint caller.
'''
from __future__ import absolute_import

import os

from .. import temp_files
from ..common import extract_loci_string

def make_arguments(config, patient, out_vcf):
    '''
    Parameters
    -----------
    config : Config
    
    patient : string

    out_vcf : string
        Path to result VCF that we want guacamole to write.

    Returns
    -----------
    string
    '''
    patient_dict = config["patients"][patient]
    reads = patient_dict["reads"]
    only_somatic = all(
        x['kind'] == 'somatic' for x in config['variants'].values())
    force_call_loci_string = extract_loci_string(patient, [
        x.get_substituted('path', path=True)
        for x in config['variants'].values()
    ])

    force_call_loci_path = temp_files.tempfile_path(
            prefix='loci_',
            suffix=".txt",
            contents=force_call_loci_string)

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

    if 'loci' in patient_dict:
        arguments.append("--loci")
        arguments.append(patient_dict.get_substituted('loci'))

    arguments.extend([
        "--force-call-loci-file",
        "file://" + os.path.abspath(force_call_loci_path)])
    arguments.extend(
        ["--reference-fasta", config.get_substituted("reference", path=True)])
    if config.get("reference_fasta_is_partial", "false") == "true":
        arguments.append("--reference-fasta-is-partial")
    arguments.extend(config.get("guacamole_arguments", []))
    arguments.extend([
        "--header-metadata",
        "VCB_BENCHMARK=%s" % config["benchmark"],
        "VCB_PATIENT=%s" % patient])

    arguments.extend(["--out", out_vcf])

    return arguments