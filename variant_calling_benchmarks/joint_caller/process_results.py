'''
Utilities for parsing the results of the guacamole joint caller.
'''

import collections
import os
import getpass
import socket
import hashlib
import time
import json
import logging

import pandas
import numpy
import six

import varcode
import varlens
import varlens.variants_util
from pyensembl.locus import normalize_chromosome

from ..common import load_benchmark_variants, git_info_for_guacamole_jar
from .. import cloud_util, analysis

def sha1_hash(s, num_digits=16):
    return hashlib.sha1(s).hexdigest()[:num_digits]

def write_results(args, config, patient_to_vcf, extra={}):
    guacamole_calls = load_results(patient_to_vcf)
    vcf_metadata = load_result_vcf_header_metadata(patient_to_vcf)

    merged_calls = merge_calls_with_others(config, guacamole_calls)
    merged_calls_hash = sha1_hash(
        merged_calls.to_csv(None, index=False))
    logging.info("Merged calls hash: %s" % merged_calls_hash)

    merged_calls_filename = "merged_calls.%s.%s.csv.gz" % (
            config['benchmark'], merged_calls_hash)
    merged_calls_csv = os.path.join(
            args.out_dir, merged_calls_filename)
    merged_calls.to_csv(merged_calls_csv, index=False, compression="gzip")
    logging.info("Wrote: %s" % merged_calls_csv)

    accuracy = analysis.accuracy_summary(merged_calls)

    manifest = collections.OrderedDict([
        ('user', getpass.getuser()),
        ('host', socket.gethostname()),
        ("cwd", os.getcwd()),
        ('time', time.asctime()),
        ('out_dir', os.path.abspath(args.out_dir)),
        ('merged_calls_hash', merged_calls_hash),
        ('merged_calls_filename', merged_calls_filename),
        ('guacamole_git_info', git_info_for_guacamole_jar(args.guacamole_jar)),
        ('accuracy_summary', accuracy),
        ('vcf_metadata', vcf_metadata),
        ('arguments', {
            'args': args._get_args(),
            'kwargs': args._get_kwargs(),
        }),
        ('config', config),
        ('extra', extra),
    ])
    manifest_json_dump = json.dumps(manifest, indent=2)
    manifest_hash = sha1_hash(manifest_json_dump)
    logging.info("Manifest hash: %s" % manifest_hash)
    manifest_json = os.path.join(
        args.out_dir,
        "manifest.%s.%s.%s.json" % (
            config['benchmark'], manifest_hash, merged_calls_hash))

    with open(manifest_json, "w") as fd:
        fd.write(manifest_json_dump)
    logging.info("Wrote: %s" % manifest_json)

    if args.out_bucket:
        cloud_util.copy_to_google_storage_bucket(
            merged_calls_csv,
            args.out_bucket,
            no_clobber=True)
        cloud_util.copy_to_google_storage_bucket(
            manifest_json,
            args.out_bucket,
            no_clobber=True)
    return manifest

JOIN_COLUMNS = [
    "patient",
    "genome",
    "contig",
    "interbase_start",
    "interbase_end",
    "ref",
    "alt",
]
def merge_calls_with_others(config, guacamole_calls_df):
    '''
    Join the given guacamole calls dataframe with the comparison variants
    in the benchmark.
    '''
    merged = guacamole_calls_df
    patients = set(config["patients"])
    assert set(guacamole_calls_df.patient) == patients,\
        "%s != %s" % (
            set(guacamole_calls_df.patient), patients)

    for (name, info) in config['variants'].items():
        variant_file = info['path']
        df = load_benchmark_variants(variant_file)
        if 'patient' not in df.columns:
            assert len(patients) == 1, \
                "VCF files only supported for single-patient benchmarks"
            df["patient"] = list(patients)[0]

        df = df.ix[df.patient.isin(patients)]

        # Since we load guacamole VCFs with varcode, the contigs will be
        # normalized, so we have to normalize them here.
        df["contig"] = df.contig.map(normalize_chromosome)
        df["called_%s" % name] = True
        join_columns = list(JOIN_COLUMNS)

        merged = pandas.merge(
            merged,
            df,
            how='outer',
            on=join_columns)

    bool_columns_default_false = [
        "called_guacamole",
        "trigger_GERMLINE_POOLED",
        "trigger_SOMATIC_INDIVIDUAL",
        "trigger_SOMATIC_POOLED",
        "triggered",
        "filtered",
    ] + ["called_%s" % name for name in config['variants']]

    for c in bool_columns_default_false:
        if c in merged:
            merged[c] = merged[c].fillna(False).astype(bool)
    return merged

def load_results(patient_to_vcf_paths):
    '''
    Given a dict of patient -> list of VCF paths written by guacamole for
    that patient, return a dataframe giving the calls for all patients.
    '''
    dfs = []
    for (patient, vcf_path) in patient_to_vcf_paths.items():
        logging.info("Loading VCF: %s" % vcf_path)
        calls = varlens.variants_util.load_as_dataframe(
            vcf_path, only_passing=False)
        logging.info("Done. Now parsing joint caller fields.")
        calls["patient"] = patient
        dfs.append(parse_joint_caller_fields(calls))
        logging.info("Done.")
    return pandas.concat(dfs, ignore_index=True)

def load_result_vcf_header_metadata(patient_to_vcf):
    result = {}
    for (patient, vcf_path) in patient_to_vcf.items():
        reader = varcode.vcf.PyVCFReaderFromPathOrURL(vcf_path)
        result[patient] = reader.vcf_reader.metadata
        reader.close()
    return result
 
def yes_no_to_bool(value):
    if value == "YES":
        return True
    if value == "NO": 
        return False
    raise ValueError("Not YES/NO: %s" % value)

def parse_mixture_likelihoods(strings):
    result = collections.OrderedDict()
    for component in strings:
        (mixture, value) = component.split("=")
        value = float(value)
        assert mixture.startswith("[")
        assert mixture.endswith("]")
        mixture = mixture[1:-1]
        if "/" in mixture:
            # germline mixture
            parsed_mixture = tuple(mixture.split("/"))
            assert len(parsed_mixture) == 2
        else:
            parsed_mixture = []
            total_vaf = 0
            for piece in mixture.split("|"):
                (allele, vaf) = piece.split("->")
                vaf = float(vaf)
                total_vaf += vaf
                parsed_mixture.append((allele, vaf))
            parsed_mixture = tuple(parsed_mixture)
            if not pandas.isnull(total_vaf):
                numpy.testing.assert_almost_equal(total_vaf, 1.0, decimal=1)
        result[parsed_mixture] = value
    return result

def expand_sample_info_columns_one_row(full_row, result):
    sample_info = full_row['sample_info']
    for (sample, info) in six.iteritems(sample_info):
        info = dict(info)
        if 'FF' not in info:
            info['FF'] = []
        (info['AD_ref'], info['AD_alt']) = info['AD']
        del info['AD']
        parsed_rl = parse_mixture_likelihoods(info['RL'])
        info['RL'] = parsed_rl
        rl_values = list(parsed_rl.values())
        info['RL_ref'] = rl_values[0]
        info['RL_alt'] = rl_values[1] if len(rl_values) > 1 else None
        info['TRIGGERED'] = (
            info['TRIGGERED'] == 'YES' or info['TRIGGERED'] == 'EXPRESSED')
        for (field, value) in six.iteritems(info):
            result["%s_%s" % (sample, field)].append(value)

def parse_joint_caller_fields(df):
    df = df.copy()
    new_columns = collections.defaultdict(list)
    expand_sample_info_columns_one_row(df.iloc[0], new_columns)
    new_columns = dict((key, []) for key in new_columns)
    column_dtypes = {}
    
    all_filters = set()
    all_triggers = set()
    for (_, row) in df.iterrows():
        if row['filter']:
            all_filters.update(row['filter'])
        if isinstance(
                row["info:TRIGGER"], list) and row["info:TRIGGER"] != ["NONE"]:
            all_triggers.update(row["info:TRIGGER"])
            
    all_filters = sorted(all_filters)
    all_triggers = sorted(all_triggers)
    for fltr in all_filters:
        new_columns["filter_%s" % fltr] = []
        column_dtypes["filter_%s" % fltr] = numpy.bool
    for trigger in all_triggers:
        new_columns["trigger_%s" % trigger] = []
        column_dtypes["trigger_%s" % trigger] = numpy.bool

    for (i, row) in df.iterrows():
        sample_info = row.sample_info
        expand_sample_info_columns_one_row(row, new_columns)

        filters_present = set(row["filter"]) if row["filter"] else set()
        for fltr in all_filters:
            new_columns["filter_%s" % fltr].append(fltr in filters_present)

        triggers_present = set(
            row["info:TRIGGER"]
            if isinstance(row["info:TRIGGER"], list) and
            row["info:TRIGGER"] != ["NONE"]
            else set())
        for trigger in all_triggers:
            new_columns["trigger_%s" % trigger].append(
                trigger in triggers_present)

        for (column, lst) in six.iteritems(new_columns):
            if len(lst) != i + 1:
                raise ValueError(
                    "Info\n%s\nfor row %d\n%s\ndid not add to column %s" % (
                     str(sample_info), i, str(row), column))

    for column in sorted(new_columns):
        values = new_columns[column]
        df[column] = values
        if column in column_dtypes:
            df[column] = df[column].astype(column_dtypes[column])

    df["tumor_expression"] = [
        yes_no_to_bool(value) for value in df["info:TUMOR_EXPRESSION"]
    ]
    del df['info:TUMOR_EXPRESSION']
    df["triggered"] = [
        isinstance(v, list) and v != ["NONE"] for v in df['info:TRIGGER']
    ]
    df["filtered"] = [bool(v) for v in df["filter"]]
    df["called_guacamole"] = df["triggered"] & (~ df["filtered"])
    return df



