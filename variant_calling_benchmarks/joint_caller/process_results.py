'''
Utilities for parsing the results of the guacamole joint caller.
'''

import collections
import os

import pandas
import numpy

import varlens
import varlens.variants_util
from pyensembl.locus import normalize_chromosome

def write_merged_calls(args, config, patient_to_vcf):
    guacamole_calls = load_results(patient_to_vcf)
    guacamole_calls_csv = os.path.join(
            args.out_dir,
            "guacamole_calls.%s.csv" % (config['benchmark']))
    guacamole_calls.to_csv(guacamole_calls_csv, index=False)
    print("Wrote: %s" % guacamole_calls_csv)

    merged_calls = merge_calls_with_others(
        config, guacamole_calls)
    merged_calls_csv = os.path.join(
            args.out_dir,
            "merged_calls.%s.csv" % (config['benchmark']))
    merged_calls.to_csv(merged_calls_csv, index=False)
    print("Wrote: %s" % merged_calls_csv)

    summary = summary_stats(
        config, merged_calls)
    summary_csv = os.path.join(
            args.out_dir,
            "summary.%s.csv" % (config['benchmark']))
    summary.to_csv(summary_csv, index=False)
    print("Wrote: %s" % summary_csv)

join_columns = [
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

    for (name, info) in config['variants'].items():
        df = pandas.read_csv(info.get_substituted('path', path=True))

        # Since we load guacamole VCFs with varcode, the contigs will be
        # normalized, so we have to normalize them here.
        df["contig"] = df.contig.map(normalize_chromosome)
        df["called_%s" % name] = True
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
        calls = varlens.variants_util.load_as_dataframe(
            vcf_path, only_passing=False)
        calls["patient"] = patient
        dfs.append(parse_joint_caller_fields(calls))
    return pandas.concat(dfs, ignore_index=True)

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
    for (sample, info) in sample_info.iteritems():
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
        for (field, value) in info.iteritems():
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

        for (column, lst) in new_columns.iteritems():
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


def summary_stats(config, merged):
    def stat(bool_series):
        return (
            bool_series.sum(), len(bool_series), bool_series.mean() * 100.0)

    rows = []
    rows.append(("calls", "", merged["called_guacamole"].sum()))
    rows.append((
        "calls before filtering",
        "",
        merged.triggered.sum()))

    for name in config["variants"]:
        called_col = "called_%s" % name
        rows.append(("calls", name, merged[called_col].sum()))

        # with filters
        rows.append(("sensitivity with filters", name) + 
            stat(merged.ix[merged[called_col]].called_guacamole))
        rows.append(("specificity with filters", name) + 
            stat(merged.ix[merged.called_guacamole][called_col]))

        # without filters
        rows.append(
            ("sensitivity from pooled calling only without filters", name) +
            stat(merged.ix[merged[called_col]].trigger_SOMATIC_POOLED))
        rows.append(
            ("sensitivity individual calling only without filters", name) +
            stat(merged.ix[merged[called_col]].trigger_SOMATIC_INDIVIDUAL))
        rows.append(
            ("specificity without filters", name) +
            stat(merged.ix[merged.triggered][called_col]))
        rows.append(
            ("specificity both pooled and individual triggers firing", name) +
            stat(
                merged.ix[
                    merged.trigger_SOMATIC_INDIVIDUAL &
                    merged.trigger_SOMATIC_POOLED
                ][called_col]))

    columns = [
        "stat", "comparison_dataset",
        "numerator", "denominator", "percent",
    ]
    return pandas.DataFrame(
        [list(row) + [None] * (len(columns) - len(row)) for row in rows],
        columns=columns)
