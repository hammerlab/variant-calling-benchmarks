import collections

import pandas
import numpy

import varlens
import varlens.variants_util

from . import temp_files

#################################################
# Invoking joint caller

def extract_loci_string(patient, variant_filenames):
    loci = []
    for filename in variant_filenames:
        df = pandas.read_csv(filename)
        df = df.ix[df.patient == patient]
        for (i, row) in df.iterrows():
            loci.append("%s:%d-%d" % (
                row["contig"], row["interbase_start"], row["interbase_end"]))
    return ",\n".join(loci)

def make_arguments(config, patient, out_vcf):
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

    arguments.extend(["--force-call-loci-from-file", force_call_loci_path])
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

#################################################
# Parsing joint caller results

join_columns = [
    "patient",
    "genome",
    "contig",
    "interbase_start",
    "interbase_end",
    "ref",
    "alt"
]
def merge_calls_with_others(config, guacamole_calls_df):
    merged = guacamole_calls_df

    for (name, info) in config['variants'].items():
        df = pandas.read_csv(info.get_substituted('path', path=True))
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

'''

print("Dataset")
print("\tPublished calls: %d" % merged.called_pub.sum())
print("\tGuacamole calls: %d" % merged.called_guac.sum())
print("\tGuacamole calls before filtering: %d" % merged.triggered.sum())
print("")
print("Peformance with filters:")
print("\tSensitivity: %s" % stat(merged.ix[merged.called_pub].called_guac))
print("\tSpecificity: %s" % stat(merged.ix[merged.called_guac].called_pub))
print("")
print("Performance w/o filters:")
print("\tSensitivity from pooled calling only: %s"
      % stat(merged.ix[merged.called_pub].trigger_SOMATIC_POOLED))
print("\tSensitivity individual calling only: %s"
      % stat(merged.ix[merged.called_pub].trigger_SOMATIC_INDIVIDUAL))
print("\tSpecificity: %s" % stat(merged.ix[merged.triggered].called_pub))
print("\tSpecificity both pooled and individual triggers firing: %s"
      % stat(merged.ix[merged.trigger_SOMATIC_INDIVIDUAL & merged.trigger_SOMATIC_POOLED].called_pub))


'''

