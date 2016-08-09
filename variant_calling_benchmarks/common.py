import subprocess
import logging
import os
import collections
import json
from six import string_types

import numpy
import pandas
import varlens

def extract_loci_string(patient, variant_filenames):
    '''
    Given a patient and a list of variant CSV files, return a string
    like "chr1:2-10,chr2-50-51" that gives the loci of all variants
    containined in the variant files.

    Parameters
    -----------
    patient : string

    variant_filenames : list of string

    Returns
    -----------
    string

    '''
    loci = []
    for filename in variant_filenames:
        df = load_benchmark_variants(filename)
        if 'patient' in df.columns:
            df = df.ix[df.patient == patient]
        for (i, row) in df.iterrows():
            loci.append("%s:%d-%d" % (
                row["contig"], row["interbase_start"], row["interbase_end"]))
    return ",\n".join(loci)

def add_common_run_args(parser):
    parser.add_argument("configs", nargs="+", help="JSON config files")
    parser.add_argument("--guacamole-jar", required=True,
        help="Path to guacamole-VERSION.jar")
    parser.add_argument("--guacamole-dependencies-jar", required=True,
        help="Path to guacamole-deps-only-VERSION.jar")
    parser.add_argument("--patient", nargs="+",
        help="One or more patients to run. Default: all patients are run.")
    
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--out-bucket")

    parser.add_argument("--keep-temp-files", action="store_true",
        default=False,
        help="Don't delete temporary files.")
    parser.add_argument("--skip-guacamole", action="store_true", default=False,
        help="Don't actually run guacamole")

def load_benchmark_variants(variant_file):
    if variant_file.endswith('.vcf') or variant_file.endswith('.vcf.gz'):
        df = varlens.variants_util.load_as_dataframe(
            variant_file, only_passing=False)
    else:
        df = pandas.read_csv(variant_file)
    return df

def compress_file(filename, method='gzip', dry_run=False):
    """
    Compress the given file using gzip or bzip2 and return the path to the
    compressed file. If dry_run=True, do nothing.
    """
    method_to_extension = {
        'bzip2': 'bz2',
        'gzip': 'gz',
    }
    if method not in method_to_extension:
        raise ValueError("Unknown method: %s" % method)
    if not dry_run:
        logging.info("%s compressing: %s" % (method, filename))
        subprocess.check_call([method, "-n", "-f", filename])
    return "%s.%s" % (filename, method_to_extension[method])

def git_info_for_guacamole_jar(jar_path):
    """
    Given a path to a guacamole jar, return a dict of git info for the checkout
    the jar comes from.
    """
    result = collections.OrderedDict()
    target_dir = os.path.dirname(jar_path)
    error = None
    if target_dir.endswith('target'):
        guacamole_dir = os.path.dirname(target_dir)
        logging.info("Inferred guacamole repository dir: %s" % guacamole_dir)
        try:
            result["repository_path"] = guacamole_dir
            result["status"] = subprocess.check_output(["git", "status"],
                cwd=guacamole_dir).strip()
            result["commit"] = subprocess.check_output(
                ["git", "log", "-1", "--format='%H'"],
                cwd=guacamole_dir).strip()
            result["diff"] = subprocess.check_output(
                ["git", "diff"], cwd=guacamole_dir).strip()
        except Exception as e:
            error = str(e)
    else:
        error = "not a repository?"

    if error is not None:
        logging.warn("Failed to check git status for repository: %s: %s" %
            (guacamole_dir, error))

    return result

def df_encode_json_columns(df):
    """
    Given a dataframe where some columns contain objects, JSON encode those
    objects and append "_json" to their column names.
    """
    df = df.copy()
    original_dtypes = list(df.dtypes.iteritems())
    new_columns_order = []
    for (column, dtype) in original_dtypes:
        if (dtype == numpy.object_ and
                not all(
                    isinstance(x, string_types + (bool, int, float))
                    for x in df[column].dropna())):
            logging.info("JSON encoding: %s" % column)
            new_name = "%s_json" % column
            df[new_name] = [
                json.dumps(x) for x in df[column]
            ]
            new_columns_order.append(new_name)
        else:
            new_columns_order.append(column)

    return df[new_columns_order]

def df_decode_json_columns(df):
    """
    Inverse of df_encode_json_columns.
    """
    def load_json(s):
        try:
            return json.loads(x, object_pairs_hook=collections.OrderedDict)
        except:
            logging.warn("Couldn't parse: %s" % x)
            raise

    df = df.copy()
    column_order = []
    for column in df.columns:
        if column.endswith("_json"):
            logging.info("Decoding column: %s" % column)
            new_name = column.replace("_json", "")
            df[new_name] = [
                load_json(x)
                for x in df[column].fillna("null")
            ]
            column_order.append(new_name)
        else:
            column_order.append(column)
    return df[column_order].copy()
