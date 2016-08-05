import subprocess
import json
import collections
import logging
import os

from six import string_types, StringIO

import numpy
import pandas
import requests

def load_benchmark_result(filename_or_url):
    """
    Given a path or URL to a benchmark result manifest JSON file, return
    a pandas.DataFrame giving the merged (guacamole and reference) variant
    calls. The manifest information is available in the 'manifest' attribute
    of the resulting dataframe.
    """
    manifest = load_json(filename_or_url)
    merged_calls_path = (
        os.path.dirname(filename_or_url) +
        "/" +
        manifest["merged_calls_filename"])
    logging.info("Loading merged calls from: %s" % merged_calls_path)
    if merged_calls_path.startswith("gs://"):
        data = load_url(merged_calls_path)
        merged_calls = pandas.read_csv(StringIO(data), compression="gzip")
    else:
        merged_calls = pandas.read_csv(merged_calls_path)
    logging.info("Done loading.")
    del merged_calls["sample_info"]
    for column in merged_calls.columns:
        parseable = (
            value_looks_parseable(x)
            for x in merged_calls[column].dropna().head(100))
        if all(parseable):
            logging.debug("Parsing column: %s" % column)
            merged_calls[column] = parse_values(merged_calls[column])
    merged_calls["snv"] = (
        (merged_calls.ref.str.len() == 1) &
        (merged_calls.alt.str.len() == 1))
    merged_calls["alt"] = merged_calls["alt"].fillna("")
    merged_calls["ref"] = merged_calls["ref"].fillna("")
    merged_calls._metadata.append("manifest")
    merged_calls.manifest = manifest
    return merged_calls

eval_environment = {
    "OrderedDict": collections.OrderedDict,
    "nan": numpy.nan
}
def parse_values(series):
    strings = series.fillna("None")
    return strings.map(
        dict((key, eval(key, eval_environment)) for key in strings.unique()))

def value_looks_parseable(value):
    return isinstance(value, string_types) and (
        value.startswith("['") or
        value.startswith("OrderedDict") or
        value.startswith("{"))

def load_url(filename_or_url):
    if (filename_or_url.startswith("http://") or
            filename_or_url.startswith("https://")):
        return requests.get(filename_or_url).text
    if filename_or_url.startswith("gs://"):
        return subprocess.check_output(["gsutil", "cat", filename_or_url])
    with open(filename_or_url) as fd:
        return fd.read()

def load_json(filename_or_url):
    content = load_url(filename_or_url)
    return json.loads(content, object_pairs_hook=collections.OrderedDict)

def accuracy_summary(merged):
    def stat(bool_series):
        return collections.OrderedDict([
            ('numerator', bool_series.sum()),
            ('denominator', len(bool_series)),
            ('percent', bool_series.mean() * 100.0),
        ])

    rows = []
    rows.append(("calls", "", merged["called_guacamole"].sum()))
    rows.append((
        "calls before filtering",
        "",
        merged.triggered.sum()))

    called_columns = [
        c for c in merged.columns
        if c.startswith("called_") and c != "called_guacamole"
    ]

    result = collections.OrderedDict([
        ("variants", collections.OrderedDict()),
    ])

    for called_col in called_columns:
        name = called_col.replace("called_", "")
        sub_result = result["variants"][name] = collections.OrderedDict()
        rows.append(("calls", name, merged[called_col].sum()))

        # with filters
        sub_result["recall with filters"] = stat(
            merged.ix[merged[called_col]].called_guacamole)

        sub_result["precision with filters"] = stat(
            merged.ix[merged[called_col]].called_guacamole)

        # without filters
        sub_result["recall from pooled calling only without filters"] = stat(
            merged.ix[merged[called_col]].trigger_SOMATIC_POOLED)
        sub_result["recall individual calling only without filters"] = stat(
            merged.ix[merged[called_col]].trigger_SOMATIC_INDIVIDUAL)
        sub_result["precision without filters"] = stat(
            merged.ix[merged.triggered][called_col])
        sub_result["precision both pooled and individual triggers firing"] = \
            stat(
                merged.ix[
                    merged.trigger_SOMATIC_INDIVIDUAL &
                    merged.trigger_SOMATIC_POOLED
                ][called_col])

    return result