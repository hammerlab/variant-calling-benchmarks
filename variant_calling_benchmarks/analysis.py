import subprocess
import json
import collections
import logging
import os

from six import StringIO

import pandas
import requests

from . import common

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

    merged_calls["interbase_start"] = (
        merged_calls["interbase_start"].astype(int))
    merged_calls["interbase_end"] = merged_calls["interbase_end"].astype(int)
    merged_calls["ref"] = merged_calls["ref"].fillna("")
    merged_calls["alt"] = merged_calls["alt"].fillna("")

    merged_calls = common.df_decode_json_columns(merged_calls)

    merged_calls._metadata.append("manifest")
    merged_calls.manifest = manifest
    return merged_calls

def load_url(filename_or_url):
    """
    Given a path or URL, return the data. Google cloud project "gs://" URLs are
    supported.
    """
    if (filename_or_url.startswith("http://") or
            filename_or_url.startswith("https://")):
        return requests.get(filename_or_url).text
    if filename_or_url.startswith("gs://"):
        return subprocess.check_output(["gsutil", "cat", filename_or_url])
    with open(filename_or_url) as fd:
        return fd.read()

def load_json(filename_or_url):
    """
    Given a path or URL to a JSON file, return the decoded data.
    """
    content = load_url(filename_or_url)
    return json.loads(content, object_pairs_hook=collections.OrderedDict)

def accuracy_summary(merged):
    """
    Given a dataframe of merged guacamole / non-guacamole calls, return a dict
    summarizing gucacamole recall and precision.
    """
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