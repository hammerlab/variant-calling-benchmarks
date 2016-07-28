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
    parser.add_argument("--keep-temp-files", action="store_true",
        default=False,
        help="Don't delete temporary files.")
    parser.add_argument("--skip-guacamole", action="store_true", default=False,
        help="Don't actually run guacamole")

def load_benchmark_variants(variant_file):
    if variant_file.endswith('vcf'):
        df = varlens.variants_util.load_as_dataframe(variant_file, only_passing=False)
    else:
        df = pandas.read_csv(variant_file)
    return df
