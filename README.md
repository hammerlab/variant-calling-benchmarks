# variant-calling-benchmarks
Automated and curated variant calling benchmarks for Guacamole

## Installation

From a checkout run:
```
pip install -e .
```

Also checkout and build guacamole.

## Config files

The benchmark defintions and guacamole configurations are given in JSON. They are split into multiple files for clarity but the 'configuration' is just the union of all the files passed, i.e. the code
doesn't care what the files are or which file has a particular property.

Paths in config files are relative to the directory of the config file. A simple substitution mechanism is supported where strings like "foo-${NAME}" will have $NAME expanded according to substitutions defined in the config files (under the 'substitutions' keys).


## Running Locally

```
vcb-guacamole-local \
    base_config.json \
    infrastructure-configs/local.json \
    guacamole-configs/default.json \
    benchmarks/local-wgs1/benchmark.json \
    --guacamole-jar ~/sinai/git/guacamole/target/guacamole-with-dependencies-0.0.1-SNAPSHOT.jar \
    --out-dir results


```

## Running on demeter

TODO, this does not work yet

```
vcb-guacamole-demeter \
    base_config.json \
    infrastructure-configs/local.json \
    guacamole-configs/default.json \
    benchmarks/aocs/benchmark.json \
    --guacamole-jar ~/sinai/git/guacamole/target/guacamole-with-dependencies-0.0.1-SNAPSHOT.jar \
    --out-dir results


```