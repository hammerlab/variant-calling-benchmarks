# variant-calling-benchmarks
Automated and curated variant calling benchmarks for Guacamole

## Installation

From a checkout run:
```
pip install -e .
```

Also checkout and build guacamole.

## Running Locally

```
vcb-guacamole-local \
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
    infrastructure-configs/local.json \
    guacamole-configs/default.json \
    benchmarks/aocs/benchmark.json \
    --guacamole-jar ~/sinai/git/guacamole/target/guacamole-with-dependencies-0.0.1-SNAPSHOT.jar \
    --out-dir results


```