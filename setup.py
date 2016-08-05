try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

version = "0.0.1"

setup(
    name="variant_calling_benchmarks",
    version=version,
    author="Tim O'Donnell",
    author_email="timodonnell@gmail.com",
    packages=["variant_calling_benchmarks"],
    url="https://github.com/hammerlab/variant_calling_benchmarks",
    license="Apache License",
    description="Benchmarks for Guacamole variant calling",
    long_description="",
    download_url='https://github.com/hammerlab/variant_calling_benchmarks/tarball/%s' % version,
    entry_points={
        'console_scripts': [
            'vcb-guacamole-local = variant_calling_benchmarks.guacamole_local:run',
            'vcb-guacamole-cluster = variant_calling_benchmarks.guacamole_cluster:run',

        ]
    },
    classifiers=[
        "Development Status :: 1 - Planning",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.4",
    ],
    install_requires=[
        "varcode",
        "varlens",
        "pyensembl",
        "requests",
        "nose>=1.3.1",
        "typechecks>=0.0.2",
        "pandas>=0.18.1",
    ]
)
