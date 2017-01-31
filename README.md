# Palette Insight LoadTables
[Insight Server]: https://github.com/palette-software/insight-server
[Palette database schema]: rpm-build/opt/insight-gp-import/init_palette_schema.sql
[![Build Status](https://travis-ci.com/palette-software/insight-gp-import.svg?token=qWG5FJDvsjLrsJpXgxSJ&branch=master)](https://travis-ci.com/palette-software/insight-gp-import)

## What is Palette Insight LoadTables?

Imports data files coming from [Insight Server] to the Greenplum Database for further processing.

In this project you will find:

- the [supervisord](http://supervisord.org/) configuration
 [file](rpm-build/etc/supervisord.d/insight-gpfdist.ini) for the
 [gpfdist](http://gpdb.docs.pivotal.io/4340/utility_guide/admin_utilities/gpfdist.html)
 service of Greenplum Database which serves the CSV files of the [Insight Server].
- the initialization script of the [Palette database schema]
- the LoadTables service which imports the [tables](rpm-build/etc/palette-insight-server/gp-import-config.yml)
 to the Greenplum Database

### CSV import

The following process is performed by the Palette Insight LoadTables service:

1. the CSV files are uploaded to the `uploads` folder of the working directory (eg. `/data/insight-server/uploads/palette/uploads`)
2. process files in `retry` folder.
    1. if it succeed copy file to `archive`
    2. if not copy file to `retried`
3. copy files from `uploads` to `processing`
4. run SQL statements which try to import data to staging tables
    1. if it succeed copy file to `archive`
    2. if not copy file to `retry`

## How do I set up Palette Insight LoadTables?

### Prerequisites

- Palette Insight LoadTables is compatible with Python 3.5

### Packaging

To build the package you may use the [scripts/rpm.sh](scripts/rpm.sh):

```bash
export PACKAGEVERSION=123
export VERSION="$(sed -n 's/Version. \([0-9]*\.[0-9]*\.[0-9]*$\)/\1/p' < rpm-build/etc/palette-insight-server/gp-import-config.yml)".$PACKAGEVERSION
scripts/rpm.sh
```

### Installation

The most convenient is to build the RPM package and install it using either yum or rpm.
It does require and install the other necessary components and services.

The following process is executed by the installer:

- installation of the required python packages (`pip3 install -r requirements.txt`)
- preparation of the `processing` folder in the working directory
- startup of the gpfdist service
- initialization of the [Palette database schema]
- import of the initial Tableau metadata (creates neccessary tables)
- initialization of the `ext_error_table` table

## How can I test-drive Palette Insight LoadTables?

In order to allow only one instance to run it is advised to use the [run_gp_import.sh](run_gp_import.sh) script.

Check the `threadinfo`, `h_users`, `http_requests` and `serverlogs` tables for data.

The unittests can be run with [pytest](http://doc.pytest.org):

```bash
python -m pytest test/
```

## Is Palette Insight LoadTables supported?

Palette Insight LoadTables is licensed under the GNU GPL v3 license. For professional support please contact developers@palette-software.com

Any bugs discovered should be filed in the [Palette Insight LoadTables Git issue tracker](https://github.com/palette-software/insight-gp-import/issues) or contribution is more than welcome.
