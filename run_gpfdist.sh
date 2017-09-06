#!/bin/bash

# Make older version of libyaml-0.so.1, libcrypto.so.0.9.8 and libssl.so.0.9.8
# available for gpfdist on RHEL 7.3
source /usr/local/greenplum-db/greenplum_path.sh
/usr/local/greenplum-db/bin/gpfdist -p 18010 -v -m 268435456
