#!/bin/bash

# Fail if there are errors
set -e

LOCKFILE=/tmp/PI_GP_Import.flock

flock -n ${LOCKFILE} \
    python3 /opt/insight-gp-import/loadtables.py /etc/palette-insight-server/gp-import-config.yml /data/insight-server/uploads/PAL-DEV-CLONE-FOR-TEST
