#!/bin/bash

# Stop on first error
set -e

PACKAGEVERSION=${PACKAGEVERSION:-$TRAVIS_BUILD_NUMBER}
export PACKAGEVERSION

if [ -z "$VERSION" ]; then
    echo "VERSION is missing"
    exit 1
fi

if [ -z "$PACKAGEVERSION" ]; then
    echo "PACKAGEVERSION is missing"
    exit 1
fi

# Prepare for rpm-build
pushd rpm-build
mkdir -p _build

# Create directories
mkdir -p opt/insight-gp-import
mkdir -p var/log/insight-gp-import

# Copy the package contents
cp -v ../*.py opt/insight-gp-import
cp -v ../*.sh opt/insight-gp-import
cp -v ../requirements.txt opt/insight-gp-import

echo "BUILDING VERSION:v$VERSION"

# # Freeze the dependencies of requirements
export SPEC_FILE=insight-gp-import.spec
# - ./freeze-requirement.sh palette-greenplum-installer x86_64 ${SPEC_FILE}
# # Show the contents of the modified (frozen versions) spec file
# - cat ${SPEC_FILE}

# build the rpm
rpmbuild -bb --buildroot "$(pwd)" --define "version $VERSION" --define "buildrelease $PACKAGEVERSION" --define "_rpmdir $(pwd)/_build" ${SPEC_FILE}
popd
