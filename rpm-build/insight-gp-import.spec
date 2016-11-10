%define serviceuser insight
%define servicehome /etc/palette-insight-server


# Disable the stupid stuff rpm distros include in the build process by default:
#   Disable any prep shell actions. replace them with simply 'true'
%define __spec_prep_post true
%define __spec_prep_pre true
#   Disable any build shell actions. replace them with simply 'true'
%define __spec_build_post true
%define __spec_build_pre true
#   Disable any install shell actions. replace them with simply 'true'
%define __spec_install_post true
%define __spec_install_pre true
#   Disable any clean shell actions. replace them with simply 'true'
%define __spec_clean_post true
%define __spec_clean_pre true
# Disable checking for unpackaged files ?
#%undefine __check_files

# Use md5 file digest method.
# The first macro is the one used in RPM v4.9.1.1
%define _binary_filedigest_algorithm 1
# This is the macro I find on OSX when Homebrew provides rpmbuild (rpm v5.4.14)
%define _build_binary_file_digest_algo 1

# Use bzip2 payload compression
%define _binary_payload w9.bzdio


Name: palette-insight-gp-import
Version: %version
Release: %buildrelease
Summary: Palette Insight GP Import
AutoReqProv: no
# Seems specifying BuildRoot is required on older rpmbuild (like on CentOS 5)
# fpm passes '--define buildroot ...' on the commandline, so just reuse that.
#BuildRoot: %buildroot
# Add prefix, must not end with / except for root (/)

Prefix: /

Group: default
License: proprietary
Vendor: palette-software.net
URL: http://www.palette-software.com
Packager: Palette Developers <developers@palette-software.com>

# Add the user for the service & setup SELinux
# ============================================

Requires(pre): postgresql-devel >= 8.4, python35u-devel >= 3.5
Requires: palette-insight-toolkit
Requires: palette-greenplum-installer
Requires: palette-supervisor
Requires: palette-insight-server >= 400:2.0.0

%pre
# noop

%postun
# noop

%description
Palette Insight GP Import

%prep
# noop

%build
# noop

%install
# noop

%post
# Python3 and pip3 is installed by palette-insight-toolkit
pip3 install -r /opt/insight-gp-import/requirements.txt

supervisorctl restart insight-gpfdist

%clean
# noop

%files
%defattr(-,insight,insight,-)

# Reject config files already listed or parent directories, then prefix files
# with "/", then make sure paths with spaces are quoted.
# /usr/local/bin/palette-insight-server
/opt/insight-gp-import
/etc/palette-insight-server
/etc/supervisord.d
%dir /var/log/insight-gp-import
%dir /var/log/insight-gpfdist

# config files can be defined according to this
# http://www-uxsup.csx.cam.ac.uk/~jw35/docs/rpm_config.html
%config /etc/palette-insight-server/gp-import-config.yml

%changelog
