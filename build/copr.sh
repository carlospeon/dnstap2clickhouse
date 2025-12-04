#!/bin/bash

set -eo pipefail

function usage() {
  echo "Usage: $0 spec outdir"
  exit 1
}

spec="${1}"
[ -z "${spec}" ] && usage || shift
outdir="${1}"
[ -z "${outdir}" ] && usage || shift

target="./.rpmbuild"
builddir="build"

which git || dnf -y install git

name=$(grep ^Name ${spec} | cut -f 2 -d : | xargs echo)
version=$(git describe --tags | sed 's/v//;s/-/./;s/-/_/')
release_macro=$(grep ^Release ${spec} | cut -f 2 -d : | xargs echo)
release=$(rpm --eval "${release_macro}")

srpm="${name}-${version}-${release}.src.rpm"

[ -d ${target}/SOURCES ] || mkdir -p .rpmbuild/SOURCES
git archive --output=${target}/SOURCES/${name}-${version}.tar.gz --prefix=${name}-${version}/ HEAD

sed -i "/^Version/s/%{?version}/${version}/" ${spec}
rpmbuild --define "_topdir ${target}" -bs ${spec}

cp ${target}/SRPMS/${srpm} ${outdir}

ls -l ${outdir}
