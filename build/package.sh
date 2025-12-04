#!/bin/bash

set -eo pipefail

extra_args=""
while (($#)) ; do
  case "$1" in
    "-d"|"--debug")
      set -x
      extra_args="$extra_args --noclean"
      ;;
    "-h"|"--help")
      echo "Usage: $0 [-d|--debug]"
      ;;
  esac
  shift
done

mock_configs="rocky+epel-9-x86_64"

target="./.rpmbuild"
builddir="build"
mockdir="/var/lib/mock"

spec="${builddir}/dnstap2clickhouse.spec"
name=$(grep ^Name ${spec} | cut -f 2 -d : | xargs echo)
version=$(git describe --tags | sed 's/v//;s/-/./;s/-/_/')
release_macro=$(grep ^Release ${spec} | cut -f 2 -d : | xargs echo)
release=$(rpm --eval "${release_macro}")

srpm="${name}-${version}-${release}.src.rpm"

[ -d ${target} ] || mkdir ${target}
git archive --output=${target}/${name}-${version}.tar.gz --prefix=${name}-${version}/ HEAD

mock --define "version ${version}" \
  --buildsrpm --spec ${spec} \
  --sources ${target}/${name}-${version}.tar.gz \
  --resultdir ${target} \
  ${extra_args}
mock --enable-network \
  --define "version ${version}" \
  --define "dist %{nil}" \
  --resultdir ${target} \
  ${extra_args} \
  ${target}/${srpm}
for mock in ${mock_configs}; do
  mock --enable-network -r ${mock} \
    --define "version ${version}" \
    --resultdir ${target} \
    ${extra_args} \
    ${target}/${srpm}
done

