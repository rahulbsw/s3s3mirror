#!/bin/bash

THISDIR=$(cd "$(dirname $0)" && pwd)

VERSION=1.3.0
JARFILE=$(ls s3tos3util*.jar)
VERSION_ARG="-Ds3tos3util.version=${VERSION}"

DEBUG=$1
if [ "${DEBUG}" = "--debug" ] ; then
  # Run in debug mode
  shift   # remove --debug from options
  java -Dlog4j.configuration=file:log4j.xml -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=5005 ${VERSION_ARG} -jar "${JARFILE}" --move "$@"

else
  # Run in regular mode
  java ${VERSION_ARG} -Dlog4j.configuration=file:log4j.xml -jar "${JARFILE}" --action delete "$@" dummy
fi

exit $?

