#!/usr/bin/env bash
# Compile the thrift API for the Ruby service

. "`dirname "$0"`/.compile-thrift-include"

TARGET="../concourse-driver-ruby/lib/thrift_api"
PACKAGE=$TARGET

cd $THRIFT_DIR

# Run the thrift compile
thrift -out $TARGET -gen rb concourse.thrift

if [ $? -ne 0 ]; then
  exit 1
fi

echo "Finished compiling the Thrift API for Ruby to "$(cd $PACKAGE && pwd)

exit 0
