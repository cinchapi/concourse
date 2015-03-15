#!/usr/bin/env bash
# Compile the thrift API for the Python service

. "`dirname "$0"`/.compile-thrift-include"

TARGET="../concourse-driver-python"
PACKAGE=$TARGET

cd $THRIFT_DIR

# Run the thrift compile
thrift -out $TARGET -gen py concourse.thrift

if [ $? -ne 0 ]; then
  exit 1
fi

echo "Finished compiling the Thrift API for Python to "$(cd $PACKAGE && pwd)

exit 0
