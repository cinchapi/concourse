#!/usr/bin/env bash
# Compile the thrift API for the PHP service

. "`dirname "$0"`/.compile-thrift-include"

TARGET="../concourse-driver-php"
PACKAGE=$TARGET"/cinchapi/concourse/src"

cd $THRIFT_DIR

# Run the thrift compile
thrift -out $TARGET -gen php concourse.thrift

if [ $? -ne 0 ]; then
  exit 1
fi

# Replace all TransactionToken declarations to take no type
perl -p -i -e "s/, \\\\thrift\\\\shared\\\\TransactionToken/, /g" $PACKAGE"/thrift/ConcourseService.php"

echo "Finished compiling the Thrift API for PHP to "$(cd $PACKAGE && pwd)

exit 0
