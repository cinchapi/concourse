#!/usr/bin/env bash
# Compile the thrift API for the PHP service

. "`dirname "$0"`/.compile-thrift-include"

TARGET="../concourse-driver-php"
PACKAGE=$TARGET"/cinchapi/concourse/src"

cd $THRIFT_DIR

# Run the thrift compile
thrift -out $PACKAGE -gen php concourse.thrift

if [ $? -ne 0 ]; then
  exit 1
fi

API=$PACKAGE"/thrift/ConcourseService.php"

# Replace all TransactionToken declarations to take no type since we allow that
# parameter to legally be null in method invocations.
perl -p -i -e "s/, \\\\thrift\\\\shared\\\\TransactionToken/, /g" $API

# Ensure that we consistently populate array values instead of array keys
# regardless of whether the value is scalar or not
perl -p -i -e 's/\[(\$[A-Za-z]{1,}[0-9]{0,})\]\s*=\strue;/ []= $1;/g' $API

# Serialize any array keys that are not strings or integers since PHP doesn't
# allow them.
perl -p -i -e 's/\[(\$[A-Za-z]{1,}[0-9]{0,})\]/[(!is_string($1) && !is_integer($1)) ? serialize($1) : $1]/g' $API

echo "Finished compiling the Thrift API for PHP to "$(cd $PACKAGE && pwd)

exit 0
