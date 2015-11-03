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

# Run 2to3 to fix the source files
2to3 -wn $TARGET/"concourse/thriftapi"

# Delete unnecessary files
rm $TARGET"/concourse/thriftapi/ttypes.py"
rm $TARGET"/concourse/thriftapi/ConcourseService-remote"

# Fix module importing in the $API file
API=$TARGET"/concourse/thriftapi/ConcourseService.py"
perl -i -0pe 's/from .ttypes import \*/from .data.ttypes import \*\nfrom .shared.ttypes import \*\nfrom .exceptions.ttypes import */g' $API
perl -p -i -e 's/concourse.thriftapi.(data|shared|exceptions).ttypes.//g' $API

CONSTANTS=$TARGET"/concourse/thriftapi/constants.py"
perl -p -i -e 's/.ttypes import \*/.data.ttypes import TObject/g' $CONSTANTS
perl -p -i -e 's/concourse.thriftapi.data.ttypes.//g' $CONSTANTS

# Use Python lists instead of sets so that insertion ordered is preserved
perl -p -i -e 's/= set\(\)/= []/g' $API
perl -p -i -e 's/\.add\(/.append(/g' $API

echo "Finished compiling the Thrift API for Python to "$(cd $PACKAGE && pwd)

exit 0
