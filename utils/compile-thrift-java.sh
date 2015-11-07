#!/usr/bin/env bash
# Compile the thrift API for the Java service

. "`dirname "$0"`/.compile-thrift-include"

TARGET="../concourse-driver-java/src/main/java"
PACKAGE=$TARGET"/com/cinchapi/concourse/thrift"

cd $THRIFT_DIR

# Run the thrift compile
thrift -out $TARGET -gen java concourse.thrift

if [ $? -ne 0 ]; then
  exit 1
fi

# Replace all instances of Hash* data structures with their LinkedHash
# counterparts (see THRIFT-2115)
perl -p -i -e "s/Hash/LinkedHash/g" $PACKAGE"/ConcourseService.java"

# Remove unnecessary files
rm $PACKAGE"/concourseConstants.java"

# Supress all the necessary warnings
perl -p -i -e 's/"cast", "rawtypes", "serial", "unchecked"/"cast", "rawtypes", "serial", "unchecked", "unused"/g' $PACKAGE"/ConcourseService.java"

echo "Finished compiling the Thrift API for Java to "$(cd $PACKAGE && pwd)

exit 0
