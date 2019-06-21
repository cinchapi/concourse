#!/usr/bin/env bash

# Copyright (c) 2015 Cinchapi Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Compile the thrift API for the Node.js service

. "`dirname "$0"`/.compile-thrift-include"

TARGET="../concourse-driver-node-js"
PACKAGE=$TARGET"/src/thrift"

cd $THRIFT_DIR

# Run the thrift compile
thrift --gen js:es6,node -out $PACKAGE -recurse concourse.thrift

if [ $? -ne 0 ]; then
  exit 1
fi

# Patch the `NULL` type in the "concourse_types.js" file with a `data` property so that it validates properly.
perl -e 's#^(var\sthrift\s=\srequire\('"'"'thrift'"'"'\);)$#var buffer = require('"'"'buffer'"'"');\nvar Buffer = buffer.Buffer;\n$1#m' -i -p $PACKAGE"/concourse_types.js"
perl -e 's#^(\s{2}'"'"'type'"'"'\s:\s9)$#  '"'"'data'"'"' : Buffer.alloc(0),\n$1#m' -i -p $PACKAGE"/concourse_types.js"
$TARGET"/tools/thrift-transformers/remove-server-code.js" $PACKAGE"/ConcourseService.js"
# $TARGET"/tools/thrift-transformers/thrift-map-return-types-to-javascript-maps.js" $TARGET"/../interface/concourse.thrift" $PACKAGE"/ConcourseService.js"

echo "Finished compiling the Thrift API for Node.js to "$(cd $PACKAGE && pwd)

exit 0
