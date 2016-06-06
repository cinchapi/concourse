#/usr/bin/env bash

# Copyright (c) 2016 Cinchapi Inc.
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

# An idempotent script to symlink Spotless configuration files for Gradle
. "`dirname "$0"`/.include"

SOURCE_DIR=`pwd`/../

cd $SOURCE_DIR
DIRS=($(ls -d */))
FILES=(spotless.license.java spotless.importorder spotless.eclipseformat.xml)
for DIR in "${DIRS[@]}"
do
    if [ -d $DIR/src/main/java ] || [ -d $DIR/src/test/java ]; then
        for FILE in "${FILES[@]}"
        do
            if [ ! -f $DIR/$FILE ];
            then
                ln -s $SOURCE_DIR/$FILE $DIR/
            fi
        done
    fi
done
