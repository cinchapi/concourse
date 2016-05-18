#/usr/bin/env bash

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

# A script to install version controlled Git hooks into the local git repo
. "`dirname "$0"`/.include"

REMOTE_HOOKS_DIR=`pwd`/../git-hooks
LOCAL_HOOKS_DIR=`pwd`/../.git/hooks

cd $LOCAL_HOOKS_DIR
HOOKS=$REMOTE_HOOKS_DIR/*

if [ "$(ls $REMOTE_HOOKS_DIR)" ]; then
    # Go through each file in the $REMOTE_HOOKS_DIR to ensure that it is
    # executable and create a symlink within the $LOCAL_HOOKS_DIR
    for file in $HOOKS
    do
        echo "Installing `basename $file`"
        chmod +x $file
        ln -sf $file .
    done
else
    echo "No git-hooks to install"
fi

exit $?
