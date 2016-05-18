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

# This script will update the copyright notices on every file to include
# the current year (e.g. 2013-2014 will become 2013-2015, etc). This only
# needs to be run once a year. The script is smart enough to get the current
# year without being told, so it is a noop if it is run multiple times in
# the same year

# Change to the directory of the repo
DIR=`dirname $0`
cd $DIR

YEAR=`date +"%Y"`
LAST_YEAR=$(($YEAR - 1))
REPLACE="2013-$YEAR"

SEARCH="2013-$LAST_YEAR"
grep -r -l "$SEARCH" --exclude=\*sh . | uniq | xargs perl -e "s/$SEARCH/$REPLACE/" -pi

SEARCH="Copyright (c) $LAST_YEAR"
SEARCH2="Copyright \(c\) $LAST_YEAR"
grep -r -l "$SEARCH" --exclude=\*sh . | uniq | xargs perl -e "s/$SEARCH2/Copyright \(c\) $REPLACE/" -pi

echo "The copyright text has been updated to reflect the year $YEAR"

exit 0
