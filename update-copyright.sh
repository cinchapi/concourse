#!/usr/bin/env bash
# This script will update the copyright notices on every file to include
# the current year (e.g. 2013-2014 will become 2013-2015, etc). This only
# needs to be run once a year. The script is smart enough to get the current
# year without being told, so it is a noop if it is run multiple times in
# the same year
YEAR=`date +"%Y"`
LAST_YEAR=$(($YEAR - 1))
SEARCH="2013-$LAST_YEAR"
REPLACE="2013-$YEAR"

grep -r -l "$SEARCH" --exclude=\*sh . | uniq | xargs perl -e "s/$SEARCH/$REPLACE/" -pi

echo "The copyright text has been updated to reflect the year $YEAR"

exit 0
