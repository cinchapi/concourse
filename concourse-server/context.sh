#!/usr/bin/env bash
# Script to print context info in the project

FILE="context.txt";
rm $FILE
cat <(echo "Last Commit:" & git rev-parse HEAD) <(git status) <(git diff *) > $FILE
exit 0
