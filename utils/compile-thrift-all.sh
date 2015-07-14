#!/usr/bin/env bash
# Compile the thrift API for the all languages

. "`dirname "$0"`/.compile-thrift-include"

ME=`basename $0`
cd $THRIFT_DIR"/../utils"
for SCRIPT in `ls | grep compile-thrift`
do
  if [ -f $SCRIPT -a $SCRIPT != $ME ];
    then
    echo
    bash $SCRIPT
  fi
done
