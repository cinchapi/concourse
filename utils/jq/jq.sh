#!/usr/bin/env bash

# Copyright (c) 2021 Cinchapi Inc.
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

# Normalize working directory
cd "${0%/*}"
HOME="`pwd -P`"
HOME=`cd "${HOME}"; pwd`


# Get the name of the OS in case there is OS-spceific configuration that needs
# to occur. Variable values will generally correspond as follows:
#
# Value   | OS
# -----------------
# darwin  | OS X
# linux   | CentOS
OS=`uname -s | tr "[A-Z]" "[a-z]" | tr -d ' '`
cygwin=false
msys=false
darwin=false
case $OS in
  cygwin* )
  cygwin=true
  ;;
  darwin* )
  darwin=true
  ;;
  mingw* )
  msys=true
  ;;
esac

if [ "$darwin" = "true" ]; then
  prg="$HOME/jq-osx-amd64"
else
  prg="$HOME/jq-linux64"
fi

exec $prg "$@"