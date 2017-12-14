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

# Generate documentation for CaSH

# First check to see if ruby and gem binaries are installed on the $PATH. If not, just display a warning and exist
hash ruby 2>/dev/null || { echo >&2 "Ruby is required to generate CaSH documentation. Aborting."; exit 0; }
hash gem 2>/dev/null || { echo >&2 "RubyGems is required to generate CaSH documentation. Aborting."; exit 0; }

# Normalize working directory
cd "${0%/*}"
HOME="`pwd -P`"
HOME=`cd "${HOME}"; pwd`

RONN_HOME=$HOME/ronn
RONN_GEMS=$RONN_HOME/gems
RONN=$RONN_HOME/bin/ronn
DOCS=$HOME/../docs/shell
TARGET=$HOME/../concourse-shell/src/main/resources

# Setup Ronn
cd "$RONN_GEMS"
installed=`gem list rdiscount -i`
if [ $installed != "true" ]; then
  gem install rdiscount
fi
installed=`gem list hpricot -i`
if [ $installed != "true" ]; then
  gem install hpricot
fi
installed=`gem list mustache -i`
if [ $installed != "true" ]; then
  gem install mustache
fi

# Generate all the docs
cd "$DOCS"
for DOC in `ls | grep .md`
do
  name=${DOC%.md}
  "$RONN" --man "$DOC" > "$TARGET/$name" 2>/dev/null
done

exit 0
