#!/usr/bin/env bash

# Copyright (c) 2013-2024 Cinchapi Inc.
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

# Check if groff is installed
if ! command -v groff >/dev/null 2>&1; then
  echo "groff is not installed. Attempting to install..."
  if [[ "$OSTYPE" == "linux-gnu"* ]]; then
      if [ -f /etc/debian_version ]; then
          sudo apt-get update
          sudo apt-get install -y groff
      elif [ -f /etc/redhat-release ]; then
          sudo yum update
          sudo yum install -y groff
      else
          echo "Unsupported Linux distribution."
          exit 1
      fi
  elif [[ "$OSTYPE" == "darwin"* ]]; then
      if command -v brew >/dev/null 2>&1; then
          brew install groff
      else
          echo "Homebrew is not installed. Please install Homebrew first."
          exit 1
      fi
  else
      echo "Unsupported operating system."
      exit 1
  fi
fi

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
overall_success=true
for DOC in $(ls | grep .md)
do
  name=${DOC%.md}
  "$RONN" --man "$DOC" > "$TARGET/$name" 2> >(grep -v "warn: cannot load such file -- ronn" >&2)
  if [ $? -ne 0 ]; then
    echo "Error processing $DOC"
    overall_success=false
  fi
done

if [ "$overall_success" = true ]; then
    echo "All documentation files were processed successfully."
    exit 0
else
    echo "There were errors in processing some documentation files."
    exit 1
fi
