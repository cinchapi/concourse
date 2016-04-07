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

# Initialize this repo

# Ensure the script operates from the project root directory
cd "${0%/*}"
this=`basename "$0"`

pip install pdoc
pip install invoke
pip install nose
pip install wheel
pip install twine
pip install -r requirements.txt
hash -r

# Delete this script so it isn't run again
git update-index --assume-unchanged $this
rm $this
exit 0
