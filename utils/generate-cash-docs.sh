#!/usr/bin/env bash
# Generate documentation for CaSH

# Normalize working directory
cd "${0%/*}"
HOME="`pwd -P`"
HOME=`cd ${HOME}; pwd`

RONN_HOME=$HOME/ronn
RONN_GEMS=$RONN_HOME/gems
RONN=$RONN_HOME/bin/ronn
DOCS=$HOME/../docs/md
TARGET=$HOME/../concourse-shell/src/main/resources

# Setup Ronn
cd $RONN_GEMS
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
cd $DOCS
for DOC in `ls | grep .md`
do
  name=${DOC%.md}
  $RONN --man $DOC > $TARGET/$name
done

exit 0
