#!/usr/bin/env bash
# Compile the thrift API for the PHP service

. "`dirname "$0"`/.compile-thrift-include"

TARGET="../concourse-driver-php"
PACKAGE=$TARGET"/cinchapi/concourse/src"

cd $THRIFT_DIR

# Run the thrift compile
thrift -out $PACKAGE -gen php concourse.thrift

if [ $? -ne 0 ]; then
  exit 1
fi

API=$PACKAGE"/thrift/ConcourseService.php"

# Replace all TransactionToken declarations to take no type since we allow that
# parameter to legally be null in method invocations.
perl -p -i -e "s/, \\\\thrift\\\\shared\\\\TransactionToken/, /g" $API

# PHP doesn't allow array keys that are not integers or strings and the Thrift
# compiler isn't smart enough to generate code that gets around that limitation.
# We must replace offending code blocks with logic that will serialize invalid
# array keys and also ensure that arrays are populated in a sensible manner.
perl -i -0pe 's/if\s*\(is_scalar\(\$([A-Za-z]{1,}\d{0,})\)\)\s*\{\n\s*\$([A-Za-z]{1,}\d{0,})\[\$\1\]\s*=\s*true;\n\s*\}[\s*|\n\s*]else\s*\{\n\s*\$\2\s*\[\]=\s*\$\1;\n\s*\}\n\s*\}\n\s*\$xfer\s*\+=\s*\$input->readSetEnd\(\);\n\s*(\$this->success\[\$([A-Za-z]{1,}[0-9]{0,})\]\s*=\s*\$\2;)/\$$2\[\$$1\] = \$$1;\n\t\t\t  }\n\t\t\t  \$$4 = (!is_integer(\$$4) && !is_string(\$$4)) ? serialize(\$$4) : \$$4;\n\t\t\t  $3/g' $API

echo "Finished compiling the Thrift API for PHP to "$(cd $PACKAGE && pwd)

exit 0
