# This setup routine is run before each unit test. It has logic to ensure that the test environment is 
# in a state that is conducive to running the unit tests. If so, it handles some boilerplate and exports 
# the following variables:
# $CONCOURSE_TEMP_DIR - The absolute path to a temporary directory that can be used to store files.
# $CONCOURSE_INSTALLER_FILENAME - The absolute path to an installer that can be used to install a server 
#   instance in $CONCOURSE_TEMP_DIR
setup() {
    cwd=`dirname "${BASH_SOURCE[0]}"` # Assumes that setup.bash is in the root of the concourse-bash-tests directory
    distribution_directory="$cwd/../concourse-server/build/distributions"

    # Check to see if the $DISTRIBUTION_DIRECTORY exists, if not fail immediately
    if [ ! -d $distribution_directory ]; then
        echo "Cannot run any unit test because the distribution directory does not exist:
        $distribution_directory
        Please run ./gradlew clean installer" >&2
        false
    fi

    # Get the name of the installer file
    installer=`ls $distribution_directory | grep ".bin$" | head -n 1`
    installer_filename=$distribution_directory/$installer
    if [ -z $installer ] || [ ! -e $installer_filename ]; then
        echo "Cannot run any unit tests because no installer exists in $distribution_directory.
        Please run ./gradlew clean installer" >&2
        false
    fi

    # Create temporary directory and copy the installer there
    CONCOURSE_TEMP_DIR=`mktemp -d $cwd/tmp/XXXXXX`
    CONCOURSE_INSTALLER_FILENAME=$CONCOURSE_TEMP_DIR/concourse-server.bin
    cp $installer_filename $CONCOURSE_INSTALLER_FILENAME
    extend_setup
}

# This teardown routine is run after each unit test.
teardown() {
    extend_teardown
    if [ -d "$CONCOURSE_TEMP_DIR" ]; then
        rm -r $CONCOURSE_TEMP_DIR
    fi
}

# This function can be overridden by tests deeper down the chain to provide additional setup logic.
extend_setup() {
    true
}

# This function can be overriden by tests deeper down the chain to provide additional teardown logic.
extend_teardown() {
    true
}
