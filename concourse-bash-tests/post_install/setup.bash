load "../setup"

# Do additional setup for the post install test cases. This function will leave the test case inside the home directory of the server install, but exposes the following:
# $CONCOURSE_BIN_DIR -> the directory that contains all the shell scripts
extend_setup() {
    cd $CONCOURSE_TEMP_DIR;
    CONCOURSE_TEMP_DIR=`pwd -P`
    sh concourse-server.bin -- skip-integration
    cd concourse-server
    prefs="conf/concourse.prefs"
    buffer_directory="buffer_directory = $CONCOURSE_TEMP_DIR/buffer"
    db_directory="db_directory = $CONCOURSE_TEMP_DIR/db"
    client_port="client_port = 0"
    http_port="http_port = 0"
    jmx_port="jmx_port = 0"
    echo $buffer_directory >> $prefs
    echo $db_directory >> $prefs
    echo $client_port >> $prefs
    echo $http_port >> $prefs
    echo $jmx_port >> $prefs
    CONCOURSE_BIN_DIR="$CONCOURSE_TEMP_DIR/concourse-server/bin"
    true
}
