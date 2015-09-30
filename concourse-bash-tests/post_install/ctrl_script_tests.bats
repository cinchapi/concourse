load "setup"

@test "start server" {
    run $CONCOURSE_BIN_DIR/concourse start
    [[ $output == *"running PID:"* ]]
}

@test "stop server" {
    run $CONCOURSE_BIN_DIR/concourse start
    run $CONCOURSE_BIN_DIR/concourse stop
    [[ $output == *"Stopped Concourse Server"* ]]
}

@test "status not started" {
    run $CONCOURSE_BIN_DIR/concourse status
    [[ $output == *"not running"* ]]
}

@test "status started" {
run $CONCOURSE_BIN_DIR/concourse start
    run $CONCOURSE_BIN_DIR/concourse status
    [[ $output == *"is running"* ]]
}
