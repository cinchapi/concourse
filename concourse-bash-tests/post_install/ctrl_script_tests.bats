load "setup"

@test "ensure server starts" {
    run $CONCOURSE_BIN_DIR/concourse start
    echo $output
    [[ $output == *"running PID:"* ]]
}

teardown() {
    # Ensure cleanup by stopping server in case it was started
    run $CONCOURSE_BIN_DIR/concourse stop
}
