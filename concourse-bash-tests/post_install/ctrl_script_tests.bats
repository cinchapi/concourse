load "setup"

@test "ensure server starts" {
    run $CONCOURSE_BIN_DIR/concourse start
    echo $output
    [[ $output == *"running PID:"* ]]
}
