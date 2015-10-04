load "setup"

@test "uninstall" {
    run $CONCOURSE_BIN_DIR/concourse uninstall
    [ ! -d $CONCOURSE_BIN_DIR ]
}

@test "can't uninstall while running" {
    run $CONCOURSE_BIN_DIR/concourse start
    run $CONCOURSE_BIN_DIR/concourse uninstall
    echo $output
    [[ $output == *"Cannot uninstall Concourse Server while it is running"* ]]
    [[ $status -ne 0 ]]
}
