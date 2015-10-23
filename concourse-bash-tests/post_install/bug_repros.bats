load "setup"

@test "CON-172 repro: Import CLI does not take relative paths" {
    run $CONCOURSE_BIN_DIR/concourse start
    file=$CONCOURSE_BIN_DIR"/../import.csv"
    echo "name,email" >> $file
    echo "jeff,jeff@foo.com" >> $file
    run $CONCOURSE_BIN_DIR/concourse import -d $file
    [[ $output == *"Imported data"* ]]
}
