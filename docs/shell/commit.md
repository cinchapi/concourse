commit(1) -- Attempt to permanently apply any changes staged in a transaction
=============================================================================

## SYNOPSIS

`commit` -> boolean<br />

## DESCRIPTION
The **commit** method attempts to permanently apply any work done within a transaction. 

  * `commit` -> boolean:
    Attempt to permanently commit any changes that are staged in a transaction and return <true> if and only if all the changes can be applied. Otherwise, returns <false> and all the changes are discarded.

    After returning, the shell will return to <autocommit> mode and all subsequent changes will be committed immediately.

    This method will return <false> if it is called when the driver is not in <staging> mode.

## SEE ALSO
*abort(1)*<br />
*stage(1)*<br />

## AUTHOR
Written by Jeff Nelson.

## COPYRIGHT
Copyright (c) 2013-2017 Cinchapi Inc.

## LICENSE
This manual is licensed under the Creative Commons Attribution 4.0 International Public License. <br />
https://creativecommons.org/licenses/by/4.0/
