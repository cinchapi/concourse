abort(1) -- Abort the current transaction and discard any staged changes
========================================================================

## SYNOPSIS

`abort` -> void<br />

## DESCRIPTION
The **abort** method turns off *staging* mode and returns the shell to
*autocommit* mode where all subsequent changes are committed immediately. Calling this method when the shell is not in *staging* mode is a no-op.

  * `abort` -> void:
    Abort the current transaction and discard and changes that are currently staged.

## AUTHOR
Written by Jeff Nelson.

## COPYRIGHT
Copyright (c) 2013-2017 Cinchapi Inc.

## LICENSE
This manual is licensed under the Creative Commons Attribution 4.0 International Public License. <br />
https://creativecommons.org/licenses/by/4.0/
