clear(1) -- Atomically modify a field to only contain the exact values
from a collection.
==================================================================================================

## SYNOPSIS

`reconcile` <key>, <record>, <values>  -> void<br />

## PARAMETERS
[String] `key` - the field name<br />
[long] `record` - the record id<br />
[Collection&lt;Object&gt;] `values` - the values that should be exactly what is
contained in the field after this method executes<br />

## DESCRIPTION
The **reconcile** method atomically modifies a field to only contain the exact
values from a collection.

  * `reconcile` <key>, <record>, <values> -> void:
    Atomically make the necessary changes to the data stored for <key> in
    <record> so that it contains the exact same <values> as the specified
    collection.

## AUTHOR
Written by Jeff Nelson.

## COPYRIGHT
Copyright (c) 2013-2017 Cinchapi Inc.

## LICENSE
This manual is licensed under the Creative Commons Attribution 4.0 International Public License. <br />
https://creativecommons.org/licenses/by/4.0/
