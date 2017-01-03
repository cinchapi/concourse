clear(1) -- Atomically remove all the values from one or more fields
====================================================================

## SYNOPSIS

`clear` <record> -> void<br />
`clear` <records> -> void<br />
`clear` <key>, <record> -> void<br />
`clear` <keys>, <record> -> void<br />
`clear` <key>, <records> -> void<br />
`clear` <keys>, <records> -> void<br />

## PARAMETERS
[String] `key` - the field name<br />
[Collection&lt;String&gt;] `keys` - a collection of field names<br />
[long] `record` - the record id<br />
[Collection&lt;Long&gt;] `records` - a collection of record ids<br />

## DESCRIPTION
The **clear** methods atomically remove all the values that are stored within one or more fields.

  * `clear` <record> -> void:
    Atomically remove all the values stored for every key in <record>.

  * `clear` <records> -> void:
    Atomically remove all the values stored for every key in each of the <records>.

  * `clear` <key>, <record> -> void:
    Atomically remove all the values stored for <key> in <record>.

  * `clear` <keys>, <record> -> void:
    Atomically remove all the values stored for each of the <keys> in <record>.

  * `clear` <key>, <records>,  -> void:
    Atomically remove all the values stored for <key> in each of the <records>.

  * `clear` <keys>, <records> -> void:
    Atomically remove all the values stored for each of the <keys> in each of the <records>.

## SEE ALSO
*remove(1)*<br />

## AUTHOR
Written by Jeff Nelson.

## COPYRIGHT
Copyright (c) 2013-2017 Cinchapi Inc.

## LICENSE
This manual is licensed under the Creative Commons Attribution 4.0 International Public License. <br />
https://creativecommons.org/licenses/by/4.0/
