add(1) -- Append a *key* as a *value* in one or more *records*
==============================================================

## SYNOPSIS

`add` <key>, <value> --> long<br />
`add` <key>, <value>, <record> --> boolean<br />
`add` <key>, <value>, <records> --> Map&lt;Long, Boolean&gt;<br />

## DESCRIPTION
The **add** methods allow you to *append* a value to a field without overwriting and previously stored data.

  * `add` <key>, <value> -> long:
    Append <key> as <value> in a new record and return the id.

  * `add` <key>, <value>, <record> -> boolean:
    Append <key> as <value> to <record> if and only if it doesn't exist.

  * `add` <key>, <value>, <records> -> Map&lt;Long, Boolean&gt;:
    Append <key> as <value> in each of the <records> where it is doesn't exist.

## AUTHOR
Written by Jeff Nelson.

## COPYRIGHT
Copyright (c) 2015 Cinchapi Inc.

## LICENSE
This manual is licensed under the Creative Commons Attribution 4.0 International Public License. <br />
https://creativecommons.org/licenses/by/4.0/
