add(1) -- Append a *key* as a *value* in one or more *records*
==============================================================

## SYNOPSIS

`add` <key>, <value> -> long<br />
`add` <key>, <value>, <record> -> boolean<br />
`add` <key>, <value>, <records> -> Map&lt;Long, Boolean&gt;<br />

## PARAMETERS
[String] `key` - the field name<br />
[Object] `value` - the value to add<br />
[long] `record` - the record id where an attempt is made to add the data<br />
[Collection&lt;Long&gt;] `records` - a collection of record ids where an attempt is made to add the data<br />

## DESCRIPTION
The **add** methods allow you to *append* a value to a field without overwriting any previously stored data.

  * `add` <key>, <value> -> long:
    Append <key> as <value> in a new record and return the id.

  * `add` <key>, <value>, <record> -> boolean:
    Append <key> as <value> in <record> if and only if it doesn't exist and return a boolean that indicates if the data was added.

  * `add` <key>, <value>, <records> -> Map&lt;Long, Boolean&gt;:
    Append <key> as <value> in each of the <records> where it doesn't exist and return a mapping from each record id to a boolean that indicates if the data was added.

## AUTHOR
Written by Jeff Nelson.

## COPYRIGHT
Copyright (c) 2013-2017 Cinchapi Inc.

## LICENSE
This manual is licensed under the Creative Commons Attribution 4.0 International Public License. <br />
https://creativecommons.org/licenses/by/4.0/
