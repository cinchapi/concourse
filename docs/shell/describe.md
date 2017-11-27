describe(1) -- For one or more records, list all the keys that have at least one value
=====================================================================================

## SYNOPSIS

`describe` <record> -> Set&lt;String&gt;<br />
`describe` <record>, <timestamp> -> Set&lt;String&gt;<br />
`describe` <records> -> Map&lt;Long, Set&lt;String&gt;&gt;<br />
`describe` <records>, <timestamp> -> Map&lt;Long, Set&lt;String&gt;&gt;<br />

## PARAMETERS
[long] `record` - the record id<br />
[Collection&lt;Long&gt;] `records` - a collection of record ids<br />
[Timestamp] `timestamp` - the historical timestamp to use in the lookup (retrieve using the *time()* function)<br />

## DESCRIPTION
The **describe** methods give a summary containing all the populated keys in a one or more records.

  * `describe` <record> -> Set&lt;String&gt;:
    List all the keys in <record> that have at least one value and return that Set of keys.

  * `describe` <record>, <timestamp> -> Set&lt;String&gt;:
    List all the keys in <record> that had at least one value at <timestamp> and return that Set of keys.

  * `describe` <records> -> Map&lt;Long, Set&lt;String&gt;&gt;:
    For each of the <records>, list all the keys that have at least one value and return a Map associating each record id to the Set of keys in that record.

  * `describe` <records>, <timestamp> -> Map&lt;Long, Set&lt;String&gt;&gt;:
    For each of the <records>, list all the keys that had at least one value at <timestamp> and return a Map associating each record id to the Set of key that were in that record at <timestamp>.

## SEE ALSO
*time(1)*

## AUTHOR
Written by Jeff Nelson.

## COPYRIGHT
Copyright (c) 2013-2017 Cinchapi Inc.

## LICENSE
This manual is licensed under the Creative Commons Attribution 4.0 International Public License. <br />
https://creativecommons.org/licenses/by/4.0/
