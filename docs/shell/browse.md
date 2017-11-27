browse(1) -- For one or more fields, view the values from all records currently or previously stored
====================================================================================================

## SYNOPSIS

`browse` <key> -> Map&lt;Object, Set&lt;Long&gt;&gt;<br />
`browse` <key>, <timestamp> -> Map&lt;Object, Set&lt;Long&gt;&gt;<br />
`browse` <keys> -> Map&lt;String, Map&lt;Object, Set&lt;Long&gt;&gt;&gt;<br />
`browse` <keys>, <timestamp> -> Map&lt;String, Map&lt;Object, Set&lt;Long&gt;&gt;&gt;<br />

## PARAMETERS
[String] `key` - the field name<br />
[Collection&lt;String&gt;] `keys` - a collection of field names<br />
[Timestamp] `timestamp` - the historical timestamp to use in the lookup (retrieve using the *time()* function)<br />

## DESCRIPTION
The **browse** methods give a global view of all the values that have been stored for one or more keys.

  * `browse` <key> -> Map&lt;Object, Set&lt;Long&gt;&gt;:
    View the values from all records that are currently stored for <key> and return a Map associating each value to the Set of records that contain that value in the <key> field.

  * `browse` <key>, <timestamp> -> Map&lt;Object, Set&lt;Long&gt;&gt;:
    View the values from all records that were stored for <key> at <timestamp> and return a Map associating each value to the Set of records that contained that value in the <key> field at <timestamp>.

  * `browse` <keys> -> Map&lt;String, Map&lt;Object, Set&lt;Long&gt;&gt;&gt;:
    View the values from all records that are currently stored for each of the <keys> and return a Map associating each key to a Map associating each value to the Set of records that contain that value in the <key> field.

  * `browse` <keys>, <timestamp> -> Map&lt;String, Map&lt;Object, Set&lt;Long&gt;&gt;&gt;:
    View the values from all records that were stored for each of the <keys> at <timestamp> and return a Map associating each key to a Map associating each value to the Set of records that contained that value in the <key> field at <timestamp>.

## SEE ALSO
*time(1)*

## AUTHOR
Written by Jeff Nelson.

## COPYRIGHT
Copyright (c) 2013-2017 Cinchapi Inc.

## LICENSE
This manual is licensed under the Creative Commons Attribution 4.0 International Public License. <br />
https://creativecommons.org/licenses/by/4.0/
