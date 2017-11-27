chronologize(1) -- View a time series with snapshots of a field after every change
==================================================================================

## SYNOPSIS

`chronologize` <key>, <record> -> Map&lt;Timestamp, Set&lt;Object&gt;&gt;<br />
`chronologize` <key>, <record>, <start> -> Map&lt;Timestamp, Set&lt;Object&gt;&gt;<br />
`chronologize` <key>, <record>, <start>, <end> -> Map&lt;Timestamp, Set&lt;Object&gt;&gt;<br />

## PARAMETERS
[String] `key` - the field name<br />
[long] `record` - the record id<br />
[Timestamp] `start` - the first possible timestamp to include in the time series (retrieve using the *time()* function)<br />
[Timestamp] `end` - the timestamp that should be greater than every timestamp in the time series (retrieve using the *time()* function)<br />

## DESCRIPTION
The **chronologize** methods allow you to retrieve a time series that contains data points as snapshots of a field after every change.

  * `chronologize` <key>, <record> -> Map&lt;Timestamp, Set&lt;Object&gt;&gt;:
    View a time series that associates the timestamp of each modification for <key> in <record> to a snapshot containing the values that were stored in the field after the change.

  * `chronologize` <key>, <record>, <start> -> Map&lt;Timestamp, Set&lt;Object&gt;&gt;:
    View a time series between <start> (inclusive) and the present that associates the timestamp of each modification for <key> in <record> to a snapshot containing the values that were stored in the field after the change.

  * `chronologize` <key>, <record>, <start>, <end> -> Map&lt;Timestamp, Set&lt;Object&gt;&gt;:
    View a time series between <start> (inclusive) and <end> (non-inclusive) that associates the timestamp of each modification for <key> in <record> to a snapshot containing the values that were stored in the field after the change.

## SEE ALSO
*audit(1)*<br />
*diff(1)*<br />
*time(1)*<br />

## AUTHOR
Written by Jeff Nelson.

## COPYRIGHT
Copyright (c) 2013-2017 Cinchapi Inc.

## LICENSE
This manual is licensed under the Creative Commons Attribution 4.0 International Public License. <br />
https://creativecommons.org/licenses/by/4.0/
