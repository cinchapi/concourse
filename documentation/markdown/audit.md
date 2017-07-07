audit(1) -- List the changes made to a *field* or *record* over time
====================================================================

## SYNOPSIS

`audit` <record> -> Map&lt;Timestamp, String&gt;<br />
`audit` <record>, <start> -> Map&lt;Timestamp, String&gt;<br />
`audit` <record>, <start>, <end> -> Map&lt;Timestamp, String&gt;<br />
`audit` <key>, <record> -> Map&lt;Timestamp, String&gt;<br />
`audit` <key>, <record>, <start> -> Map&lt;Timestamp, String&gt;<br />
`audit` <key>, <record>, <start>, <end> -> Map&lt;Timestamp, String&gt;<br />

## PARAMETERS
[String] `key` - the field name<br />
[long] `record` - the record id<br />
[Timestamp] `start` - an inclusive timestamp for the oldest change that should possibly be included in the audit (retrieve using the *time()* function)<br />
[Timestamp] `end` - a non-inclusive timestamp for the most recent change that should possibly be included in the audit (retrieve using the *time()* function)<br />

## DESCRIPTION
The **audit** methods allow you to retrieve a log of descriptions for changes to a field or record over time.

  * `audit` <record> -> Map&lt;Timestamp, String&gt;:
    List all the changes ever made to <record> and for each change, a mapping from timestamp to a description of the revision.

  * `audit` <record>, <start> -> Map&lt;Timestamp, String&gt;:
    List all the changes made to <record> since <start> (inclusive) and for each change, a mapping from timestamp to a description of the revision.

  * `audit` <record>, <start>, <end> -> Map&lt;Timestamp, String&gt;:
    List all the changes made to <record> between <start> (inclusive) and <end> (non-inclusive) and for each change, a mapping from timestamp to a description of the revision.

  * `audit` <key>, <record> -> Map&lt;Timestamp, String&gt;:
    List all the changes ever made to the <key> field in <record> and for each change, a mapping from timestamp to a description of the revision.

  * `audit` <key>, <record>, <start> -> Map&lt;Timestamp, String&gt;:
    List all the changes ever made to the <key> field in <record> since <start> (inclusive) and for each change, a mapping from timestamp to a description of the revision.

  * `audit` <key>, <record>, <start>, <end> -> Map&lt;Timestamp, String&gt;:
    List all the changes ever made to <key> field in <record> between <start> (inclusive) and <end> (non-inclusive) and and for each change, a mapping from timestamp to a description of the revision.

## SEE ALSO
*chronologize(1)*<br />
*diff(1)*<br />
*time(1)*

## AUTHOR
Written by Jeff Nelson.

## COPYRIGHT
Copyright (c) 2013-2017 Cinchapi Inc.

## LICENSE
This manual is licensed under the Creative Commons Attribution 4.0 International Public License. <br />
https://creativecommons.org/licenses/by/4.0/
