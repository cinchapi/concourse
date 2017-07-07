diff(1) -- List the net changes made to a field, record or index from one timestamp to another.
=====================================================================================================

## SYNOPSIS

`diff` <record>, <start> -> Map&lt;String, Map&lt;Diff, Set&lt;Object&gt;&gt;&gt;<br />
`diff` <record>, <start>, <end> -> Map&lt;String, Map&lt;Diff, Set&lt;Object&gt;&gt;&gt;<br />
`diff` <key>, <record>, <start> -> Map&lt;Diff, Set&lt;Object&gt;&gt;<br />
`diff` <key>, <record>, <start>, <end> -> Map&lt;Diff, Set&lt;Object&gt;&gt;<br />
`diff` <key>, <start> -> Map&lt;Object, Map&lt;Diff, Set&lt;Long&gt;&gt;&gt;<br />
`diff` <key>, <start>, <end> -> Map&lt;Object, Map&lt;Diff, Set&lt;Long&gt;&gt;&gt;<br />

## PARAMETERS
[String] `key` - the field name<br />
[long] `record` - the record id<br />
[Timestamp] `start` - the base timestamp from which the diff is calculated (retrieve using the *time()* function)<br />
[Timestamp] `end` - the comparison timestamp to which the diff is calculated (retrieve using the *time()* function)<br />

## DESCRIPTION
The **diff** methods shows the net changes made to a field, record or index
between two timestamps. Whereas the *audit* method returns a history of all
revisions, this method merely describes the changes that are necessary to
transaction the state from the first timestamp to the second one.

  * `diff` <record>, <start> -> Map&lt;String, Map&lt;Diff, Set&lt;Object&gt;&gt;&gt;:
    List the net changes made to <record> since <start> and return a Map that
    associates each key in <record> to another Map that associates a <change
    description> to the Set of values that fit the description.

  * `diff` <record>, <start>, <end> -> Map&lt;String, Map&lt;Diff, Set&lt;Object&gt;&gt;&gt;:
    List the net changes made to <record> from <start> to <end> and return a Map
    that associates each key in <record> to another Map that associates a
    <change description> to the Set of values that fit the description.

  * `diff` <key>, <record>, <start> -> Map&lt;Diff, Set&lt;Object&gt;&gt;:
    List the net changes made to <key> in <record> since <start> and return a
    Map that associates a <change description> to the Set of values that fit the
    description.

  * `diff` <key>, <record>, <start>, <end> -> Map&lt;Diff, Set&lt;Object&gt;&gt;:
    List the net changes made to <key> in <record> from <start> to <end> and
    return a Map that associates a <change description> to the Set of values
    that fit the description.

  * `diff` <key>, <start> -> Map&lt;Object, Map&lt;Diff, Set&lt;Long&gt;&gt;&gt;:
    List the net changes made to the <key> field across all records since
    <start> and return a Map that associates each value stored for <key> across
    all records to another Map that associates a <change description> to the Set
    of records where the description applies to that value in the <key> field.

  * `diff` <key>, <start>, <end> -> Map&lt;Object, Map&lt;Diff, Set&lt;Long&gt;&gt;&gt;:
    List the net changes made to the <key> field across all records from <start>
    to <end> and return a Map that associates each value stored for <key> across
    all records to another Map that associates a <change description> to the Set
    of records where the description applies to that value in the <key> field.

## CHANGE DESCRIPTION
`ADDED` is an alias for the `Diff.ADDED` description. <br />
`REMOVED` is an alias for the `Diff.REMOVED` description. <br />

## SEE ALSO
*audit(1)* <br />
*chronologize(1)*<br />
*time(1)*

## AUTHOR
Written by Jeff Nelson.

## COPYRIGHT
Copyright (c) 2013-2017 Cinchapi Inc.

## LICENSE
This manual is licensed under the Creative Commons Attribution 4.0 International Public License. <br />
https://creativecommons.org/licenses/by/4.0/
