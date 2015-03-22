add(1) -- Append a value to a key within a record.
==================================================

## USAGE
**Append** a value to a key within a record if it doesn't currently exist.

### Methods
You can use Java or Groovy syntax (e.g. no paranthesis) to invoke methods in CaSH. Groovy syntax is only parseable in standalone statements.

**method** arg1, arg2, arg3 *OR* **method(**arg1, arg2, **method1(**arg3**)****)**

* `add`(key, value) -> *long*:
    Add *key* (String) as *value* (Object) within a new record and return its id.

* `add`(key, value, record) -> *long*:
    Add *key* (String) as *value* (Object) within *record* (long) if it does not currently exist and return a boolean that indicates success or failure.

* `add`(key, value, [records]) -> *Map[Long, Boolean]*:
    Add *key* (String) as *value* (Object) in each of the *records* (List[Long]) in the list. Return a mapping from each of the records to a boolean that indicates success or failure.

## AUTHOR
Jeff Nelson
Cinchapi, Inc.
