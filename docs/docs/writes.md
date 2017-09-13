# Writing Data

## Create a new Record
A new record is created using the `insert` method.

Data can be represented in the form of either a JSON formatted string, a `java.util.Map<String, Set<Object>>` or a `com.google.common.collect.Multimap<String, Object>`.
```gradle
insert({
  "name": "Jeff Nelson",
  "company": "Cinchapi",
  "age": 100,
})
```

## Adding values

## Setting values

## Clearing values

## Atomic Writes

## Simulating a unique index
