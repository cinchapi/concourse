# Introduction to Concourse
Concourse is an [open source](https://github.com/cinchapi/concourse) self-tuning database warehouse that provides [strong consistency](glossary.md#strong-consistency), [decentralized coordination](glossary.md#coordinator) and [optimistic availability](glossary.md#optimistic-availability). Concourse takes the best ideas from SQL and NoSQL databases to provide an intuitive and scalable platform for both transaction and analytic workflows.

## Document-Graph Database
Concourse is a document-graph database.

A [record](/) in Concourse is a [schemaless](/) document, composed of a [unique id](/) and one or more fields. Each field is labeled with a [key](/) and may contain one or more distinct [primitive values](/). Additionally, fields may contain [links](/) to other records, which facilitates modeling a rich graph of descriptive relationships.

Concourse records are similar to linked [JSON](https://en.wikipedia.org/wiki/JSON) objects:

The **document-graph** data model is ideal for development:

* Intuitively mirrors the way that developers think about data.
* Makes few assumptions and adapts to application changes without translation (goodbye [object-relational impedance mismatch](https://en.wikipedia.org/wiki/Object-relational_impedance_mismatch)).
* Models complex relationships with referential integrity while avoiding hacks.

## Automatic Indexing
Concourse **automatically indexes** data for search and analytics while guaranteeing **constant time writes** that are incredibly fast. This eliminates the need to plan queries in advance because Concourse fully supports ad-hoc, range, and predicate queries and caches frequently requested data for optimal performance.

## Version Control
Concourse **automatically tracks changes** to data–like Git does for source code–giving the power to [query data from the past](time-travel.md), [audit](/) changes on the fly and [revert](/) to previous states without downtime. Version control in Concourse makes it easy to build applications that leverage definitive data across time.

## ACID Transactions
Concourse uses a [novel protocol](/) to provide serializable distributed [transactions](transactions.md). Internally, dynamic resource allocation and [just-in-time locking](/) ensure that transactions have both the highest performance and strongest consistency possible. So there is no need to guess when your data will eventually become consistent. When Concourse responds to a query, you can **trust the results immediately**.

By default, each change is [autocommited](/) and written to the database immediately. However, you can explicitly start a [transaction](transactions.md) to control when a group of changes are [atomically](/) [committed](/) or [aborted](/) using the [stage](/) functionality.

* Start a transaction within [concourse shell](/):
```bash
[default/cash]$ stage
```

* Use a [transaction](transaction.md) to atomically transfer $50 from one account to another in [Java](/):
```java
concourse.stage();
try {
  int balance1 = concourse.get("balance", 1);
  int balance2 = concourse.get("balance", 2);
  concourse.set("balance", balance1 + 50, 1);
  concourse.set("balance", balance2 - 50, 2);
  concourse.commit();
}
catch (TransactionException e) {
  concourse.abort();
}
```
* The same example above using Java 8+ [lambdas](/):
```java
concourse.stage(() -> {
  int balance1 = concourse.get("balance", 1);
  int balance2 = concourse.get("balance", 2);
  concourse.set("balance", balance1 + 50, 1);
  concourse.set("balance", balance2 - 50, 2);
});
```

## Plugins
Concourse can be extended by plugins.

## Environments
Concourse separates records into [environments](/), which are similar to *databases*, *keyspaces*, or *schemas* in a other database systems. Beyond that, Concourse **doesn't** impose any other organization such as tables or collections.

You can specify the desired environment when connecting to Concourse. If no environment is specified, the [default_environment](/) defined in the [concourse.prefs](/) configuration file is used.

* Connect to the `production` environment using [concourse shell](shell.md):
```bash
concourse shell -e production
```
* Connect to the `production` environment using the [Java Driver](/):
```java
Concourse concourse = Concourse.connect("production");
```

!!! warning
    It is **not** possible to interact with data in multiple environments simultaneously.

### Create an Environment
If an environment does not exist, Concourse creates it when you first store data within the environment. As such, you can connect to a non-existent environment and perform standard operations.

For a list of restrictions on environment names, see [Naming Restrictions](/).
