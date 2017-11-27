# Glossary

## Captain

## Cluster
A group of Concourse [nodes](#node) amongst which data is [partitioned](#partition) and [replicated](#replica), but together form a single logical database.  

## Cohort

## Coordinator
The [node](#node) to which a client connects to perform an operation. The coordinator is responsible for routing the request to the appropriate node(s) within the [cluster](#cluster) and returning the result to the client.

In Concourse, coordinators are chosen per operation and any node may serve as a coordinator.

## Ensemble Protocol

## Gossip Protocol

## Key

## Leader

## Node
A Concourse instance that is a member of distributed [cluster](#cluster).

## Optimistic Availability
A property of [distributed systems](https://en.wikipedia.org/wiki/Distributed_computing) that allows tolerance for arbitrary node failure while preserving [availability](https://en.wikipedia.org/wiki/Availability) for an operation as long as the [coordinator](#coordinator) and at least one relevant process agree on the state of the system. In laymen's terms: given sufficient [partitioning](#partition) and [replication](#replica) there is optimism that the system remains available in the face of failure or latency.

## Partition
A subset of nodes in a distributed [cluster](#cluster) that each only contain data certain [token](#token) ranges. Data is partitioned within Concourse to better distributed load across the cluster.

## Record
A [schemaless](#schemaless) group of fields mapping [keys](#key) to [values](#value). A single record should map to a single person, place or thing in the real world.

## Replica
A node

## Schemaless
A Concourse feature that allows users to store data without first specifying the data format or data types with the database, and
2. allows records within the database to contain different formats and data types.

## Strong Consistency
A property of [distributed systems](https://en.wikipedia.org/wiki/Distributed_computing) where all processes *observe* state changes in the same order. According to the [CAP Theorem](https://en.wikipedia.org/wiki/CAP_theorem), distributed database are strongly consistent if every read receives the most recent write or an error, in the event that the most recent write cannot be determined because of network failure or latency.

## Three-Phase Commit

## Token

## Value
