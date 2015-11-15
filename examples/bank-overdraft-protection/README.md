# bank-overdraft-protection
This example is a simple bank application that supports *overdraft protection*
for customer accounts. With overdraft protection, an attempt is made to transfer
funds from other accounts owned by a customer in order to satisfy a debit or
withdrawal from another account.

## Concepts Explored
* ACID transactions across records
* Atomic updates using `verifyAndSwap`
* Batch inserts
* Connection pooling
* Links and graph traversal
* Multi valued fields
* Queries using CCL
* Unit tests that use an embedded Concourse Server
