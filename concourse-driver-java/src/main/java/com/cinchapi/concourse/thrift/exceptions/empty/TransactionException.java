package com.cinchapi.concourse.thrift.exceptions.empty;

import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.thrift.exceptions.GenericEmptyException;

@SuppressWarnings({ "unused" })

public class TransactionException extends GenericEmptyException {
    private static String description =
        "The base class for all exceptions that happen during (staged) operations in a transaction";
    private static String details =
        "All operations that occur within a transaction should be wrapped in a" +
        "try-catch block so that transaction exceptions can be caught and the" +
        "transaction can be properly aborted.";
    private static String note =
        "Please note that this and all descendant exceptions are unchecked for" +
        "backwards compatibility, but they may be changed to be checked in a future" +
        "API breaking release";

    private static void example(Concourse concourse) {
        try {
            concourse.stage();
            concourse.get("foo", 1);
            concourse.add("foo", "bar", 1);
            concourse.commit();
        } catch (com.cinchapi.concourse.TransactionException e) {
            concourse.abort();
        }
    }

    public TransactionException() { super(); }

    public TransactionException(GenericEmptyException other) { super(other); }
}
