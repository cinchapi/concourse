package com.cinchapi.concourse.thrift.exceptions.messages;

import com.cinchapi.concourse.thrift.exceptions.GenericMessageException;

@SuppressWarnings({ "unused" })

public class DuplicateEntryException extends GenericMessageException {
    private static String description =
        "Signals that an attempt to conditionally add data based on" +
        "a condition that should be unique, cannot happen because the " +
        "condition is not unique.";

    public DuplicateEntryException() { super(); }

    public DuplicateEntryException(String message) { super(message); }

    public DuplicateEntryException(GenericMessageException other) { super(other); }
}
