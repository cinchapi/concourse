package com.cinchapi.concourse.thrift.exceptions.messages;

import com.cinchapi.concourse.thrift.exceptions.GenericMessageException;

@SuppressWarnings({ "unused" })

public class ManagementException extends GenericMessageException {
    private static String description = "Thrown when a managed operation fails.";

    public ManagementException() { super(); }

    public ManagementException(String message) { super(message); }

    public ManagementException(GenericMessageException other) { super(other); }
}
