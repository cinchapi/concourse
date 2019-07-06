package com.cinchapi.concourse.thrift.exceptions.messages;

import com.cinchapi.concourse.thrift.exceptions.GenericMessageException;

@SuppressWarnings({ "unused" })

public class SecurityException extends GenericMessageException {
    private static String description =
        "Signals that a security violation has occurred and the " +
        "currently running session must end immediately";

    public SecurityException() { super(); }

    public SecurityException(String message) { super(message); }

    public SecurityException(GenericMessageException other) { super(other); }
}
