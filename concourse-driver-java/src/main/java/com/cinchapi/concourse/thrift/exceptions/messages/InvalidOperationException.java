package com.cinchapi.concourse.thrift.exceptions.messages;

import com.cinchapi.concourse.thrift.exceptions.GenericMessageException;

@SuppressWarnings({ "unused" })

public class InvalidOperationException extends GenericMessageException {
    public InvalidOperationException() { super(); }

    public InvalidOperationException(String message) { super(message); }

    public InvalidOperationException(GenericMessageException other) { super(other); }
}
