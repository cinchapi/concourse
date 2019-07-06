package com.cinchapi.concourse.thrift.exceptions.messages;

import com.cinchapi.concourse.thrift.exceptions.GenericMessageException;

@SuppressWarnings({ "unused" })

public class InvalidArgumentException extends GenericMessageException {
    public InvalidArgumentException() { super(); }

    public InvalidArgumentException(String message) { super(message); }

    public InvalidArgumentException(GenericMessageException other) { super(other); }
}