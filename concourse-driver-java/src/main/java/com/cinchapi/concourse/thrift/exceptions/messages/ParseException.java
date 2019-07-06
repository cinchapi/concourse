package com.cinchapi.concourse.thrift.exceptions.messages;

import com.cinchapi.concourse.thrift.exceptions.GenericMessageException;

@SuppressWarnings({ "unused" })

public class ParseException extends GenericMessageException {
    private static String description =
        "Signals that an unexpected or invalid token was reached while parsing.";

    public ParseException() { super(); }

    public ParseException(String message) { super(message); }

    public ParseException(GenericMessageException other) { super(other); }
}
