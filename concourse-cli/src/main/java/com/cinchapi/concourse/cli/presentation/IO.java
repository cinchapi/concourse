package com.cinchapi.concourse.cli.presentation;

/*
TODO: Encapsulate output too (bigger deal though)
 */
public interface IO {
    String readLine(String output, Character mask);
    default String readLine(String output) {
        return readLine(output, null);
    }

    void setExpandEvents(boolean expand);
}
