package com.cinchapi.concourse.cli.presentation;

import jline.console.ConsoleReader;

import java.io.IOException;

public class ConsoleIO implements IO {
    /*
    TODO: Make this private if/when CommandLineInterface is able to change
    it's protected API
     */
    public final ConsoleReader reader = new ConsoleReader();

    public ConsoleIO() throws IOException { }

    @Override
    public String readLine(String output, Character mask) {
        try {
            return reader.readLine(output);
        } catch (IOException e) {
            // TODO: possibly use `die` here
            System.err.println("ERROR: " + e.getMessage());
            System.exit(2);
            return null;
        }
    }

    @Override
    public void setExpandEvents(boolean expand) {
        reader.setExpandEvents(expand);
    }
}
