/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013-2014 Jeff Nelson, Cinchapi Software Collective
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package org.cinchapi.concourse.server.cli;

import org.cinchapi.concourse.server.jmx.ConcourseServerMXBean;

import com.beust.jcommander.Parameter;

/**
 * A debugging tool that dumps the contents of a specified {@link Block}.
 * 
 * @author jnelson
 */
public final class DumpToolCli extends ManagedOperationCli {

    /**
     * Run the program...
     * 
     * @param args
     */
    public static void main(String... args) {
        DumpToolCli cli = new DumpToolCli(args);
        cli.run();
    }

    /**
     * Construct a new instance.
     * 
     * @param opts
     * @param args
     */
    public DumpToolCli(String[] args) {
        super(new DumpToolOptions(), args);
    }

    @Override
    protected void doTask(ConcourseServerMXBean bean) {
        if(((DumpToolOptions) options).list) {
            System.out.println("These are the storage units "
                    + "that are currently dumpable, sorted in "
                    + "reverse chronological order such that units "
                    + "holding newer data appear first.");
            System.out.println(bean.getDumpList());
        }
        else if(((DumpToolOptions) options).id != null) {
            System.out.println(bean.dump(((DumpToolOptions) options).id));
        }
        else {
            throw new IllegalArgumentException(
                    "Please specify the id of "
                            + "a storage unit to dump. Call this CLI with the '--list' "
                            + "flag to list all the available storage units.");
        }

    }

    /**
     * The options that can be passed to the main method of this script.
     * 
     * @author jnelson
     */
    private static class DumpToolOptions extends Options {

        @Parameter(names = { "-i", "--id" }, description = "The id of the storage component to dump. Specify an ID of 'BUFFER' to dump the Buffer content")
        public String id;

        @Parameter(names = { "-l", "--list" }, description = "List the ids of the blocks that can be dumped")
        public boolean list;

    }

}
