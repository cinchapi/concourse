/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2014 Jeff Nelson, Cinchapi Software Collective
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

/**
 * A tool that is used to manage the environments in Concourse Server.
 * 
 * @author jnelson
 */
public class EnvToolCli extends ManagedOperationCli {

    /**
     * Run the program...
     * 
     * @param args
     */
    public static void main(String... args) {
        EnvToolCli cli = new EnvToolCli(args);
        cli.run();
    }

    /**
     * Construct a new instance.
     * 
     * @param options
     * @param args
     */
    public EnvToolCli(String[] args) {
        super(new EnvToolOptions(), args);
    }

    @Override
    protected void doTask(ConcourseServerMXBean bean) {
        System.out.println("These are the environments in Concourse Server:");
        System.out.println(bean.listAllEnvironments());
    }

    /**
     * The options that can be passed to the main method of this script.
     * 
     * @author jnelson
     */
    private static final class EnvToolOptions extends Options {} // NOTE: not
                                                                 // extending
                                                                 // EnvironmentOptions
                                                                 // because any
                                                                 // management
                                                                 // options that
                                                                 // we take on
                                                                 // an
                                                                 // environment
                                                                 // (i.e.
                                                                 // delete)
                                                                 // should be
                                                                 // specified as
                                                                 // --delete
                                                                 // <env>
                                                                 // instead of
                                                                 // --delete -e
                                                                 // <env>

}
