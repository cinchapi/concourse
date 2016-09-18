/*
 * Copyright (c) 2013-2016 Cinchapi Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cinchapi.concourse.server.cli;

import java.nio.file.Files;
import java.nio.file.Paths;

import com.beust.jcommander.Parameter;
import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.server.jmx.ConcourseServerMXBean;
import com.google.common.base.Strings;

/**
 * A management CLI to add/remove/upgrade/etc plugins.
 * 
 * @author Jeff Nelson
 */
public class PluginCli extends ManagedOperationCli {

    /**
     * An enum that represents broad code paths for the CLIs operations.
     * 
     * @author Jeff Nelson
     */
    private static enum CodePath {
        INSTALL_BUNDLE, LIST_BUNDLES, UNINSTALL_BUNDLE, NONE;

        /**
         * Given a collection of {@code options}, figure out the correct
         * {@link CodePath code path} based on precedence rules.
         * 
         * @param options the {@link PluginOptions} that were parsed
         * @return the correct {@link CodePath}
         */
        public static CodePath getCodePath(PluginOptions options) {
            if(options.listBundles) {
                return LIST_BUNDLES;
            }
            else if(!Strings.isNullOrEmpty(options.installBundle)) {
                return INSTALL_BUNDLE;
            }
            else if(!Strings.isNullOrEmpty(options.uninstallBundle)) {
                return UNINSTALL_BUNDLE;
            }
            else {
                return NONE;
            }
        }
    }

    /**
     * Run the program...
     * 
     * @param args
     */
    public static void main(String... args) {
        PluginCli cli = new PluginCli(args);
        cli.run();
    }

    /**
     * Construct a new instance.
     * 
     * @param args
     */
    public PluginCli(String[] args) {
        super(new PluginOptions(), args);
    }

    @Override
    protected void doTask(ConcourseServerMXBean bean) {
        PluginOptions opts = (PluginOptions) this.options;
        CodePath codePath = CodePath.getCodePath(opts);
        switch (codePath) {
        case INSTALL_BUNDLE:
            if(Files.exists(Paths.get(opts.installBundle))) {
                String path = FileSystem.expandPath(opts.installBundle,
                        getLaunchDirectory());
                bean.installPluginBundle(path);
                System.out.println("Successfully installed "
                        + opts.installBundle);
            }
            else {
                throw new UnsupportedOperationException(
                        com.cinchapi.concourse.util.Strings.format(
                                "Cannot download plugin bundle '{}'. Please "
                                        + "manually download the plugin and "
                                        + "provide its local path to the "
                                        + "installer", opts.installBundle));
            }
            break;
        case UNINSTALL_BUNDLE:
            break;
        case LIST_BUNDLES:

            break;
        case NONE:
        default:
            throw new IllegalStateException("Please specify an operation");
        }
    }

    /**
     * Special options for the plugin cli.
     * 
     * @author Jeff Nelson
     */
    protected static class PluginOptions extends Options {

        @Parameter(names = { "-i", "--install-bundle" }, description = "The name or path to a plugin bundle to install")
        public String installBundle;

        @Parameter(names = { "-x", "--uninstall-bundle" }, description = "The name of the plugin bundle to uninstall")
        public String uninstallBundle;

        @Parameter(names = { "-l", "--list-bundles" }, description = "list all the available plugins")
        public boolean listBundles;
    }

}
