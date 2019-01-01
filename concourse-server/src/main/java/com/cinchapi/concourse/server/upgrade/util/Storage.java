/*
 * Copyright (c) 2013-2019 Cinchapi Inc.
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
package com.cinchapi.concourse.server.upgrade.util;

import java.util.Iterator;
import java.util.List;

import com.cinchapi.common.base.AdHocIterator;
import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.server.GlobalState;
import com.cinchapi.concourse.server.io.FileSystem;
import com.cinchapi.concourse.server.storage.db.BlockStats;
import com.cinchapi.concourse.server.storage.db.Revision;
import com.cinchapi.concourse.util.Environments;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * A simulation of Concourse storage units that can be used to write more fluent
 * upgrade tasks.
 *
 * @author Jeff Nelson
 */
public class Storage {

    /**
     * Return a simulated handler for all of Concourse's environments.
     * 
     * @return the environments
     */
    public static Iterable<Environment> environments() {
        Iterator<String> it = Environments.iterator(
                GlobalState.BUFFER_DIRECTORY, GlobalState.DATABASE_DIRECTORY);
        return new Iterable<Environment>() {

            @Override
            public Iterator<Environment> iterator() {
                return new AdHocIterator<Environment>() {

                    @Override
                    protected Environment findNext() {
                        if(it.hasNext()) {
                            return new Environment(it.next());
                        }
                        else {
                            return null;
                        }
                    }

                };
            }

        };
    }

    /**
     * A simulated handler for a Database's blocks.
     *
     * @author Jeff Nelson
     */
    public static class Block {

        /**
         * The schema version for the Block
         */
        public static long SCHEMA_VERSION = Reflection
                .getStatic("SCHEMA_VERSION", Reflection.getClassCasted(
                        "com.cinchapi.concourse.server.storage.db.Block"));

        /**
         * The real Block, for which interaction happens using reflection.
         */
        private final Object source;

        /**
         * Construct a new instance.
         * 
         * @param source
         */
        private Block(Object source) {
            this.source = source;
        }

        /**
         * Return an {@link Iterable} over all the revisions in the Block.
         * 
         * @return the block's revisions
         */
        public Iterable<Revision<?, ?, ?>> revisions() {
            return Reflection.call(source, "revisions");
        }

        /**
         * If possible, flush the content to disk in a block file, sync the
         * stats, filter and index and finally make the Block immutable.
         */
        public void sync() {
            Reflection.call(source, "sync");
        }

        /**
         * Return the {@link BlockStats}.
         * 
         * @return the stats
         */
        public BlockStats stats() {
            return Reflection.call(source, "stats");
        }

        /**
         * Return {@code true} if this Block is mutable.
         * 
         * @return a boolean indicating the Block's mutability
         */
        public boolean isMutable() {
            return Reflection.get("mutable", source);
        }
    }

    /**
     * A simulated handler for an environment's database.
     *
     * @author Jeff Nelson
     */
    public static class Database {

        /**
         * The real database.
         */
        private final com.cinchapi.concourse.server.storage.db.Database db;

        /**
         * Construct a new instance.
         * 
         * @param path
         */
        private Database(String path) {
            this.db = new com.cinchapi.concourse.server.storage.db.Database(
                    path);
        }

        /**
         * Return a simulated for each immutable (e.g. stored on disk) block in
         * the Database.
         * 
         * @return the blocks
         */
        public Iterable<Block> blocks() {
            List<Object> blocks = Lists.newArrayList();
            ImmutableList.of("cpb", "csb", "ctb").forEach(
                    variable -> blocks.addAll(Reflection.get(variable, db)));
            Iterator<Object> it = blocks.iterator();
            return new Iterable<Block>() {

                @Override
                public Iterator<Block> iterator() {
                    return new AdHocIterator<Block>() {

                        @Override
                        protected Block findNext() {
                            if(it.hasNext()) {
                                Block block = new Block(it.next());
                                if(block.isMutable()) {
                                    // The database creates a new mutable block
                                    // during the #start routine. Since the
                                    // mutable block isn't stored on disk, there
                                    // is no need to upgrade it so it is
                                    // filtered out in the Blocks returned here.
                                    return findNext();
                                }
                                else {
                                    return block;
                                }
                            }
                            else {
                                return null;
                            }
                        }

                    };
                }

            };
        }

        /**
         * Start the database.
         */
        public void start() {
            db.start();
        }

        /**
         * Stop the database.
         */
        public void stop() {
            db.stop();
        }
    }

    /**
     * A simulated handler for a Concourse environment.
     *
     * @author Jeff Nelson
     */
    public static class Environment {

        /**
         * The name of the environment.
         */
        private final String name;

        /**
         * Construct a new instance.
         * 
         * @param name
         */
        private Environment(String name) {
            this.name = name;
        }

        /**
         * Return a simulated handler for the environments' database.
         * 
         * @return the database
         */
        public Database database() {
            return new Database(
                    FileSystem.makePath(GlobalState.DATABASE_DIRECTORY, name));
        }

    }

}
