/*
 * Copyright (c) 2013-2016 Cinchapi Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cinchapi.concourse.server.plugin;

import com.cinchapi.concourse.annotate.PackagePrivate;

/**
 * 
 * 
 * @author Jeff Nelson
 */
@PackagePrivate
abstract class RealTimePlugin extends Plugin {

    /**
     * Construct a new instance.
     * @param station
     * @param notifier
     */
    public RealTimePlugin(String station, Object notifier) {
        super(station, notifier);
    }
    
    @Override
    public void run(){
        // first message from server has to be the address of the real time queue
        //TODO add a thread/shared memory to listen for real time write updates from the server
        super.run();
    }

}
