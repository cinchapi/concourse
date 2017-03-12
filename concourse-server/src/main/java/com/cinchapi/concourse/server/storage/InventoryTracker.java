/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
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
package com.cinchapi.concourse.server.storage;

/**
 * A {@link Store} that keeps track of the {@link Inventory inventory} of
 * records that have been created.
 * 
 * @author Jeff Nelson
 */
public interface InventoryTracker {

    /**
     * Return the Buffer's inventory collection.
     * 
     * @return the {@link Inventory} that is associated with this instance
     */
    public Inventory getInventory();

}
