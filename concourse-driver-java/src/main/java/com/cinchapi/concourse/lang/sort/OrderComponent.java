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
package com.cinchapi.concourse.lang.sort;

import java.util.Objects;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import com.cinchapi.concourse.Timestamp;

/**
 * An {@link OrderComponent} describes a consideration for ordering a result
 * set.
 *
 * @author Jeff Nelson
 */
@Immutable
public class OrderComponent {

    /**
     * The order key.
     */
    private final String key;

    /**
     * The order timestamp.
     */
    @Nullable
    private final Timestamp timestamp;

    /**
     * The order direction
     */
    private final Direction direction;

    /**
     * Construct a new instance.
     * 
     * @param key
     * @param direction
     */
    public OrderComponent(String key, Direction direction) {
        this(key, null, direction);
    }

    /**
     * Construct a new instance.
     * 
     * @param key
     * @param timestamp
     * @param direction
     */
    public OrderComponent(String key, @Nullable Timestamp timestamp,
            Direction direction) {
        this.key = key;
        this.timestamp = timestamp;
        this.direction = direction;
    }

    /**
     * Return the order direction.
     * 
     * @return the direction
     */
    public Direction direction() {
        return direction;
    }

    public boolean equals(Object obj) {
        if(obj instanceof OrderComponent) {
            return Objects.equals(key, ((OrderComponent) obj).key())
                    && Objects.equals(direction,
                            ((OrderComponent) obj).direction())
                    && Objects.equals(timestamp,
                            ((OrderComponent) obj).timestamp());
        }
        else {
            return false;
        }

    }

    @Override
    public int hashCode() {
        return Objects.hash(key, timestamp, direction);
    }

    /**
     * Return the order key.
     * 
     * @return the key
     */
    public String key() {
        return key;
    }

    /**
     * Return the order timestamp.
     * 
     * @return the timestamp
     */
    @Nullable
    public Timestamp timestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(key);
        if(timestamp != null) {
            sb.append(" at ").append(timestamp);
        }
        sb.append(" ").append(direction);
        return sb.toString();
    }

}
