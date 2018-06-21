/*
 * Copyright (c) 2013-2018 Cinchapi Inc.
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
package com.cinchapi.concourse.config;

import org.apache.commons.configuration.ConfigurationException;
import com.cinchapi.lib.config.Configuration;
import com.cinchapi.lib.config.read.Interpreters;

/**
 * A {@link PreferencesHandler} parses a {@code .prefs} file for programmatic
 * management (i.e. reading/writing values).
 * 
 * @author Jeff Nelson
 */
public class PreferencesHandler extends Configuration {

    /**
     * Construct a new instance.
     * <p>
     * The implementing class is encouraged to provide a static "open" method
     * that catches {@link ConfigurationException} and propagates it as a
     * {@link RuntimeException} so that users don't have to worry about it.
     * </p>
     * 
     * @param file the absolute path to the preferences file (relative paths
     *            will resolve to the user's home directory)
     * @throws ConfigurationException
     */
    protected PreferencesHandler(String file) {
        super(file);
    }

    /**
     * Get the size description associated with the given key. If the key
     * doesn't map to an existing object, the defaultValue is returned.
     * 
     * @param key
     * @param defaultValue
     * @return the associated size description if key is found, defaultValue
     *         otherwise
     */
    public long getSize(String key, long defaultValue) {
        return getOrDefault(key, Interpreters.numberOfBytes(), defaultValue);
    }

    /**
     * Get the enum value associated with the given key. If the key doesn't map
     * to an existing object or the mapped value is not a valid enum, the
     * defaultValue is returned
     * 
     * @param key
     * @param defaultValue
     * @return the associated enum if key is found, defaultValue
     *         otherwise
     */
    @SuppressWarnings("unchecked")
    public <T extends Enum<T>> T getEnum(String key, T defaultValue) {
        return getOrDefault(key,
                Interpreters.enumValue(defaultValue.getClass()), defaultValue);
    }
}
