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
package com.cinchapi.concourse.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.ConfigurationUtils;
import org.apache.commons.configuration.DefaultFileSystem;
import org.apache.commons.configuration.PropertiesConfiguration;
import com.cinchapi.concourse.util.Logging;

/**
 * A {@link PreferencesHandler} parses a {@code .prefs} file for programmatic
 * management (i.e. reading/writing values).
 * 
 * @author Jeff Nelson
 */
public class PreferencesHandler extends PropertiesConfiguration {

    /*
     * This implementation uses lower case strings when putting and getting from
     * the map in order to facilitate case insensitivity. DO NOT use any other
     * methods beyond #get and #put because those have not been overridden to
     * account for the case insensitivity.
     */
    private static Map<String, Integer> multipliers = new HashMap<String, Integer>() {

        private static final long serialVersionUID = 1L;

        @Override
        public Integer get(Object key) {
            if(key instanceof String) {
                key = ((String) key).toLowerCase();
            }
            return super.get(key);
        }

        @Override
        public Integer put(String key, Integer value) {
            key = key.toLowerCase();
            return super.put(key, value);
        }

    };
    static {
        multipliers.put("b", (int) Math.pow(1024, 0));
        multipliers.put("k", (int) Math.pow(1024, 1));
        multipliers.put("kb", (int) Math.pow(1024, 1));
        multipliers.put("m", (int) Math.pow(1024, 2));
        multipliers.put("mb", (int) Math.pow(1024, 2));
        multipliers.put("g", (int) Math.pow(1024, 3));
        multipliers.put("gb", (int) Math.pow(1024, 3));

        // Prevent logging from showing up in the console
        Logging.disable(PreferencesHandler.class);
        Logging.disable(ConfigurationUtils.class);
        Logging.disable(DefaultFileSystem.class);
    }

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
    protected PreferencesHandler(String file) throws ConfigurationException {
        // NOTE: {@code file} must be absolute because the Apache configuration
        // framework has some twisted logic that resolves relative file paths in
        // a non-intuitive manner. I know it is tempting to support allowing the
        // caller to input a relative path by adding logic here to expand that
        // relative path using the {@link FileOps#expandPath()} method. The
        // problem is that expanding the path correctly requires knowledge of
        // the working directory, which is not consistently known to us here.
        // CLIs can provide their working directory; however, user applications
        // (those that construct Concourse and ConnectionPool objects with
        // preferences) might not necessarily know their working directory, and
        // even if they did, we wouldn't automatically have access to it). So,
        // the easiest this is to simply require people to expand paths
        // themselves and pass in the absolute file path to us.
        super(file);
        setAutoSave(true);
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
        String value = getString(key, null);
        if(value != null) {
            String[] parts = value.split("(?<=\\D)(?=\\d)|(?<=\\d)(?=\\D)");
            long base = Long.parseLong(parts[0]);
            int multiplier;
            if(parts.length > 1) {
                multiplier = multipliers.get(parts[1]);
            }
            else {
                multiplier = multipliers.get("b");
            }
            return base * multiplier;
        }
        else {
            return defaultValue;
        }
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
        String value = getString(key, null);
        if(value != null) {
            try {
                defaultValue = (T) Enum.valueOf(defaultValue.getClass(), value);
            }
            catch (IllegalArgumentException e) {
                // ignore
            }
        }
        return defaultValue;
    }
}
