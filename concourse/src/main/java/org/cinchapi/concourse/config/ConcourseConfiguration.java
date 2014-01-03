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
package org.cinchapi.concourse.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.ConfigurationUtils;
import org.apache.commons.configuration.DefaultFileSystem;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;

import com.google.common.base.Throwables;

/**
 * A {@link PropertiesConfiguration} loader with additional methods.
 * 
 * @author jnelson
 */
public class ConcourseConfiguration extends PropertiesConfiguration {

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
        ((ch.qos.logback.classic.Logger) LoggerFactory
                .getLogger(ConcourseConfiguration.class)).setLevel(Level.OFF);
        ((ch.qos.logback.classic.Logger) LoggerFactory
                .getLogger(ConfigurationUtils.class)).setLevel(Level.OFF);
        ((ch.qos.logback.classic.Logger) LoggerFactory
                .getLogger(DefaultFileSystem.class)).setLevel(Level.OFF);
    }

    /**
     * Load the configuration from {@code file}.
     * 
     * @param file
     * @return the configuration
     */
    public static ConcourseConfiguration loadConfig(String file) {
        try {
            return new ConcourseConfiguration(file);
        }
        catch (ConfigurationException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Construct a new instance.
     * 
     * @param file
     * @throws ConfigurationException
     */
    private ConcourseConfiguration(String file) throws ConfigurationException {
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
