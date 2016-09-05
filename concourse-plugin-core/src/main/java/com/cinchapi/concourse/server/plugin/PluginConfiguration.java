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
package com.cinchapi.concourse.server.plugin;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import javax.annotation.Nullable;

import org.apache.commons.configuration.ConfigurationException;

import ch.qos.logback.classic.Level;

import com.cinchapi.concourse.config.PreferencesHandler;
import com.cinchapi.concourse.util.Logging;
import com.google.common.base.CaseFormat;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;

/**
 * A collection of preferences that are used to configure a plugin's JVM and
 * possible internal operations.
 * <p>
 * Defaults can be {@link #addDefault(String, Object) declared} programmatically
 * by the plugin and can be overridden by the user by specifying a prefs file in
 * the plugin's home directory.
 * </p>
 * 
 * @author Jeff Nelson
 */
public abstract class PluginConfiguration {

    static {
        // Prevent logging from showing up in the console
        Logging.disable(PluginConfiguration.class);
    }

    /**
     * The default value for the {@link SystemPreference#HEAP_SIZE} preference
     * (in bytes).
     */
    private static final long DEFAULT_HEAP_SIZE_IN_BYTES = 268435456;

    /**
     * The name of the prefs file in the plugin's home directory.
     */
    protected static final String PLUGIN_PREFS_FILENAME = "plugin.prefs";

    /**
     * The absolute path to the prefs file in the plugin's home directory.
     */
    private static final Path PLUGIN_PREFS_LOCATION = Paths.get(
            System.getProperty(Plugin.PLUGIN_HOME_JVM_PROPERTY),
            PLUGIN_PREFS_FILENAME).toAbsolutePath();

    /**
     * Default configuration values that are defined within the plugin. These
     * defaults are used if they are not overridden in the prefs file.
     */
    private final Map<String, Object> defaults = Maps.newHashMap();

    /**
     * A handler to the underlying prefs file, if it exists within the plugin's
     * home directory.
     */
    @Nullable
    private final PreferencesHandler prefs;

    /**
     * Construct a new instance.
     */
    public PluginConfiguration() {
        this(PLUGIN_PREFS_LOCATION);
    }

    /**
     * DO NOT CALL
     * <p>
     * Provided for the plugin manager to create a local handler for every
     * plugin's preferences.
     * </p>
     * 
     * @param location
     */
    protected PluginConfiguration(Path location) {
        if(Files.exists(location)) {
            try {
                this.prefs = new PreferencesHandler(location.toString()) {};
            }
            catch (ConfigurationException e) {
                throw Throwables.propagate(e);
            }
        }
        else {
            this.prefs = null;
        }
        addDefault(SystemPreference.HEAP_SIZE, DEFAULT_HEAP_SIZE_IN_BYTES);
        addDefault(SystemPreference.LOG_LEVEL, Level.INFO.levelStr);
    }

    /**
     * Return the heap_size for the plugin's JVM.
     * 
     * @return the heap_size preference
     */
    public final long getHeapSize() {
        long theDefault = (long) defaults.get(SystemPreference.HEAP_SIZE
                .getKey());
        if(prefs != null) {
            return prefs.getSize(SystemPreference.HEAP_SIZE.getKey(),
                    theDefault);
        }
        else {
            return theDefault;
        }
    }

    /**
     * Return the log_level for the plugin's JVM.
     * 
     * @return the log_level preference
     */
    public final Level getLogLevel() {
        Level theDefault = Level.valueOf((String) defaults
                .get(SystemPreference.LOG_LEVEL.getKey()));
        if(prefs != null) {
            return Level.valueOf(prefs.getString(
                    SystemPreference.LOG_LEVEL.getKey(), theDefault.levelStr));
        }
        else {
            return theDefault;
        }
    }

    /**
     * Define a default preference to be used if not provided in the prefs file.
     * 
     * @param key
     * @param value
     */
    protected void addDefault(String key, Object value) {
        SystemPreference sys = null;
        try {
            sys = SystemPreference.valueOf(CaseFormat.LOWER_UNDERSCORE.to(
                    CaseFormat.UPPER_UNDERSCORE, key));
        }
        catch (IllegalArgumentException e) {/* no-op */}
        if(sys != null) {
            addDefault(sys, value);
        }
        else {
            defaults.put(key, value);
        }
    }

    /**
     * Define a default preference to be used if not provided in the prefs file.
     * 
     * @param key
     * @param value
     */
    protected void addDefault(SystemPreference key, Object value) {
        key.validate(value);
        defaults.put(key.getKey(), value);
    }

    /**
     * A collection of "system" preferences with (possibly) special validation
     * rules.
     * 
     * @author Jeff Nelson
     */
    private enum SystemPreference {
        HEAP_SIZE(null, int.class, long.class, Integer.class, Long.class),
        LOG_LEVEL(null, String.class);

        /**
         * A function that can be defined to validate values for this
         * preference.
         */
        @Nullable
        private final Function<Object, Boolean> validator;

        /**
         * A list of valid classes that values for this preference can hold
         */
        private final Class<?>[] validTypes;

        /**
         * Construct a new instance.
         * 
         * @param validator
         * @param validTypes
         */
        SystemPreference(Function<Object, Boolean> validator,
                Class<?>... validTypes) {
            this.validator = validator;
            this.validTypes = validTypes;
        }

        /**
         * Return the canonical key for the system preference.
         * 
         * @return the canonical key
         */
        public String getKey() {
            return name().toLowerCase();
        }

        /**
         * Determine if {@code value} is valid for this preference.
         * 
         * @param value the value to validate
         * @return {@code true} if the value is valid
         */
        public boolean validate(Object value) {
            Class<?> clazz = value.getClass();
            for (Class<?> type : validTypes) {
                if(type.isAssignableFrom(clazz)) {
                    if(validator != null) {
                        return validator.apply(value);
                    }
                    else {
                        return true;
                    }
                }
            }
            throw new IllegalArgumentException(value
                    + " is not a valid value for " + getKey());
        }
    }

}
