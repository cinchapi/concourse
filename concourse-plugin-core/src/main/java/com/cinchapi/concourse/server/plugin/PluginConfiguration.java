/*
 * Copyright (c) 2013-2025 Cinchapi Inc.
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import javax.annotation.Nullable;

import ch.qos.logback.classic.Level;

import com.cinchapi.concourse.util.Logging;
import com.cinchapi.lib.config.Configuration;
import com.cinchapi.lib.config.read.Interpreters;
import com.google.common.base.CaseFormat;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
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
public abstract class PluginConfiguration extends Configuration {

    /**
     * The absolute {@link Path} to plugin pref file in the plugin's home dir
     */
    protected static Path PLUGIN_PREFS;

    /**
     * The name of the dev prefs file in the plugin's home directory.
     */
    protected static final String PLUGIN_PREFS_DEV_FILENAME = "plugin.prefs.dev";

    /**
     * The name of the prefs file in the plugin's home directory.
     */
    protected static final String PLUGIN_PREFS_FILENAME = "plugin.prefs";

    /**
     * The default value for the {@link SystemPreference#HEAP_SIZE} preference
     * (in bytes).
     */
    private static final long DEFAULT_HEAP_SIZE_IN_BYTES = 268435456;

    /**
     * The default value for the {@link SystemPreference#DEBUG_PORT} preference
     */
    private static final int DEFAULT_REMOTE_DEBUGGER_PORT = 0;

    /**
     * The absolute path to the dev prefs file in the plugin's home directory.
     */
    private static final Path PLUGIN_PREFS_DEV_LOCATION = PluginRuntime
            .getRuntime().home()
            .resolve(Paths.get("conf", PLUGIN_PREFS_DEV_FILENAME))
            .toAbsolutePath();

    /**
     * The absolute path to the prefs file in the plugin's home directory.
     */
    private static final Path PLUGIN_PREFS_LOCATION = PluginRuntime.getRuntime()
            .home().resolve(Paths.get("conf", PLUGIN_PREFS_FILENAME))
            .toAbsolutePath();

    static {
        // Prevent logging from showing up in the console
        Logging.disable(PluginConfiguration.class);

        // Set location of the plugin preferences files depending on the
        // existence of the preferences files
        PLUGIN_PREFS = Files.exists(PLUGIN_PREFS_DEV_LOCATION)
                ? PLUGIN_PREFS_DEV_LOCATION
                : PLUGIN_PREFS_LOCATION;
    }

    /**
     * Default configuration values that are defined within the plugin. These
     * defaults are used if they are not overridden in the prefs file.
     */
    private final Map<String, Object> defaults = Maps.newHashMap();

    /**
     * Construct a new instance.
     */
    public PluginConfiguration() {
        this(PLUGIN_PREFS);
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
        super(location);
        addDefault(SystemPreference.REMOTE_DEBUGGER_PORT,
                DEFAULT_REMOTE_DEBUGGER_PORT);
        addDefault(SystemPreference.HEAP_SIZE, DEFAULT_HEAP_SIZE_IN_BYTES);
        addDefault(SystemPreference.LOG_LEVEL, Level.INFO.levelStr);
    }

    /**
     * Returns the list of aliases. If no aliases available, it will return a
     * default empty list.
     *
     * @return List<String>.
     */
    public List<String> getAliases() {
        List<String> aliases = get(SystemPreference.ALIAS.getKey(),
                Interpreters.listFromDelimitedString(','));
        if(aliases == null) {
            aliases = get(SystemPreference.ALIASES.getKey(),
                    Interpreters.listFromDelimitedString(','));
        }
        else {
            aliases.addAll(getOrDefault(SystemPreference.ALIASES.getKey(),
                    Interpreters.listFromDelimitedString(','),
                    ImmutableList.of()));
        }
        // TODO: add support default aliases...
        return MoreObjects.firstNonNull(aliases, ImmutableList.of());
    }

    /**
     * Return the heap_size for the plugin's JVM.
     *
     * @return the heap_size preference
     */
    public long getHeapSize() {
        long theDefault = (long) defaults
                .get(SystemPreference.HEAP_SIZE.getKey());
        return getOrDefault(SystemPreference.HEAP_SIZE.getKey(),
                Interpreters.numberOfBytes(), theDefault);
    }

    /**
     * Return the log_level for the plugin's JVM.
     *
     * @return the log_level preference
     */
    public Level getLogLevel() {
        Level theDefault = Level.valueOf(
                (String) defaults.get(SystemPreference.LOG_LEVEL.getKey()));
        return getOrDefault(SystemPreference.LOG_LEVEL.getKey(),
                Interpreters.logLevel(), theDefault);
    }

    /**
     * Return the debug_mode for the plugin's JVM.
     *
     * @return boolean
     */
    public boolean getRemoteDebuggerEnabled() {
        Integer port = getRemoteDebuggerPort();
        return port != null && port > 0;
    }

    /**
     * Return the debug_port for the plugin's JVM.
     *
     * @return int
     */
    public int getRemoteDebuggerPort() {
        int theDefault = (int) defaults
                .get(SystemPreference.REMOTE_DEBUGGER_PORT.getKey());
        return getOrDefault(SystemPreference.REMOTE_DEBUGGER_PORT.getKey(),
                theDefault);
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
            sys = SystemPreference.valueOf(CaseFormat.LOWER_UNDERSCORE
                    .to(CaseFormat.UPPER_UNDERSCORE, key));
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
    protected enum SystemPreference {
        ALIAS(null, ArrayList.class),
        ALIASES(null, ArrayList.class),
        HEAP_SIZE(null, int.class, long.class, Integer.class, Long.class),
        LOG_LEVEL(null, String.class),
        REMOTE_DEBUGGER_PORT(null, int.class, Integer.class);

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
            throw new IllegalArgumentException(
                    value + " is not a valid value for " + getKey());
        }
    }

}
