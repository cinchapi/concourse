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
package com.cinchapi.concourse.server.plugin;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

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
     * The default value for the {@link SystemPreference#DEBUG_MODE} preference
     */
    private static final boolean DEFAULT_REMOTE_DEBUGGER_ENABLED = false;

    /**
     * The default value for the {@link SystemPreference#DEBUG_PORT} preference
     */
    private static final int DEFAULT_REMOTE_DEBUGGER_PORT = 48410;

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
    private static final Path PLUGIN_PREFS_LOCATION = PluginRuntime
            .getRuntime().home()
            .resolve(Paths.get("conf", PLUGIN_PREFS_FILENAME)).toAbsolutePath();

    static {
        // Prevent logging from showing up in the console
        Logging.disable(PluginConfiguration.class);

        // Set location of the plugin preferences files depending on the
        // existence of the preferences files
        PLUGIN_PREFS = Files.exists(PLUGIN_PREFS_DEV_LOCATION) ? PLUGIN_PREFS_DEV_LOCATION
                : PLUGIN_PREFS_LOCATION;
    }

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
        if (Files.exists(location)) {
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
        addDefault(SystemPreference.REMOTE_DEBUGGER, DEFAULT_REMOTE_DEBUGGER_ENABLED);
        addDefault(SystemPreference.REMOTE_DEBUGGER_PORT, DEFAULT_REMOTE_DEBUGGER_PORT);
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
        if(prefs != null) {
            List<Object> aliases = prefs.getList(SystemPreference.ALIAS
                    .getKey());
            aliases.addAll(prefs.getList(SystemPreference.ALIASES.getKey()));
            return aliases.stream().map(alias -> Objects.toString(alias))
                    .collect(Collectors.toList());
        }
        else {
            return Collections.emptyList();
        }
    }

    /**
     * Return the heap_size for the plugin's JVM.
     *
     * @return the heap_size preference
     */
    public long getHeapSize() {
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
    public Level getLogLevel() {
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
     * Return the debug_mode for the plugin's JVM.
     *
     * @return boolean
     */
    public boolean getRemoteDebuggerEnabled() {
        boolean theDefault = (boolean) defaults.get(
            SystemPreference.REMOTE_DEBUGGER.getKey());
        if (prefs != null) {
            return prefs.getBoolean(
                SystemPreference.REMOTE_DEBUGGER.getKey(),
                theDefault);
        }
        else {
            return theDefault;
        }
    }

    /**
     * Return the debug_port for the plugin's JVM.
     *
     * @return int
     */
    public int getRemoteDebuggerPort() {
        int theDefault = (int) defaults.get(
            SystemPreference.REMOTE_DEBUGGER_PORT.getKey());
        if (prefs != null) {
            return prefs.getInt(
                SystemPreference.REMOTE_DEBUGGER_PORT.getKey(),
                theDefault);
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
    protected enum SystemPreference {
        ALIAS(null, ArrayList.class),
        ALIASES(null, ArrayList.class),
        HEAP_SIZE(null, int.class, long.class, Integer.class, Long.class),
        LOG_LEVEL(null, String.class),
        REMOTE_DEBUGGER(null, boolean.class, Boolean.class),
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
            throw new IllegalArgumentException(value
                    + " is not a valid value for " + getKey());
        }
    }

}
