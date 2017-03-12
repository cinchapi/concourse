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
package com.cinchapi.concourse.util;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cinchapi.common.reflect.Reflection;
import com.google.common.collect.Maps;

/**
 * A collection of utility methods for programmatically configuring logging
 * preferences.
 * 
 * @author Jeff Nelson
 */
public class Logging {

    /**
     * A cache of the object that is passed to the {@code setLevel} method of
     * the appropriate Logger.
     */
    static Map<String, Object> levelCache = Maps.newHashMap();

    /**
     * Disable ALL logging from {@code clazz}
     * 
     * @param clazz the {@link Class} for which logging should be disabled
     */
    public static void disable(Class<?> clazz) {
        Logger logger = LoggerFactory.getLogger(clazz);
        if(logger instanceof ch.qos.logback.classic.Logger) {
            ((ch.qos.logback.classic.Logger) logger)
                    .setLevel(ch.qos.logback.classic.Level.OFF);
        }
        else if(logger instanceof java.util.logging.Logger) {
            ((java.util.logging.Logger) logger)
                    .setLevel(java.util.logging.Level.OFF);
        }
        else {
            // This code is very brittle because it calls a lot of stuff using
            // reflection. If internal APIs change, this could go bad very
            // quickly. The moral of the story is that everyone should just use
            // Logback, but since that isn't the reality we have to live with
            // this hack. In actuality, the real moral is that client side code
            // should never have logging statements, otherwise we wouldn't have
            // to disable them in the first place.
            String type = logger.getClass().getName();
            Object theLogger = Reflection.get("logger", logger);
            Object level = levelCache.get(type);
            if(level == null) {
                switch (type) {
                case "org.slf4j.impl.Log4jLoggerAdapter":
                default:
                    level = Reflection
                            .getStatic("OFF", Reflection
                                    .getClassCasted("org.apache.log4j.Level"));
                    break;
                }
                levelCache.put(type, level);
            }
            Reflection.call(theLogger, "setLevel", level);
        }
    }
}
