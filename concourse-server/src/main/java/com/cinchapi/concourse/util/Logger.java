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

import java.io.File;

import org.apache.thrift.ProcessFunction;
import org.apache.thrift.server.TThreadPoolServer;
import org.slf4j.LoggerFactory;

import com.cinchapi.concourse.server.GlobalState;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.ConsoleAppender;
import ch.qos.logback.core.rolling.FixedWindowRollingPolicy;
import ch.qos.logback.core.rolling.RollingFileAppender;
import ch.qos.logback.core.rolling.SizeBasedTriggeringPolicy;

/**
 * Contains methods to print log messages in the appropriate files.
 * 
 * @author Jeff Nelson
 */
public final class Logger {

    /**
     * Print {@code message} with {@code params} to the INFO log.
     * 
     * @param message
     * @param params
     */
    public static void info(String message, Object... params) {
        INFO.info(message, params);
    }

    /**
     * Print {@code message} with {@code params} to the ERROR log.
     * 
     * @param message
     * @param params
     */
    public static void error(String message, Object... params) {
        ERROR.error(message, params);
    }

    /**
     * Print {@code message} with {@code params} to the WARN log.
     * 
     * @param message
     * @param params
     */
    public static void warn(String message, Object... params) {
        WARN.warn(message, params);
    }

    /**
     * Print {@code message} with {@code params} to the DEBUG log.
     * 
     * @param message
     * @param params
     */
    public static void debug(String message, Object... params) {
        DEBUG.debug(message, params);
    }

    private static String MAX_LOG_FILE_SIZE = "10MB";
    private static final String LOG_DIRECTORY = "log";
    private static final ch.qos.logback.classic.Logger ERROR = setup(
            "com.cinchapi.concourse.server.ErrorLogger", "error.log");
    private static final ch.qos.logback.classic.Logger WARN = setup(
            "com.cinchapi.concourse.server.WarnLogger", "warn.log");
    private static final ch.qos.logback.classic.Logger INFO = setup(
            "com.cinchapi.concourse.server.InfoLogger", "info.log");
    private static final ch.qos.logback.classic.Logger DEBUG = setup(
            "com.cinchapi.concourse.server.DebugLogger", "debug.log");
    static {
        // Capture logging from Thrift classes and route it to our error
        // log so we have details on processing failures.
        setup(ProcessFunction.class.getName(), "error.log");
        setup(TThreadPoolServer.class.getName(), "error.log");
    }

    /**
     * Setup logger {@code name} that prints to {@code file}.
     * 
     * @param name
     * @param file
     * @return the logger
     */
    private static ch.qos.logback.classic.Logger setup(String name, String file) {
        if(!GlobalState.ENABLE_CONSOLE_LOGGING) {
            ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger) LoggerFactory
                    .getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME);
            root.detachAndStopAllAppenders();
        }
        LoggerContext context = (LoggerContext) LoggerFactory
                .getILoggerFactory();
        // Configure Pattern
        PatternLayoutEncoder encoder = new PatternLayoutEncoder();
        encoder.setPattern("%date [%thread] %level - %msg%n");
        encoder.setContext(context);
        encoder.start();

        // Create File Appender
        RollingFileAppender<ILoggingEvent> appender = new RollingFileAppender<ILoggingEvent>();
        appender.setFile(LOG_DIRECTORY + File.separator + file);
        appender.setContext(context);

        // Configure Rolling Policy
        FixedWindowRollingPolicy rolling = new FixedWindowRollingPolicy();
        rolling.setMaxIndex(1);
        rolling.setMaxIndex(5);
        rolling.setContext(context);
        rolling.setFileNamePattern(LOG_DIRECTORY + File.separator + file
                + ".%i.zip");
        rolling.setParent(appender);
        rolling.start();

        // Configure Triggering Policy
        SizeBasedTriggeringPolicy<ILoggingEvent> triggering = new SizeBasedTriggeringPolicy<ILoggingEvent>();
        triggering.setMaxFileSize(MAX_LOG_FILE_SIZE);
        triggering.start();

        // Configure File Appender
        appender.setEncoder(encoder);
        appender.setRollingPolicy(rolling);
        appender.setTriggeringPolicy(triggering);
        appender.start();

        // Get Logger
        ch.qos.logback.classic.Logger logger = (ch.qos.logback.classic.Logger) LoggerFactory
                .getLogger(name);
        logger.addAppender(appender);
        logger.setLevel(GlobalState.LOG_LEVEL);
        logger.setAdditive(true);
        return logger;
    }

    /**
     * Update the configuration of {@code logger} based on changes in the
     * underlying prefs file.
     * 
     * @param logger
     */
    @SuppressWarnings("unused")
    private static void update(ch.qos.logback.classic.Logger logger) {
        // TODO I need to actually reload the file from disk and check for
        // changes
        ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger) LoggerFactory
                .getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME);
        if(!GlobalState.ENABLE_CONSOLE_LOGGING) {
            root.detachAndStopAllAppenders();
        }
        else {
            root.addAppender(new ConsoleAppender<ILoggingEvent>());
        }
        logger.setLevel(GlobalState.LOG_LEVEL);
    }

    private Logger() {} /* utility class */

}
