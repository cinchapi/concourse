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
package com.cinchapi.concourse.server;

import java.nio.ByteBuffer;

import com.cinchapi.common.io.ByteBuffers;
import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.thrift.Type;
import com.cinchapi.concourse.util.Logger;
import com.cinchapi.ensemble.LoggingIntegration;
import com.cinchapi.ensemble.io.Serialization;

/**
 * Consolidated factories for configuring the Ensemble distributed system
 * framework.
 *
 * @author Jeff Nelson
 */
public final class EnsembleSetup {

    /**
     * Intercept logging from the Ensemble framework and route it to the native
     * {@link Logger}.
     */
    public static void interceptLogging() {
        com.cinchapi.ensemble.util.Logger
                .setLoggingIntegration(new LoggingIntegration() {

                    @Override
                    public void debug(String message, Object... params) {
                        Logger.debug(message, params);
                    }

                    @Override
                    public void error(String message, Object... params) {
                        Logger.error(message, params);
                    }

                    @Override
                    public void info(String message, Object... params) {
                        Logger.info(message, params);
                    }

                    @Override
                    public void warn(String message, Object... params) {
                        Logger.warn(message, params);
                    }

                });
    }

    /**
     * Register all custom serialization required for Ensemble.
     */
    public static void registerCustomSerialization() {
        Serialization.customize(TObject.Aliases.class, object -> {
            int size = 1 + 4;
            TObject[] values = object.values();
            for (TObject value : values) {
                size += 4 + 1 + value.getData().length;
            }
            ByteBuffer bytes = ByteBuffer.allocate(size);
            bytes.put((byte) object.operator().ordinal());
            bytes.putInt(values.length);
            for (TObject value : values) {
                byte[] data = value.getData();
                bytes.put((byte) value.getType().ordinal());
                bytes.putInt(data.length);
                bytes.put(data);
            }
            bytes.flip();
            return bytes;
        }, bytes -> {
            Operator operator = Operator.values()[bytes.get()];
            int length = bytes.getInt();
            TObject[] values = new TObject[length];
            for (int i = 0; i < length; ++i) {
                Type type = Type.values()[bytes.get()];
                ByteBuffer data = ByteBuffers.get(bytes, bytes.getInt());
                values[i] = new TObject(data, type);
            }
            return Reflection.newInstance(TObject.Aliases.class, operator,
                    values); /* (authorized) */
        });
    }

    private EnsembleSetup() {/* no-init */}

}
