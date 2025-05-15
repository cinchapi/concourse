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
            Serialization stream = new Serialization();
            TObject[] values = object.values();
            stream.writeByte((byte) object.operator().ordinal());
            stream.writeInt(values.length);
            for (TObject value : values) {
                byte[] data = value.getData();
                stream.writeByte((byte) value.getType().ordinal());
                stream.writeInt(data.length);
                stream.writeByteArray(data);
            }
            return stream.bytes();
        }, bytes -> {
            Serialization stream = new Serialization(bytes);
            Operator operator = Operator.values()[stream.readByte()];
            int length = stream.readInt();
            TObject[] values = new TObject[length];
            for (int i = 0; i < length; ++i) {
                Type type = Type.values()[stream.readByte()];
                ByteBuffer data = stream.readByteBuffer(stream.readInt());
                values[i] = new TObject(data, type);
            }
            return Reflection.newInstance(TObject.Aliases.class, operator,
                    values); /* (authorized) */
        });
    }

    private EnsembleSetup() {/* no-init */}

}
