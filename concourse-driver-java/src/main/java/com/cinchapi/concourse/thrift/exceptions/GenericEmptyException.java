/*
 * Copyright (c) 2013-2019 Cinchapi Inc.
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
package com.cinchapi.concourse.thrift.exceptions;

import static org.apache.thrift.protocol.TProtocolUtil.skip;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

import org.apache.thrift.*;
import org.apache.thrift.meta_data.FieldMetaData;
import org.apache.thrift.protocol.*;
import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;
import org.apache.thrift.scheme.TupleScheme;
import org.apache.thrift.transport.TIOStreamTransport;

@SuppressWarnings({ "cast", "rawtypes", "serial", "unchecked", "unused" })

public class GenericEmptyException extends TException implements
        TBase<GenericEmptyException, GenericEmptyException.Fields>,
        Serializable,
        Cloneable,
        Comparable<GenericEmptyException> {

    private static final TStruct STRUCT_DESC = new TStruct(
            "GenericEmptyException");
    private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<>();

    static {
        schemes.put(StandardScheme.class,
                new GenericEmptyExceptionStandardSchemeFactory());
        schemes.put(TupleScheme.class,
                new GenericEmptyExceptionTupleSchemeFactory());
    }

    public enum Fields implements TFieldIdEnum {
        ;
        private final short thriftId;
        private final String fieldName;

        private static final Map<String, Fields> byName = new HashMap<>();

        Fields(short thriftId, String fieldName) {
            this.thriftId = thriftId;
            this.fieldName = fieldName;
        }

        static {
            for (Fields fields : EnumSet.allOf(Fields.class))
                byName.put(fields.getFieldName(), fields);
        }

        public static Fields findByThriftId(int fieldId) {
            return null;
        }

        public static Fields findByThriftIdOrThrow(int fieldId) {
            throw new IllegalArgumentException(
                    "Field " + fieldId + " doesn't exist!");
        }

        public static Fields findByName(String name) {
            return byName.get(name);
        }

        public short getThriftFieldId() {
            return thriftId;
        }

        public String getFieldName() {
            return fieldName;
        }
    }

    public static final Map<Fields, FieldMetaData> metaDataMap;
    static {
        metaDataMap = Collections.unmodifiableMap(new EnumMap<>(Fields.class));
        FieldMetaData.addStructMetaDataMap(GenericEmptyException.class,
                metaDataMap);
    }

    public GenericEmptyException() {}

    public GenericEmptyException(GenericEmptyException other) {}

    public GenericEmptyException deepCopy() {
        return new GenericEmptyException(this);
    }

    public void clear() {
        // TODO: Check if can be removed.
    }

    public void setFieldValue(Fields field, Object value) {
        // TODO: Check if can be removed.
    }

    public Object getFieldValue(Fields field) {
        throw new IllegalStateException();
    }

    public boolean isSet(Fields field) {
        if(field == null)
            throw new IllegalArgumentException();
        else
            throw new IllegalStateException();
    }

    public boolean equals(Object other) {
        return other instanceof GenericEmptyException;
    }

    public boolean equals(GenericEmptyException that) {
        return true;
    }

    @Override
    public int hashCode() {
        return new ArrayList<>().hashCode();
    }

    @Override
    public int compareTo(GenericEmptyException other) {
        if(!getClass().equals(other.getClass())) {
            return getClass().getName().compareTo(other.getClass().getName());
        }
        return 0;
    }

    public Fields fieldForId(int fieldId) {
        return Fields.findByThriftId(fieldId);
    }

    public void read(TProtocol iprot) throws TException {
        schemes.get(iprot.getScheme()).getScheme().read(iprot, this);
    }

    public void write(TProtocol oprot) throws TException {
        schemes.get(oprot.getScheme()).getScheme().write(oprot, this);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "()";
    }

    public void validate() {}

    private void writeObject(ObjectOutputStream out) throws IOException {
        try {
            write(new TCompactProtocol(new TIOStreamTransport(out)));
        }
        catch (TException te) {
            throw new IOException(te);
        }
    }

    private void readObject(ObjectInputStream in)
            throws IOException, ClassNotFoundException {
        try {
            read(new TCompactProtocol(new TIOStreamTransport(in)));
        }
        catch (TException te) {
            throw new java.io.IOException(te);
        }
    }

    private static class GenericEmptyExceptionStandardSchemeFactory
            implements SchemeFactory {
        public GenericEmptyExceptionStandardScheme getScheme() {
            return new GenericEmptyExceptionStandardScheme();
        }
    }

    private static class GenericEmptyExceptionStandardScheme
            extends StandardScheme<GenericEmptyException> {
        public void read(TProtocol iprot, GenericEmptyException struct)
                throws TException {
            TField schemeField;
            iprot.readStructBegin();
            while (true) {
                schemeField = iprot.readFieldBegin();
                if(schemeField.type == TType.STOP)
                    break;
                else
                    skip(iprot, schemeField.type);

                iprot.readFieldEnd();
            }
            iprot.readStructEnd();

            // TODO: check for required fields of primitive type, which can't be
            // checked in validate
            struct.validate();
        }

        public void write(TProtocol oprot, GenericEmptyException struct)
                throws TException {
            struct.validate();
            oprot.writeStructBegin(STRUCT_DESC);
            oprot.writeFieldStop();
            oprot.writeStructEnd();
        }

    }

    private static class GenericEmptyExceptionTupleSchemeFactory
            implements SchemeFactory {
        public GenericEmptyExceptionTupleScheme getScheme() {
            return new GenericEmptyExceptionTupleScheme();
        }
    }

    private static class GenericEmptyExceptionTupleScheme
            extends TupleScheme<GenericEmptyException> {
        public void write(TProtocol prot, GenericEmptyException struct)
                throws TException {
            TTupleProtocol oprot = (TTupleProtocol) prot;
        }

        public void read(TProtocol prot, GenericEmptyException struct)
                throws TException {
            TTupleProtocol iprot = (TTupleProtocol) prot;
        }
    }
}
