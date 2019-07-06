package com.cinchapi.concourse.thrift.exceptions;

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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import com.cinchapi.concourse.thrift.exceptions.messages.InvalidArgumentException;
import org.apache.thrift.*;
import org.apache.thrift.meta_data.FieldMetaData;
import org.apache.thrift.meta_data.FieldValueMetaData;
import org.apache.thrift.protocol.*;
import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;
import org.apache.thrift.scheme.TupleScheme;
import org.apache.thrift.transport.TIOStreamTransport;

import static org.apache.thrift.protocol.TProtocolUtil.skip;

@SuppressWarnings({ "cast", "rawtypes", "serial", "unchecked", "unused" })

public class GenericMessageException extends TException implements
    TBase<GenericMessageException, GenericMessageException.Fields>,
    Serializable, Cloneable, Comparable<GenericMessageException> {

    private static final TStruct STRUCT_DESC = new TStruct("GenericMessageException");
    private static final TField MESSAGE_FIELD_DESC = new TField("message", TType.STRING, (short) 1);
    private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<>();

    static {
        schemes.put(StandardScheme.class, new GenericMessageExceptionStandardSchemeFactory());
        schemes.put(TupleScheme.class, new GenericMessageExceptionTupleSchemeFactory());
    }

    public String message;

    public enum Fields implements TFieldIdEnum {
        MESSAGE((short) 1, "message");

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
            return _findByThriftId(fieldId, () -> null);
        }

        public static Fields findByThriftIdOrThrow(int fieldId) {
            return _findByThriftId(fieldId, () -> {
                throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
            });
        }

        private static Fields _findByThriftId(int fieldId, Supplier<Fields> failure) {
            if (fieldId == 1)
                return MESSAGE;
            else
                return failure.get();
        }

        public static Fields findByName(String name) {
            return byName.get(name);
        }

        public short getThriftFieldId() { return thriftId; }
        public String getFieldName() { return fieldName; }
    }

    // isset id assignments
    public static final Map<Fields, FieldMetaData> metaDataMap;
    static {
        Map<Fields, FieldMetaData> tmpMap = new EnumMap<>(Fields.class);
        tmpMap.put(Fields.MESSAGE, new FieldMetaData(
            "message", TFieldRequirementType.DEFAULT, new FieldValueMetaData(TType.STRING)
        ));
        metaDataMap = Collections.unmodifiableMap(tmpMap);
        FieldMetaData.addStructMetaDataMap(GenericMessageException.class, metaDataMap);
    }

    public GenericMessageException() {}

    public GenericMessageException(String message) {
        this();
        this.message = message;
    }

    public GenericMessageException(GenericMessageException other) {
        if(other.isSetMessage()) {
            this.message = other.message;
        }
    }

    public GenericMessageException deepCopy() {
        return new GenericMessageException(this);
    }

    public void clear() {
        this.message = null;
    }

    public String getMessage() {
        return this.message;
    }

    public GenericMessageException setMessage(String message) {
        this.message = message;
        return this;
    }

    public void unsetMessage() {
        this.message = null;
    }

    public boolean isSetMessage() {
        return this.message != null;
    }

    public void setMessageIsSet(boolean value) {
        if(!value)
            this.message = null;
    }

    public void setFieldValue(Fields field, Object value) {
        if (field == Fields.MESSAGE) {
            if (value == null || !(value instanceof String))
                unsetMessage();
            else
                setMessage((String) value);
        }
    }

    public Object getFieldValue(Fields field) {
        if (field == Fields.MESSAGE)
            return getMessage();
        throw new IllegalStateException();
    }

    public boolean isSet(Fields field) {
        if (field == null)
            throw new IllegalArgumentException();
        else if (field == Fields.MESSAGE)
            return isSetMessage();
        else
            throw new IllegalStateException();
    }

    public boolean equals(Object other) {
        if (other instanceof GenericMessageException)
            return this.equals((GenericMessageException) other);
        return false;
    }

    public boolean equals(GenericMessageException that) {
        if(that == null)
            return false;

        boolean this_present_message = this.isSetMessage();
        boolean that_present_message = that.isSetMessage();
        if(this_present_message || that_present_message) {
            if(!(this_present_message && that_present_message))
                return false;
            if(!this.message.equals(that.message))
                return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        List<Object> list = new ArrayList<Object>();

        boolean present_message = true && (isSetMessage());
        list.add(present_message);
        if(present_message)
            list.add(message);

        return list.hashCode();
    }

    @Override
    public int compareTo(GenericMessageException other) {
        if(!getClass().equals(other.getClass())) {
            return getClass().getName().compareTo(other.getClass().getName());
        }

        int lastComparison = 0;

        lastComparison = Boolean.valueOf(isSetMessage())
            .compareTo(other.isSetMessage());
        if(lastComparison != 0) {
            return lastComparison;
        }
        if(isSetMessage()) {
            lastComparison = TBaseHelper.compareTo(this.message, other.message);
            if(lastComparison != 0) {
                return lastComparison;
            }
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
        StringBuilder sb = new StringBuilder(getClass().getSimpleName() + "(");
        boolean first = true;

        sb.append("message:");
        if(this.message == null) {
            sb.append("null");
        }
        else {
            sb.append(this.message);
        }
        first = false;
        sb.append(")");
        return sb.toString();
    }

    public void validate() throws TException {
        if (message == null)
            throw new InvalidArgumentException();
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        try {
            write(new TCompactProtocol(new TIOStreamTransport(out)));
        }
        catch (TException te) {
            throw new IOException(te);
        }
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        try {
            read(new TCompactProtocol(new TIOStreamTransport(in)));
        }
        catch (TException te) {
            throw new java.io.IOException(te);
        }
    }

    private static class GenericMessageExceptionStandardSchemeFactory implements SchemeFactory {
        public GenericMessageExceptionStandardScheme getScheme() {
            return new GenericMessageExceptionStandardScheme();
        }
    }

    private static class GenericMessageExceptionStandardScheme extends StandardScheme<GenericMessageException> {
        public void read(TProtocol iprot, GenericMessageException struct) throws TException {
            TField schemeField;
            iprot.readStructBegin();
            while (true) {
                schemeField = iprot.readFieldBegin();
                if (schemeField.type == TType.STOP)
                    break;
                else if (schemeField.id == 1 && schemeField.type == TType.STRING) {
                    struct.message = iprot.readString();
                    struct.setMessageIsSet(true);
                }
                else
                    skip(iprot, schemeField.type);

                iprot.readFieldEnd();
            }
            iprot.readStructEnd();

            // TODO: check for required fields of primitive type, which can't be checked in validate
            struct.validate();
        }

        public void write(TProtocol oprot, GenericMessageException struct) throws TException {
            struct.validate();
            oprot.writeStructBegin(STRUCT_DESC);

            if (struct.message != null) {
                oprot.writeFieldBegin(MESSAGE_FIELD_DESC);
                oprot.writeString(struct.message);
                oprot.writeFieldEnd();
            }

            oprot.writeFieldStop();
            oprot.writeStructEnd();
        }

    }

    private static class GenericMessageExceptionTupleSchemeFactory implements SchemeFactory {
        public GenericMessageExceptionTupleScheme getScheme() {
            return new GenericMessageExceptionTupleScheme();
        }
    }

    private static class GenericMessageExceptionTupleScheme extends TupleScheme<GenericMessageException> {
        public void write(TProtocol prot, GenericMessageException struct) throws TException {
            TTupleProtocol oprot = (TTupleProtocol) prot;
            BitSet optionals = new BitSet();
            if (struct.isSetMessage())
                optionals.set(0);
            oprot.writeBitSet(optionals, 1);
            if (struct.isSetMessage())
                oprot.writeString(struct.message);
        }

        public void read(TProtocol prot, GenericMessageException struct) throws TException {
            TTupleProtocol iprot = (TTupleProtocol) prot;
            BitSet incoming = iprot.readBitSet(1);
            if (incoming.get(0)) {
                struct.message = iprot.readString();
                struct.setMessageIsSet(true);
            }
        }
    }
}

