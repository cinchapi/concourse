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
package com.cinchapi.concourse.thrift;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Generated;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TTupleProtocol;
import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;
import org.apache.thrift.scheme.TupleScheme;
import org.apache.thrift.server.AbstractNonblockingServer.*;

@SuppressWarnings({ "cast", "rawtypes", "serial", "unchecked", "unused" })
/**
 * Signals that an unexpected or invalid token was reached while parsing.
 */
@Generated(value = "Autogenerated by Thrift Compiler (0.9.2)", date = "2015-9-30")
public class ParseException extends TException implements
        org.apache.thrift.TBase<ParseException, ParseException._Fields>,
        java.io.Serializable,
        Cloneable,
        Comparable<ParseException> {
    private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct(
            "ParseException");

    private static final org.apache.thrift.protocol.TField MESSAGE_FIELD_DESC = new org.apache.thrift.protocol.TField(
            "message", org.apache.thrift.protocol.TType.STRING, (short) 1);

    private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
    static {
        schemes.put(StandardScheme.class,
                new ParseExceptionStandardSchemeFactory());
        schemes.put(TupleScheme.class, new ParseExceptionTupleSchemeFactory());
    }

    public String message; // required

    /**
     * The set of fields this struct contains, along with convenience methods
     * for finding and manipulating them.
     */
    public enum _Fields implements org.apache.thrift.TFieldIdEnum {
        MESSAGE((short) 1, "message");

        private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

        static {
            for (_Fields field : EnumSet.allOf(_Fields.class)) {
                byName.put(field.getFieldName(), field);
            }
        }

        /**
         * Find the _Fields constant that matches fieldId, or null if its not
         * found.
         */
        public static _Fields findByThriftId(int fieldId) {
            switch (fieldId) {
            case 1: // MESSAGE
                return MESSAGE;
            default:
                return null;
            }
        }

        /**
         * Find the _Fields constant that matches fieldId, throwing an exception
         * if it is not found.
         */
        public static _Fields findByThriftIdOrThrow(int fieldId) {
            _Fields fields = findByThriftId(fieldId);
            if(fields == null)
                throw new IllegalArgumentException(
                        "Field " + fieldId + " doesn't exist!");
            return fields;
        }

        /**
         * Find the _Fields constant that matches name, or null if its not
         * found.
         */
        public static _Fields findByName(String name) {
            return byName.get(name);
        }

        private final short _thriftId;
        private final String _fieldName;

        _Fields(short thriftId, String fieldName) {
            _thriftId = thriftId;
            _fieldName = fieldName;
        }

        public short getThriftFieldId() {
            return _thriftId;
        }

        public String getFieldName() {
            return _fieldName;
        }
    }

    // isset id assignments
    public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
    static {
        Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(
                _Fields.class);
        tmpMap.put(_Fields.MESSAGE,
                new org.apache.thrift.meta_data.FieldMetaData("message",
                        org.apache.thrift.TFieldRequirementType.DEFAULT,
                        new org.apache.thrift.meta_data.FieldValueMetaData(
                                org.apache.thrift.protocol.TType.STRING)));
        metaDataMap = Collections.unmodifiableMap(tmpMap);
        org.apache.thrift.meta_data.FieldMetaData
                .addStructMetaDataMap(ParseException.class, metaDataMap);
    }

    public ParseException() {}

    public ParseException(String message) {
        this();
        this.message = message;
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public ParseException(ParseException other) {
        if(other.isSetMessage()) {
            this.message = other.message;
        }
    }

    public ParseException deepCopy() {
        return new ParseException(this);
    }

    @Override
    public void clear() {
        this.message = null;
    }

    public String getMessage() {
        return this.message;
    }

    public ParseException setMessage(String message) {
        this.message = message;
        return this;
    }

    public void unsetMessage() {
        this.message = null;
    }

    /**
     * Returns true if field message is set (has been assigned a value) and
     * false otherwise
     */
    public boolean isSetMessage() {
        return this.message != null;
    }

    public void setMessageIsSet(boolean value) {
        if(!value) {
            this.message = null;
        }
    }

    public void setFieldValue(_Fields field, Object value) {
        switch (field) {
        case MESSAGE:
            if(value == null) {
                unsetMessage();
            }
            else {
                setMessage((String) value);
            }
            break;

        }
    }

    public Object getFieldValue(_Fields field) {
        switch (field) {
        case MESSAGE:
            return getMessage();

        }
        throw new IllegalStateException();
    }

    /**
     * Returns true if field corresponding to fieldID is set (has been assigned
     * a value) and false otherwise
     */
    public boolean isSet(_Fields field) {
        if(field == null) {
            throw new IllegalArgumentException();
        }

        switch (field) {
        case MESSAGE:
            return isSetMessage();
        }
        throw new IllegalStateException();
    }

    @Override
    public boolean equals(Object that) {
        if(that == null)
            return false;
        if(that instanceof ParseException)
            return this.equals((ParseException) that);
        return false;
    }

    public boolean equals(ParseException that) {
        if(that == null)
            return false;

        boolean this_present_message = true && this.isSetMessage();
        boolean that_present_message = true && that.isSetMessage();
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
    public int compareTo(ParseException other) {
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
            lastComparison = org.apache.thrift.TBaseHelper
                    .compareTo(this.message, other.message);
            if(lastComparison != 0) {
                return lastComparison;
            }
        }
        return 0;
    }

    public _Fields fieldForId(int fieldId) {
        return _Fields.findByThriftId(fieldId);
    }

    public void read(org.apache.thrift.protocol.TProtocol iprot)
            throws org.apache.thrift.TException {
        schemes.get(iprot.getScheme()).getScheme().read(iprot, this);
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot)
            throws org.apache.thrift.TException {
        schemes.get(oprot.getScheme()).getScheme().write(oprot, this);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("ParseException(");
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

    public void validate() throws org.apache.thrift.TException {
        // check for required fields
        // check for sub-struct validity
    }

    private void writeObject(java.io.ObjectOutputStream out)
            throws java.io.IOException {
        try {
            write(new org.apache.thrift.protocol.TCompactProtocol(
                    new org.apache.thrift.transport.TIOStreamTransport(out)));
        }
        catch (org.apache.thrift.TException te) {
            throw new java.io.IOException(te);
        }
    }

    private void readObject(java.io.ObjectInputStream in)
            throws java.io.IOException, ClassNotFoundException {
        try {
            read(new org.apache.thrift.protocol.TCompactProtocol(
                    new org.apache.thrift.transport.TIOStreamTransport(in)));
        }
        catch (org.apache.thrift.TException te) {
            throw new java.io.IOException(te);
        }
    }

    private static class ParseExceptionStandardSchemeFactory implements
            SchemeFactory {
        public ParseExceptionStandardScheme getScheme() {
            return new ParseExceptionStandardScheme();
        }
    }

    private static class ParseExceptionStandardScheme
            extends StandardScheme<ParseException> {

        public void read(org.apache.thrift.protocol.TProtocol iprot,
                ParseException struct) throws org.apache.thrift.TException {
            org.apache.thrift.protocol.TField schemeField;
            iprot.readStructBegin();
            while (true) {
                schemeField = iprot.readFieldBegin();
                if(schemeField.type == org.apache.thrift.protocol.TType.STOP) {
                    break;
                }
                switch (schemeField.id) {
                case 1: // MESSAGE
                    if(schemeField.type == org.apache.thrift.protocol.TType.STRING) {
                        struct.message = iprot.readString();
                        struct.setMessageIsSet(true);
                    }
                    else {
                        org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                                schemeField.type);
                    }
                    break;
                default:
                    org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                            schemeField.type);
                }
                iprot.readFieldEnd();
            }
            iprot.readStructEnd();

            // check for required fields of primitive type, which can't be
            // checked in the validate method
            struct.validate();
        }

        public void write(org.apache.thrift.protocol.TProtocol oprot,
                ParseException struct) throws org.apache.thrift.TException {
            struct.validate();

            oprot.writeStructBegin(STRUCT_DESC);
            if(struct.message != null) {
                oprot.writeFieldBegin(MESSAGE_FIELD_DESC);
                oprot.writeString(struct.message);
                oprot.writeFieldEnd();
            }
            oprot.writeFieldStop();
            oprot.writeStructEnd();
        }

    }

    private static class ParseExceptionTupleSchemeFactory implements
            SchemeFactory {
        public ParseExceptionTupleScheme getScheme() {
            return new ParseExceptionTupleScheme();
        }
    }

    private static class ParseExceptionTupleScheme
            extends TupleScheme<ParseException> {

        @Override
        public void write(org.apache.thrift.protocol.TProtocol prot,
                ParseException struct) throws org.apache.thrift.TException {
            TTupleProtocol oprot = (TTupleProtocol) prot;
            BitSet optionals = new BitSet();
            if(struct.isSetMessage()) {
                optionals.set(0);
            }
            oprot.writeBitSet(optionals, 1);
            if(struct.isSetMessage()) {
                oprot.writeString(struct.message);
            }
        }

        @Override
        public void read(org.apache.thrift.protocol.TProtocol prot,
                ParseException struct) throws org.apache.thrift.TException {
            TTupleProtocol iprot = (TTupleProtocol) prot;
            BitSet incoming = iprot.readBitSet(1);
            if(incoming.get(0)) {
                struct.message = iprot.readString();
                struct.setMessageIsSet(true);
            }
        }
    }

}
