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

import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Generated;

import org.apache.thrift.EncodingUtils;
import org.apache.thrift.protocol.TTupleProtocol;
import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;
import org.apache.thrift.scheme.TupleScheme;
import org.apache.thrift.server.AbstractNonblockingServer.*;

@SuppressWarnings({ "cast", "rawtypes", "serial", "unchecked", "unused" })
/**
 * A token that identifies a Transaction.
 */
@Generated(value = "Autogenerated by Thrift Compiler (0.9.2)", date = "2015-2-22")
public class TransactionToken implements
        org.apache.thrift.TBase<TransactionToken, TransactionToken._Fields>,
        java.io.Serializable,
        Cloneable,
        Comparable<TransactionToken> {
    private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct(
            "TransactionToken");

    private static final org.apache.thrift.protocol.TField ACCESS_TOKEN_FIELD_DESC = new org.apache.thrift.protocol.TField(
            "accessToken", org.apache.thrift.protocol.TType.STRUCT, (short) 1);
    private static final org.apache.thrift.protocol.TField TIMESTAMP_FIELD_DESC = new org.apache.thrift.protocol.TField(
            "timestamp", org.apache.thrift.protocol.TType.I64, (short) 2);

    private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
    static {
        schemes.put(StandardScheme.class,
                new TransactionTokenStandardSchemeFactory());
        schemes.put(TupleScheme.class,
                new TransactionTokenTupleSchemeFactory());
    }

    public AccessToken accessToken; // required
    public long timestamp; // required

    /**
     * The set of fields this struct contains, along with convenience methods
     * for finding and manipulating them.
     */
    public enum _Fields implements org.apache.thrift.TFieldIdEnum {
        ACCESS_TOKEN((short) 1, "accessToken"),
        TIMESTAMP((short) 2, "timestamp");

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
            case 1: // ACCESS_TOKEN
                return ACCESS_TOKEN;
            case 2: // TIMESTAMP
                return TIMESTAMP;
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
    private static final int __TIMESTAMP_ISSET_ID = 0;
    private byte __isset_bitfield = 0;
    public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
    static {
        Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(
                _Fields.class);
        tmpMap.put(_Fields.ACCESS_TOKEN,
                new org.apache.thrift.meta_data.FieldMetaData("accessToken",
                        org.apache.thrift.TFieldRequirementType.REQUIRED,
                        new org.apache.thrift.meta_data.StructMetaData(
                                org.apache.thrift.protocol.TType.STRUCT,
                                AccessToken.class)));
        tmpMap.put(_Fields.TIMESTAMP,
                new org.apache.thrift.meta_data.FieldMetaData("timestamp",
                        org.apache.thrift.TFieldRequirementType.REQUIRED,
                        new org.apache.thrift.meta_data.FieldValueMetaData(
                                org.apache.thrift.protocol.TType.I64)));
        metaDataMap = Collections.unmodifiableMap(tmpMap);
        org.apache.thrift.meta_data.FieldMetaData
                .addStructMetaDataMap(TransactionToken.class, metaDataMap);
    }

    public TransactionToken() {}

    public TransactionToken(AccessToken accessToken, long timestamp) {
        this();
        this.accessToken = accessToken;
        this.timestamp = timestamp;
        setTimestampIsSet(true);
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public TransactionToken(TransactionToken other) {
        __isset_bitfield = other.__isset_bitfield;
        if(other.isSetAccessToken()) {
            this.accessToken = new AccessToken(other.accessToken);
        }
        this.timestamp = other.timestamp;
    }

    public TransactionToken deepCopy() {
        return new TransactionToken(this);
    }

    @Override
    public void clear() {
        this.accessToken = null;
        setTimestampIsSet(false);
        this.timestamp = 0;
    }

    public AccessToken getAccessToken() {
        return this.accessToken;
    }

    public TransactionToken setAccessToken(AccessToken accessToken) {
        this.accessToken = accessToken;
        return this;
    }

    public void unsetAccessToken() {
        this.accessToken = null;
    }

    /**
     * Returns true if field accessToken is set (has been assigned a value) and
     * false otherwise
     */
    public boolean isSetAccessToken() {
        return this.accessToken != null;
    }

    public void setAccessTokenIsSet(boolean value) {
        if(!value) {
            this.accessToken = null;
        }
    }

    public long getTimestamp() {
        return this.timestamp;
    }

    public TransactionToken setTimestamp(long timestamp) {
        this.timestamp = timestamp;
        setTimestampIsSet(true);
        return this;
    }

    public void unsetTimestamp() {
        __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield,
                __TIMESTAMP_ISSET_ID);
    }

    /**
     * Returns true if field timestamp is set (has been assigned a value) and
     * false otherwise
     */
    public boolean isSetTimestamp() {
        return EncodingUtils.testBit(__isset_bitfield, __TIMESTAMP_ISSET_ID);
    }

    public void setTimestampIsSet(boolean value) {
        __isset_bitfield = EncodingUtils.setBit(__isset_bitfield,
                __TIMESTAMP_ISSET_ID, value);
    }

    public void setFieldValue(_Fields field, Object value) {
        switch (field) {
        case ACCESS_TOKEN:
            if(value == null) {
                unsetAccessToken();
            }
            else {
                setAccessToken((AccessToken) value);
            }
            break;

        case TIMESTAMP:
            if(value == null) {
                unsetTimestamp();
            }
            else {
                setTimestamp((Long) value);
            }
            break;

        }
    }

    public Object getFieldValue(_Fields field) {
        switch (field) {
        case ACCESS_TOKEN:
            return getAccessToken();

        case TIMESTAMP:
            return Long.valueOf(getTimestamp());

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
        case ACCESS_TOKEN:
            return isSetAccessToken();
        case TIMESTAMP:
            return isSetTimestamp();
        }
        throw new IllegalStateException();
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof TransactionToken) {
            TransactionToken other = (TransactionToken) obj;
            return accessToken.equals(other.accessToken);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(accessToken);
    }

    @Override
    public int compareTo(TransactionToken other) {
        if(!getClass().equals(other.getClass())) {
            return getClass().getName().compareTo(other.getClass().getName());
        }

        int lastComparison = 0;

        lastComparison = Boolean.valueOf(isSetAccessToken())
                .compareTo(other.isSetAccessToken());
        if(lastComparison != 0) {
            return lastComparison;
        }
        if(isSetAccessToken()) {
            lastComparison = org.apache.thrift.TBaseHelper
                    .compareTo(this.accessToken, other.accessToken);
            if(lastComparison != 0) {
                return lastComparison;
            }
        }
        lastComparison = Boolean.valueOf(isSetTimestamp())
                .compareTo(other.isSetTimestamp());
        if(lastComparison != 0) {
            return lastComparison;
        }
        if(isSetTimestamp()) {
            lastComparison = org.apache.thrift.TBaseHelper
                    .compareTo(this.timestamp, other.timestamp);
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
        StringBuilder sb = new StringBuilder("TransactionToken(");
        boolean first = true;

        sb.append("accessToken:");
        if(this.accessToken == null) {
            sb.append("null");
        }
        else {
            sb.append(this.accessToken);
        }
        first = false;
        if(!first)
            sb.append(", ");
        sb.append("timestamp:");
        sb.append(this.timestamp);
        first = false;
        sb.append(")");
        return sb.toString();
    }

    public void validate() throws org.apache.thrift.TException {
        // check for required fields
        if(accessToken == null) {
            throw new org.apache.thrift.protocol.TProtocolException(
                    "Required field 'accessToken' was not present! Struct: "
                            + toString());
        }
        // alas, we cannot check 'timestamp' because it's a primitive and you
        // chose the non-beans generator.
        // check for sub-struct validity
        if(accessToken != null) {
            accessToken.validate();
        }
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
            // it doesn't seem like you should have to do this, but java
            // serialization is wacky, and doesn't call the default constructor.
            __isset_bitfield = 0;
            read(new org.apache.thrift.protocol.TCompactProtocol(
                    new org.apache.thrift.transport.TIOStreamTransport(in)));
        }
        catch (org.apache.thrift.TException te) {
            throw new java.io.IOException(te);
        }
    }

    private static class TransactionTokenStandardSchemeFactory implements
            SchemeFactory {
        public TransactionTokenStandardScheme getScheme() {
            return new TransactionTokenStandardScheme();
        }
    }

    private static class TransactionTokenStandardScheme
            extends StandardScheme<TransactionToken> {

        public void read(org.apache.thrift.protocol.TProtocol iprot,
                TransactionToken struct) throws org.apache.thrift.TException {
            org.apache.thrift.protocol.TField schemeField;
            iprot.readStructBegin();
            while (true) {
                schemeField = iprot.readFieldBegin();
                if(schemeField.type == org.apache.thrift.protocol.TType.STOP) {
                    break;
                }
                switch (schemeField.id) {
                case 1: // ACCESS_TOKEN
                    if(schemeField.type == org.apache.thrift.protocol.TType.STRUCT) {
                        struct.accessToken = new AccessToken();
                        struct.accessToken.read(iprot);
                        struct.setAccessTokenIsSet(true);
                    }
                    else {
                        org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                                schemeField.type);
                    }
                    break;
                case 2: // TIMESTAMP
                    if(schemeField.type == org.apache.thrift.protocol.TType.I64) {
                        struct.timestamp = iprot.readI64();
                        struct.setTimestampIsSet(true);
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
            if(!struct.isSetTimestamp()) {
                throw new org.apache.thrift.protocol.TProtocolException(
                        "Required field 'timestamp' was not found in serialized data! Struct: "
                                + toString());
            }
            struct.validate();
        }

        public void write(org.apache.thrift.protocol.TProtocol oprot,
                TransactionToken struct) throws org.apache.thrift.TException {
            struct.validate();

            oprot.writeStructBegin(STRUCT_DESC);
            if(struct.accessToken != null) {
                oprot.writeFieldBegin(ACCESS_TOKEN_FIELD_DESC);
                struct.accessToken.write(oprot);
                oprot.writeFieldEnd();
            }
            oprot.writeFieldBegin(TIMESTAMP_FIELD_DESC);
            oprot.writeI64(struct.timestamp);
            oprot.writeFieldEnd();
            oprot.writeFieldStop();
            oprot.writeStructEnd();
        }

    }

    private static class TransactionTokenTupleSchemeFactory implements
            SchemeFactory {
        public TransactionTokenTupleScheme getScheme() {
            return new TransactionTokenTupleScheme();
        }
    }

    private static class TransactionTokenTupleScheme
            extends TupleScheme<TransactionToken> {

        @Override
        public void write(org.apache.thrift.protocol.TProtocol prot,
                TransactionToken struct) throws org.apache.thrift.TException {
            TTupleProtocol oprot = (TTupleProtocol) prot;
            struct.accessToken.write(oprot);
            oprot.writeI64(struct.timestamp);
        }

        @Override
        public void read(org.apache.thrift.protocol.TProtocol prot,
                TransactionToken struct) throws org.apache.thrift.TException {
            TTupleProtocol iprot = (TTupleProtocol) prot;
            struct.accessToken = new AccessToken();
            struct.accessToken.read(iprot);
            struct.setAccessTokenIsSet(true);
            struct.timestamp = iprot.readI64();
            struct.setTimestampIsSet(true);
        }
    }

}
