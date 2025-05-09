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
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
 * Encapsulation for a skip/limit parameters that make up a page of data.
 */
@Generated(value = "Autogenerated by Thrift Compiler (0.9.3)", date = "2019-07-02")
public class TPage implements
        org.apache.thrift.TBase<TPage, TPage._Fields>,
        java.io.Serializable,
        Cloneable,
        Comparable<TPage> {
    private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct(
            "TPage");

    private static final org.apache.thrift.protocol.TField SKIP_FIELD_DESC = new org.apache.thrift.protocol.TField(
            "skip", org.apache.thrift.protocol.TType.I32, (short) 1);
    private static final org.apache.thrift.protocol.TField LIMIT_FIELD_DESC = new org.apache.thrift.protocol.TField(
            "limit", org.apache.thrift.protocol.TType.I32, (short) 2);

    private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
    static {
        schemes.put(StandardScheme.class, new TPageStandardSchemeFactory());
        schemes.put(TupleScheme.class, new TPageTupleSchemeFactory());
    }

    public int skip; // required
    public int limit; // required

    /**
     * The set of fields this struct contains, along with convenience methods
     * for finding and manipulating them.
     */
    public enum _Fields implements org.apache.thrift.TFieldIdEnum {
        SKIP((short) 1, "skip"), LIMIT((short) 2, "limit");

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
            case 1: // SKIP
                return SKIP;
            case 2: // LIMIT
                return LIMIT;
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
    private static final int __SKIP_ISSET_ID = 0;
    private static final int __LIMIT_ISSET_ID = 1;
    private byte __isset_bitfield = 0;
    public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
    static {
        Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(
                _Fields.class);
        tmpMap.put(_Fields.SKIP,
                new org.apache.thrift.meta_data.FieldMetaData("skip",
                        org.apache.thrift.TFieldRequirementType.REQUIRED,
                        new org.apache.thrift.meta_data.FieldValueMetaData(
                                org.apache.thrift.protocol.TType.I32)));
        tmpMap.put(_Fields.LIMIT,
                new org.apache.thrift.meta_data.FieldMetaData("limit",
                        org.apache.thrift.TFieldRequirementType.REQUIRED,
                        new org.apache.thrift.meta_data.FieldValueMetaData(
                                org.apache.thrift.protocol.TType.I32)));
        metaDataMap = Collections.unmodifiableMap(tmpMap);
        org.apache.thrift.meta_data.FieldMetaData
                .addStructMetaDataMap(TPage.class, metaDataMap);
    }

    public TPage() {}

    public TPage(int skip, int limit) {
        this();
        this.skip = skip;
        setSkipIsSet(true);
        this.limit = limit;
        setLimitIsSet(true);
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public TPage(TPage other) {
        __isset_bitfield = other.__isset_bitfield;
        this.skip = other.skip;
        this.limit = other.limit;
    }

    public TPage deepCopy() {
        return new TPage(this);
    }

    @Override
    public void clear() {
        setSkipIsSet(false);
        this.skip = 0;
        setLimitIsSet(false);
        this.limit = 0;
    }

    public int getSkip() {
        return this.skip;
    }

    public TPage setSkip(int skip) {
        this.skip = skip;
        setSkipIsSet(true);
        return this;
    }

    public void unsetSkip() {
        __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield,
                __SKIP_ISSET_ID);
    }

    /**
     * Returns true if field skip is set (has been assigned a value) and false
     * otherwise
     */
    public boolean isSetSkip() {
        return EncodingUtils.testBit(__isset_bitfield, __SKIP_ISSET_ID);
    }

    public void setSkipIsSet(boolean value) {
        __isset_bitfield = EncodingUtils.setBit(__isset_bitfield,
                __SKIP_ISSET_ID, value);
    }

    public int getLimit() {
        return this.limit;
    }

    public TPage setLimit(int limit) {
        this.limit = limit;
        setLimitIsSet(true);
        return this;
    }

    public void unsetLimit() {
        __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield,
                __LIMIT_ISSET_ID);
    }

    /**
     * Returns true if field limit is set (has been assigned a value) and false
     * otherwise
     */
    public boolean isSetLimit() {
        return EncodingUtils.testBit(__isset_bitfield, __LIMIT_ISSET_ID);
    }

    public void setLimitIsSet(boolean value) {
        __isset_bitfield = EncodingUtils.setBit(__isset_bitfield,
                __LIMIT_ISSET_ID, value);
    }

    public void setFieldValue(_Fields field, Object value) {
        switch (field) {
        case SKIP:
            if(value == null) {
                unsetSkip();
            }
            else {
                setSkip((Integer) value);
            }
            break;

        case LIMIT:
            if(value == null) {
                unsetLimit();
            }
            else {
                setLimit((Integer) value);
            }
            break;

        }
    }

    public Object getFieldValue(_Fields field) {
        switch (field) {
        case SKIP:
            return getSkip();

        case LIMIT:
            return getLimit();

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
        case SKIP:
            return isSetSkip();
        case LIMIT:
            return isSetLimit();
        }
        throw new IllegalStateException();
    }

    @Override
    public boolean equals(Object that) {
        if(that == null)
            return false;
        if(that instanceof TPage)
            return this.equals((TPage) that);
        return false;
    }

    public boolean equals(TPage that) {
        if(that == null)
            return false;

        boolean this_present_skip = true;
        boolean that_present_skip = true;
        if(this_present_skip || that_present_skip) {
            if(!(this_present_skip && that_present_skip))
                return false;
            if(this.skip != that.skip)
                return false;
        }

        boolean this_present_limit = true;
        boolean that_present_limit = true;
        if(this_present_limit || that_present_limit) {
            if(!(this_present_limit && that_present_limit))
                return false;
            if(this.limit != that.limit)
                return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        List<Object> list = new ArrayList<Object>();

        boolean present_skip = true;
        list.add(present_skip);
        if(present_skip)
            list.add(skip);

        boolean present_limit = true;
        list.add(present_limit);
        if(present_limit)
            list.add(limit);

        return list.hashCode();
    }

    @Override
    public int compareTo(TPage other) {
        if(!getClass().equals(other.getClass())) {
            return getClass().getName().compareTo(other.getClass().getName());
        }

        int lastComparison = 0;

        lastComparison = Boolean.valueOf(isSetSkip())
                .compareTo(other.isSetSkip());
        if(lastComparison != 0) {
            return lastComparison;
        }
        if(isSetSkip()) {
            lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.skip,
                    other.skip);
            if(lastComparison != 0) {
                return lastComparison;
            }
        }
        lastComparison = Boolean.valueOf(isSetLimit())
                .compareTo(other.isSetLimit());
        if(lastComparison != 0) {
            return lastComparison;
        }
        if(isSetLimit()) {
            lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.limit,
                    other.limit);
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
        StringBuilder sb = new StringBuilder("TPage(");
        boolean first = true;

        sb.append("skip:");
        sb.append(this.skip);
        first = false;
        if(!first)
            sb.append(", ");
        sb.append("limit:");
        sb.append(this.limit);
        first = false;
        sb.append(")");
        return sb.toString();
    }

    public void validate() throws org.apache.thrift.TException {
        // check for required fields
        // alas, we cannot check 'skip' because it's a primitive and you chose
        // the non-beans generator.
        // alas, we cannot check 'limit' because it's a primitive and you chose
        // the non-beans generator.
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

    private static class TPageStandardSchemeFactory implements SchemeFactory {
        public TPageStandardScheme getScheme() {
            return new TPageStandardScheme();
        }
    }

    private static class TPageStandardScheme extends StandardScheme<TPage> {

        public void read(org.apache.thrift.protocol.TProtocol iprot,
                TPage struct) throws org.apache.thrift.TException {
            org.apache.thrift.protocol.TField schemeField;
            iprot.readStructBegin();
            while (true) {
                schemeField = iprot.readFieldBegin();
                if(schemeField.type == org.apache.thrift.protocol.TType.STOP) {
                    break;
                }
                switch (schemeField.id) {
                case 1: // SKIP
                    if(schemeField.type == org.apache.thrift.protocol.TType.I32) {
                        struct.skip = iprot.readI32();
                        struct.setSkipIsSet(true);
                    }
                    else {
                        org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                                schemeField.type);
                    }
                    break;
                case 2: // LIMIT
                    if(schemeField.type == org.apache.thrift.protocol.TType.I32) {
                        struct.limit = iprot.readI32();
                        struct.setLimitIsSet(true);
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
            if(!struct.isSetSkip()) {
                throw new org.apache.thrift.protocol.TProtocolException(
                        "Required field 'skip' was not found in serialized data! Struct: "
                                + toString());
            }
            if(!struct.isSetLimit()) {
                throw new org.apache.thrift.protocol.TProtocolException(
                        "Required field 'limit' was not found in serialized data! Struct: "
                                + toString());
            }
            struct.validate();
        }

        public void write(org.apache.thrift.protocol.TProtocol oprot,
                TPage struct) throws org.apache.thrift.TException {
            struct.validate();

            oprot.writeStructBegin(STRUCT_DESC);
            oprot.writeFieldBegin(SKIP_FIELD_DESC);
            oprot.writeI32(struct.skip);
            oprot.writeFieldEnd();
            oprot.writeFieldBegin(LIMIT_FIELD_DESC);
            oprot.writeI32(struct.limit);
            oprot.writeFieldEnd();
            oprot.writeFieldStop();
            oprot.writeStructEnd();
        }

    }

    private static class TPageTupleSchemeFactory implements SchemeFactory {
        public TPageTupleScheme getScheme() {
            return new TPageTupleScheme();
        }
    }

    private static class TPageTupleScheme extends TupleScheme<TPage> {

        @Override
        public void write(org.apache.thrift.protocol.TProtocol prot,
                TPage struct) throws org.apache.thrift.TException {
            TTupleProtocol oprot = (TTupleProtocol) prot;
            oprot.writeI32(struct.skip);
            oprot.writeI32(struct.limit);
        }

        @Override
        public void read(org.apache.thrift.protocol.TProtocol prot,
                TPage struct) throws org.apache.thrift.TException {
            TTupleProtocol iprot = (TTupleProtocol) prot;
            struct.skip = iprot.readI32();
            struct.setSkipIsSet(true);
            struct.limit = iprot.readI32();
            struct.setLimitIsSet(true);
        }
    }

}
