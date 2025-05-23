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

import org.apache.thrift.protocol.TTupleProtocol;
import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;
import org.apache.thrift.scheme.TupleScheme;
import org.apache.thrift.server.AbstractNonblockingServer.*;

@SuppressWarnings({ "cast", "rawtypes", "serial", "unchecked", "unused" })
/**
 * Encapsulation for a group of {@link TOrderComponent order components}
 * that describe how a result set should be sorted.
 */
@Generated(value = "Autogenerated by Thrift Compiler (0.9.3)", date = "2019-05-29")
public class TOrder implements
        org.apache.thrift.TBase<TOrder, TOrder._Fields>,
        java.io.Serializable,
        Cloneable,
        Comparable<TOrder> {

    private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct(
            "TOrder");

    private static final org.apache.thrift.protocol.TField SPEC_FIELD_DESC = new org.apache.thrift.protocol.TField(
            "spec", org.apache.thrift.protocol.TType.LIST, (short) 1);

    private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
    static {
        schemes.put(StandardScheme.class, new TOrderStandardSchemeFactory());
        schemes.put(TupleScheme.class, new TOrderTupleSchemeFactory());
    }

    public List<TOrderComponent> spec; // required

    /**
     * The set of fields this struct contains, along with convenience methods
     * for finding and manipulating them.
     */
    public enum _Fields implements org.apache.thrift.TFieldIdEnum {
        SPEC((short) 1, "spec");

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
            case 1: // SPEC
                return SPEC;
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
        tmpMap.put(_Fields.SPEC,
                new org.apache.thrift.meta_data.FieldMetaData("spec",
                        org.apache.thrift.TFieldRequirementType.REQUIRED,
                        new org.apache.thrift.meta_data.ListMetaData(
                                org.apache.thrift.protocol.TType.LIST,
                                new org.apache.thrift.meta_data.StructMetaData(
                                        org.apache.thrift.protocol.TType.STRUCT,
                                        TOrderComponent.class))));
        metaDataMap = Collections.unmodifiableMap(tmpMap);
        org.apache.thrift.meta_data.FieldMetaData
                .addStructMetaDataMap(TOrder.class, metaDataMap);
    }

    public TOrder() {}

    public TOrder(List<TOrderComponent> spec) {
        this();
        this.spec = spec;
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public TOrder(TOrder other) {
        if(other.isSetSpec()) {
            List<TOrderComponent> __this__spec = new ArrayList<TOrderComponent>(
                    other.spec.size());
            for (TOrderComponent other_element : other.spec) {
                __this__spec.add(new TOrderComponent(other_element));
            }
            this.spec = __this__spec;
        }
    }

    public TOrder deepCopy() {
        return new TOrder(this);
    }

    @Override
    public void clear() {
        this.spec = null;
    }

    public int getSpecSize() {
        return (this.spec == null) ? 0 : this.spec.size();
    }

    public java.util.Iterator<TOrderComponent> getSpecIterator() {
        return (this.spec == null) ? null : this.spec.iterator();
    }

    public void addToSpec(TOrderComponent elem) {
        if(this.spec == null) {
            this.spec = new ArrayList<TOrderComponent>();
        }
        this.spec.add(elem);
    }

    public List<TOrderComponent> getSpec() {
        return this.spec;
    }

    public TOrder setSpec(List<TOrderComponent> spec) {
        this.spec = spec;
        return this;
    }

    public void unsetSpec() {
        this.spec = null;
    }

    /**
     * Returns true if field spec is set (has been assigned a value) and false
     * otherwise
     */
    public boolean isSetSpec() {
        return this.spec != null;
    }

    public void setSpecIsSet(boolean value) {
        if(!value) {
            this.spec = null;
        }
    }

    public void setFieldValue(_Fields field, Object value) {
        switch (field) {
        case SPEC:
            if(value == null) {
                unsetSpec();
            }
            else {
                setSpec((List<TOrderComponent>) value);
            }
            break;

        }
    }

    public Object getFieldValue(_Fields field) {
        switch (field) {
        case SPEC:
            return getSpec();

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
        case SPEC:
            return isSetSpec();
        }
        throw new IllegalStateException();
    }

    @Override
    public boolean equals(Object that) {
        if(that == null)
            return false;
        if(that instanceof TOrder)
            return this.equals((TOrder) that);
        return false;
    }

    public boolean equals(TOrder that) {
        if(that == null)
            return false;

        boolean this_present_spec = true && this.isSetSpec();
        boolean that_present_spec = true && that.isSetSpec();
        if(this_present_spec || that_present_spec) {
            if(!(this_present_spec && that_present_spec))
                return false;
            if(!this.spec.equals(that.spec))
                return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        List<Object> list = new ArrayList<Object>();

        boolean present_spec = true && (isSetSpec());
        list.add(present_spec);
        if(present_spec)
            list.add(spec);

        return list.hashCode();
    }

    @Override
    public int compareTo(TOrder other) {
        if(!getClass().equals(other.getClass())) {
            return getClass().getName().compareTo(other.getClass().getName());
        }

        int lastComparison = 0;

        lastComparison = Boolean.valueOf(isSetSpec())
                .compareTo(other.isSetSpec());
        if(lastComparison != 0) {
            return lastComparison;
        }
        if(isSetSpec()) {
            lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.spec,
                    other.spec);
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
        StringBuilder sb = new StringBuilder("TOrder(");
        boolean first = true;

        sb.append("spec:");
        if(this.spec == null) {
            sb.append("null");
        }
        else {
            sb.append(this.spec);
        }
        first = false;
        sb.append(")");
        return sb.toString();
    }

    public void validate() throws org.apache.thrift.TException {
        // check for required fields
        if(spec == null) {
            throw new org.apache.thrift.protocol.TProtocolException(
                    "Required field 'spec' was not present! Struct: "
                            + toString());
        }
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

    private static class TOrderStandardSchemeFactory implements SchemeFactory {
        public TOrderStandardScheme getScheme() {
            return new TOrderStandardScheme();
        }
    }

    private static class TOrderStandardScheme extends StandardScheme<TOrder> {

        public void read(org.apache.thrift.protocol.TProtocol iprot,
                TOrder struct) throws org.apache.thrift.TException {
            org.apache.thrift.protocol.TField schemeField;
            iprot.readStructBegin();
            while (true) {
                schemeField = iprot.readFieldBegin();
                if(schemeField.type == org.apache.thrift.protocol.TType.STOP) {
                    break;
                }
                switch (schemeField.id) {
                case 1: // SPEC
                    if(schemeField.type == org.apache.thrift.protocol.TType.LIST) {
                        {
                            org.apache.thrift.protocol.TList _list8 = iprot
                                    .readListBegin();
                            struct.spec = new ArrayList<TOrderComponent>(
                                    _list8.size);
                            TOrderComponent _elem9;
                            for (int _i10 = 0; _i10 < _list8.size; ++_i10) {
                                _elem9 = new TOrderComponent();
                                _elem9.read(iprot);
                                struct.spec.add(_elem9);
                            }
                            iprot.readListEnd();
                        }
                        struct.setSpecIsSet(true);
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
                TOrder struct) throws org.apache.thrift.TException {
            struct.validate();

            oprot.writeStructBegin(STRUCT_DESC);
            if(struct.spec != null) {
                oprot.writeFieldBegin(SPEC_FIELD_DESC);
                {
                    oprot.writeListBegin(new org.apache.thrift.protocol.TList(
                            org.apache.thrift.protocol.TType.STRUCT,
                            struct.spec.size()));
                    for (TOrderComponent _iter11 : struct.spec) {
                        _iter11.write(oprot);
                    }
                    oprot.writeListEnd();
                }
                oprot.writeFieldEnd();
            }
            oprot.writeFieldStop();
            oprot.writeStructEnd();
        }

    }

    private static class TOrderTupleSchemeFactory implements SchemeFactory {
        public TOrderTupleScheme getScheme() {
            return new TOrderTupleScheme();
        }
    }

    private static class TOrderTupleScheme extends TupleScheme<TOrder> {

        @Override
        public void write(org.apache.thrift.protocol.TProtocol prot,
                TOrder struct) throws org.apache.thrift.TException {
            TTupleProtocol oprot = (TTupleProtocol) prot;
            {
                oprot.writeI32(struct.spec.size());
                for (TOrderComponent _iter12 : struct.spec) {
                    _iter12.write(oprot);
                }
            }
        }

        @Override
        public void read(org.apache.thrift.protocol.TProtocol prot,
                TOrder struct) throws org.apache.thrift.TException {
            TTupleProtocol iprot = (TTupleProtocol) prot;
            {
                org.apache.thrift.protocol.TList _list13 = new org.apache.thrift.protocol.TList(
                        org.apache.thrift.protocol.TType.STRUCT,
                        iprot.readI32());
                struct.spec = new ArrayList<TOrderComponent>(_list13.size);
                TOrderComponent _elem14;
                for (int _i15 = 0; _i15 < _list13.size; ++_i15) {
                    _elem14 = new TOrderComponent();
                    _elem14.read(iprot);
                    struct.spec.add(_elem14);
                }
            }
            struct.setSpecIsSet(true);
        }
    }

}
