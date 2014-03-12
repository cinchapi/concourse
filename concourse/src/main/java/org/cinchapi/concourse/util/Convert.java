/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013-2014 Jeff Nelson, Cinchapi Software Collective
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package org.cinchapi.concourse.util;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;

import javax.annotation.concurrent.Immutable;

import org.cinchapi.concourse.Link;
import org.cinchapi.concourse.annotate.PackagePrivate;
import org.cinchapi.concourse.annotate.UtilityClass;
import org.cinchapi.concourse.thrift.TObject;
import org.cinchapi.concourse.thrift.Type;

/**
 * A collection of functions to convert objects. The public API defined in
 * {@link Concourse} uses certain objects for convenience that are not
 * recognized by Thrift, so it is necessary to convert back and forth between
 * different representations.
 * 
 * @author jnelson
 */
@UtilityClass
public final class Convert {

    /**
     * The component of a resolvable link symbol that comes before the
     * resolvable key specification in the raw data.
     */
    @PackagePrivate
    static final String RAW_RESOLVABLE_LINK_SYMBOL_PREPEND = "@<"; // visible
                                                                   // for
                                                                   // testing

    /**
     * The component of a resolvable link symbol that comes after the
     * resolvable key specification in the raw data.
     */
    @PackagePrivate
    static final String RAW_RESOLVABLE_LINK_SYMBOL_APPEND = ">@"; // visible
                                                                  // for
                                                                  // testing

    /**
     * Return the Thrift Object that represents {@code object}.
     * 
     * @param object
     * @return the TObject
     */
    public static TObject javaToThrift(Object object) {
        ByteBuffer bytes;
        Type type = null;
        if(object instanceof Boolean) {
            bytes = ByteBuffer.allocate(1);
            bytes.put((boolean) object ? (byte) 1 : (byte) 0);
            type = Type.BOOLEAN;
        }
        else if(object instanceof Double) {
            bytes = ByteBuffer.allocate(8);
            bytes.putDouble((double) object);
            type = Type.DOUBLE;
        }
        else if(object instanceof Float) {
            bytes = ByteBuffer.allocate(4);
            bytes.putFloat((float) object);
            type = Type.FLOAT;
        }
        else if(object instanceof Link) {
            bytes = ByteBuffer.allocate(8);
            bytes.putLong(((Link) object).longValue());
            type = Type.LINK;
        }
        else if(object instanceof Long) {
            bytes = ByteBuffer.allocate(8);
            bytes.putLong((long) object);
            type = Type.LONG;
        }
        else if(object instanceof Integer) {
            bytes = ByteBuffer.allocate(4);
            bytes.putInt((int) object);
            type = Type.INTEGER;
        }
        else {
            bytes = ByteBuffer.wrap(object.toString().getBytes(
                    StandardCharsets.UTF_8));
            type = Type.STRING;
        }
        bytes.rewind();
        return new TObject(bytes, type);
    }

    /**
     * Return the Java Object that represents {@code object}.
     * 
     * @param object
     * @return the Object
     */
    public static Object thriftToJava(TObject object) {
        Object java = null;
        ByteBuffer buffer = object.bufferForData();
        switch (object.getType()) {
        case BOOLEAN:
            java = ByteBuffers.getBoolean(buffer);
            break;
        case DOUBLE:
            java = buffer.getDouble();
            break;
        case FLOAT:
            java = buffer.getFloat();
            break;
        case INTEGER:
            java = buffer.getInt();
            break;
        case LINK:
            java = Link.to(buffer.getLong());
            break;
        case LONG:
            java = buffer.getLong();
            break;
        default:
            java = ByteBuffers.getString(buffer);
            break;
        }
        buffer.rewind();
        return java;
    }

    /**
     * Convert the {@code rawValue} into a resolvable link specification that
     * instructs the receiver to link to records that have {@code rawValue}
     * mapped from {@code key}.
     * <p>
     * Please note that this method only returns a specification and not an
     * actual {@link ResolvableLink} object. Use the
     * {@link #stringToJava(String)} method on the value returned from this
     * method to get an actual ResolvableLink object.
     * </p>
     * 
     * @param resolvableKey
     * @param rawValue
     * @return the transformed value.
     */
    public static String stringToResolvableLinkSpecification(String key,
            String rawValue) {
        return MessageFormat.format("{0}{1}{0}", MessageFormat.format(
                "{0}{1}{2}", RAW_RESOLVABLE_LINK_SYMBOL_PREPEND, key,
                RAW_RESOLVABLE_LINK_SYMBOL_APPEND), rawValue);
    }

    /**
     * Analyze {@code value} and convert it to the appropriate Java primitive or
     * Object. <h1>Conversion Rules</h1>
     * <ul>
     * <li><strong>String</strong> - the value is converted to a string if it
     * leads and ends with matching single (') or double ('') quotes.
     * Alternatively, the value is converted to a string if it cannot be
     * converted to another Object type</li>
     * <li><strong>{@link ResolvableLink}</strong> - the value is converted to a
     * ResolvableLink if it is a properly formatted specification returned from
     * the {@link #stringToResolvableLinkSpecification(String, String)} method</li>
     * <li><strong>{@link Link}</strong> - the value is converted to a Link if it is a
     * number that is wrapped by @ signs</li>
     * <li><strong>Boolean</strong> - the value is converted to a Boolean if it
     * is equal to 'true', or 'false' regardless of case</li>
     * <li><strong>Double</strong> - the value is converted to a double if and
     * only if it is a number that is immediately followed by a single capital
     * "D" (e.g. 3.14D)</li>
     * <li><strong>Integer, Long, Float</strong> - the value is converted to a
     * non double number depending upon whether it is a standard integer (e.g.
     * less than {@value Integer#MAX_VALUE}), a long integer, or a floating
     * point decimal</li>
     * </ul>
     * 
     * 
     * @param value
     * @return the converted value
     */
    public static Object stringToJava(String value) {
        if(value.matches("\"([^\"]+)\"|'([^']+)'")) { // keep value as
                                                      // string since its
                                                      // between single or
                                                      // double quotes
            return value.substring(1, value.length() - 1);
        }
        else if(value.matches(MessageFormat.format("{0}{1}{0}", MessageFormat
                .format("{0}{1}{2}", RAW_RESOLVABLE_LINK_SYMBOL_PREPEND, ".+",
                        RAW_RESOLVABLE_LINK_SYMBOL_APPEND), ".+"))) {
            String[] parts = value.split(RAW_RESOLVABLE_LINK_SYMBOL_PREPEND, 3)[1]
                    .split(RAW_RESOLVABLE_LINK_SYMBOL_APPEND, 2);
            String key = parts[0];
            Object theValue = stringToJava(parts[1]);
            return new ResolvableLink(key, theValue);
        }
        else if(value.matches("@-?[0-9]+@")) {
            return Link.to(Long.parseLong(value.replace("@", "")));
        }
        else if(value.equalsIgnoreCase("true")) {
            return true;
        }
        else if(value.equalsIgnoreCase("false")) {
            return false;
        }
        else if(value.matches("-?[0-9]+\\.[0-9]+D")) { // Must append "D" to end
                                                       // of string in order to
                                                       // force a double
            return Double.valueOf(value.substring(0, value.length() - 1));
        }
        else {
            Class<?>[] classes = { Integer.class, Long.class, Float.class,
                    Double.class };
            for (Class<?> clazz : classes) {
                try {
                    return clazz.getMethod("valueOf", String.class).invoke(
                            null, value);
                }
                catch (Exception e) {
                    if(e instanceof NumberFormatException
                            || e.getCause() instanceof NumberFormatException) {
                        continue;
                    }
                }
            }
            return value;
        }
    }

    /**
     * A special class that is used to indicate that the record to which a Link
     * should point must be resolved by finding all records that have a
     * specified key equal to a specified value.
     * <p>
     * This class is NOT part of the public API, so it should not be used as a
     * value for input to the client. Objects of this class exist merely to
     * provide utilities that depend on the client with instructions for
     * resolving a link in cases when the end-user of the utility cannot use the
     * client directly themselves (i.e. specifying a resolvable link in a raw
     * text file for the import framework).
     * </p>
     * <p>
     * To get an object of this class, call {@link Convert#stringToJava(String)}
     * on the result of calling
     * {@link Convert#stringToResolvableLinkSpecification(String String)} on the
     * raw data.
     * </p>
     * 
     * @author jnelson
     */
    @Immutable
    public static final class ResolvableLink {

        // NOTE: This class does not define #hashCode() or #equals() because the
        // defaults are the desired behaviour

        /**
         * Create a new {@link ResolvableLink} that provides instructions to
         * create
         * a link to the records which contain {@code value} for {@code key}.
         * 
         * @param key
         * @param value
         * @return the ResolvableLink
         */
        @PackagePrivate
        static ResolvableLink newResolvableLink(String key, Object value) {
            return new ResolvableLink(key, value);
        }

        protected final String key;
        protected final Object value;

        /**
         * Construct a new instance.
         * 
         * @param key
         * @param value
         */
        private ResolvableLink(String key, Object value) {
            this.key = key;
            this.value = value;
        }

        /**
         * Return the associated key.
         * 
         * @return the key
         */
        public String getKey() {
            return key;
        }

        /**
         * Return the associated value.
         * 
         * @return the value
         */
        public Object getValue() {
            return value;
        }

    }

    private Convert() {/* Utility Class */}

}
