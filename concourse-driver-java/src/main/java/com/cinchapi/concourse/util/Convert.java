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
package com.cinchapi.concourse.util;

import java.io.IOException;
import java.io.StringReader;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.annotation.concurrent.Immutable;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.cinchapi.ccl.grammar.FunctionValueSymbol;
import com.cinchapi.ccl.grammar.Symbol;
import com.cinchapi.ccl.syntax.ConditionTree;
import com.cinchapi.ccl.syntax.FunctionTree;
import com.cinchapi.ccl.type.Function;
import com.cinchapi.ccl.type.function.IndexFunction;
import com.cinchapi.ccl.type.function.KeyConditionFunction;
import com.cinchapi.ccl.type.function.KeyRecordsFunction;
import com.cinchapi.ccl.type.function.TemporalFunction;
import com.cinchapi.ccl.util.NaturalLanguage;
import com.cinchapi.common.base.AnyStrings;
import com.cinchapi.common.base.ArrayBuilder;
import com.cinchapi.common.base.CheckedExceptions;
import com.cinchapi.common.base.Enums;
import com.cinchapi.common.io.ByteBuffers;
import com.cinchapi.concourse.Concourse;
import com.cinchapi.concourse.Link;
import com.cinchapi.concourse.Tag;
import com.cinchapi.concourse.Timestamp;
import com.cinchapi.concourse.annotate.PackagePrivate;
import com.cinchapi.concourse.annotate.UtilityClass;
import com.cinchapi.concourse.lang.ConcourseCompiler;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.thrift.Type;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.primitives.Longs;
import com.google.gson.JsonParseException;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;

/**
 * A collection of functions to convert objects. The public API defined in
 * {@link Concourse} uses certain objects for convenience that are not
 * recognized by Thrift, so it is necessary to convert back and forth between
 * different representations.
 * 
 * @author Jeff Nelson
 */
@UtilityClass
public final class Convert {

    /**
     * A mapping from strings that can be translated to {@link Operator
     * operators} to the operations to which they can be translated.
     */
    @PackagePrivate
    static Map<String, Operator> OPERATOR_STRINGS;
    static {
        OPERATOR_STRINGS = Maps.newHashMap();
        OPERATOR_STRINGS.put("==", Operator.EQUALS);
        OPERATOR_STRINGS.put("=", Operator.EQUALS);
        OPERATOR_STRINGS.put("eq", Operator.EQUALS);
        OPERATOR_STRINGS.put("!=", Operator.NOT_EQUALS);
        OPERATOR_STRINGS.put("ne", Operator.NOT_EQUALS);
        OPERATOR_STRINGS.put(">", Operator.GREATER_THAN);
        OPERATOR_STRINGS.put("gt", Operator.GREATER_THAN);
        OPERATOR_STRINGS.put(">=", Operator.GREATER_THAN_OR_EQUALS);
        OPERATOR_STRINGS.put("gte", Operator.GREATER_THAN_OR_EQUALS);
        OPERATOR_STRINGS.put("<", Operator.LESS_THAN);
        OPERATOR_STRINGS.put("lt", Operator.LESS_THAN);
        OPERATOR_STRINGS.put("<=", Operator.LESS_THAN_OR_EQUALS);
        OPERATOR_STRINGS.put("lte", Operator.LESS_THAN_OR_EQUALS);
        OPERATOR_STRINGS.put("><", Operator.BETWEEN);
        OPERATOR_STRINGS.put("bw", Operator.BETWEEN);
        OPERATOR_STRINGS.put("->", Operator.LINKS_TO);
        OPERATOR_STRINGS.put("lnks2", Operator.LINKS_TO);
        OPERATOR_STRINGS.put("lnk2", Operator.LINKS_TO);
        OPERATOR_STRINGS.put("regex", Operator.REGEX);
        OPERATOR_STRINGS.put("nregex", Operator.NOT_REGEX);
        OPERATOR_STRINGS.put("like", Operator.LIKE);
        OPERATOR_STRINGS.put("nlike", Operator.NOT_LIKE);
        OPERATOR_STRINGS.put("~", Operator.CONTAINS);
        OPERATOR_STRINGS.put("search", Operator.CONTAINS);
        OPERATOR_STRINGS.put("search_match", Operator.CONTAINS);
        OPERATOR_STRINGS.put("contains", Operator.CONTAINS);
        OPERATOR_STRINGS.put("!~", Operator.NOT_CONTAINS);
        OPERATOR_STRINGS.put("search_exclude", Operator.NOT_CONTAINS);
        OPERATOR_STRINGS.put("not_contains", Operator.NOT_CONTAINS);
        OPERATOR_STRINGS.put("ncontains", Operator.NOT_CONTAINS);
        for (Operator operator : Operator.values()) {
            OPERATOR_STRINGS.put(operator.name(), operator);
            OPERATOR_STRINGS.put(operator.symbol(), operator);
        }
        OPERATOR_STRINGS = ImmutableMap.copyOf(OPERATOR_STRINGS);
    }

    /**
     * The component of a resolvable link symbol that comes after the
     * resolvable key specification in the raw data.
     */
    @PackagePrivate
    static final String RAW_RESOLVABLE_LINK_SYMBOL_APPEND = "@"; // visible
                                                                 // for
                                                                 // testing

    /**
     * The component of a resolvable link symbol that comes before the
     * resolvable key specification in the raw data.
     */
    @PackagePrivate
    static final String RAW_RESOLVABLE_LINK_SYMBOL_PREPEND = "@"; // visible
                                                                  // for
                                                                  // testing

    /**
     * These classes have a special encoding that signals that string value
     * should actually be converted to those instances in
     * {@link #jsonToJava(JsonReader)}.
     */
    private static Set<Class<?>> CLASSES_WITH_ENCODED_STRING_REPR = Sets
            .newHashSet(Link.class, Tag.class, ResolvableLink.class,
                    Timestamp.class, IndexFunction.class,
                    KeyConditionFunction.class, KeyRecordsFunction.class);

    /**
     * A {@link Pattern} that can be used to determine whether a string matches
     * the expected pattern of an instruction to insert links to records that
     * are resolved by finding matches to a criteria.
     */
    // NOTE: This REGEX enforces that the string must contain at least one
    // space, which means that a CCL string can only be considered valid if it
    // contains a space (e.g. name=jeff is not valid CCL).
    private static final Pattern STRING_RESOLVABLE_LINK_REGEX = Pattern
            .compile("^@(?=.*[ ]).+@$");

    /**
     * The character that indicates a String should be treated as a {@link Tag}.
     */
    private static final char TAG_MARKER = '`';

    /**
     * Takes a JSON string representation of an object or an array of JSON
     * objects and returns a list of {@link Multimap multimaps} with the
     * corresponding data. Unlike {@link #jsonToJava(String)}, this method will
     * allow the top level element to be an array in the {code json} string.
     * 
     * @param json
     * @return A list of Java objects
     */
    public static List<Multimap<String, Object>> anyJsonToJava(String json) {
        List<Multimap<String, Object>> result = Lists.newArrayList();
        try (JsonReader reader = new JsonReader(new StringReader(json))) {
            reader.setLenient(true);
            if(reader.peek() == JsonToken.BEGIN_ARRAY) {
                try {
                    reader.beginArray();
                    while (reader.peek() != JsonToken.END_ARRAY) {
                        result.add(jsonToJava(reader));
                    }
                    reader.endArray();
                }
                catch (IllegalStateException e) {
                    throw new JsonParseException(e.getMessage());
                }
            }
            else {
                result.add(jsonToJava(reader));
            }
        }
        catch (IOException e) {
            throw CheckedExceptions.wrapAsRuntimeException(e);
        }
        return result;
    }

    /**
     * Return the Thrift Object that represents {@code object}.
     * 
     * @param object
     * @return the TObject
     */
    public static TObject javaToThrift(Object object) {
        if(object == null) {
            return TObject.NULL;
        }
        else {
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
            else if(object instanceof BigDecimal) {
                bytes = ByteBuffer.allocate(8);
                bytes.putDouble((double) ((BigDecimal) object).doubleValue());
                type = Type.DOUBLE;
            }
            else if(object instanceof Tag) {
                bytes = ByteBuffer.wrap(
                        object.toString().getBytes(StandardCharsets.UTF_8));
                type = Type.TAG;
            }
            else if(object instanceof Timestamp) {
                try {
                    bytes = ByteBuffer.allocate(8);
                    bytes.putLong(((Timestamp) object).getMicros());
                    type = Type.TIMESTAMP;
                }
                catch (IllegalStateException e) {
                    throw new UnsupportedOperationException(
                            "Cannot convert string based Timestamp to a TObject");
                }
            }
            else if(object instanceof Function) {
                type = Type.FUNCTION;
                Function function = (Function) object;
                byte[] nameBytes = function.operation()
                        .getBytes(StandardCharsets.UTF_8);
                byte[] keyBytes = function.key()
                        .getBytes(StandardCharsets.UTF_8);
                if(function instanceof IndexFunction) {
                    /*
                     * Schema:
                     * | type (1) | timestamp(8) | nameLength (4) | name
                     * (nameLength) | key |
                     */
                    bytes = ByteBuffer.allocate(
                            1 + 8 + 4 + nameBytes.length + keyBytes.length);
                    bytes.put((byte) FunctionType.INDEX.ordinal());
                    bytes.putLong(((TemporalFunction) function).timestamp());
                    bytes.putInt(nameBytes.length);
                    bytes.put(nameBytes);
                    bytes.put(keyBytes);
                }
                else if(function instanceof KeyRecordsFunction) {
                    /*
                     * Schema:
                     * | type (1) | timestamp(8) | nameLength (4) | name
                     * (nameLength) | keyLength (4) | key (keyLength) | records
                     * (8 each) |
                     */
                    KeyRecordsFunction func = (KeyRecordsFunction) function;
                    bytes = ByteBuffer.allocate(1 + 8 + 4 + nameBytes.length + 4
                            + keyBytes.length + 8 * func.source().size());
                    bytes.put((byte) FunctionType.KEY_RECORDS.ordinal());
                    bytes.putLong(((TemporalFunction) function).timestamp());
                    bytes.putInt(nameBytes.length);
                    bytes.put(nameBytes);
                    bytes.putInt(keyBytes.length);
                    bytes.put(keyBytes);
                    for (long record : func.source()) {
                        bytes.putLong(record);
                    }
                }
                else if(function instanceof KeyConditionFunction) {
                    /*
                     * Schema:
                     * | type (1) | timestamp(8) | nameLength (4) | name
                     * (nameLength) | keyLength (4) | key (keyLength) |
                     * condition |
                     */
                    KeyConditionFunction func = (KeyConditionFunction) function;
                    String condition = ConcourseCompiler.get()
                            .tokenize(func.source()).stream()
                            .map(Symbol::toString)
                            .collect(Collectors.joining(" "));
                    bytes = ByteBuffer.allocate(1 + 9 + 4 + nameBytes.length + 4
                            + keyBytes.length + condition.length());
                    bytes.put((byte) FunctionType.KEY_CONDITION.ordinal());
                    bytes.putLong(((TemporalFunction) function).timestamp());
                    bytes.putInt(nameBytes.length);
                    bytes.put(nameBytes);
                    bytes.putInt(keyBytes.length);
                    bytes.put(keyBytes);
                    bytes.put(condition.getBytes(StandardCharsets.UTF_8));
                }
                else {
                    throw new UnsupportedOperationException(
                            "Cannot convert the following function to a TObject: "
                                    + function);
                }
            }
            else {
                bytes = ByteBuffer.wrap(
                        object.toString().getBytes(StandardCharsets.UTF_8));
                type = Type.STRING;
            }
            bytes.rewind();
            return new TObject(bytes, type).setJavaFormat(object);
        }
    }

    /**
     * Convert a JSON formatted string to a mapping that associates each key
     * with the Java objects that represent the corresponding values. This
     * method is designed to parse simple JSON structures that associate keys to
     * simple values or arrays without knowing the type of each element ahead of
     * time.
     * <p>
     * This method can properly handle JSON strings that abide by the following
     * rules:
     * <ul>
     * <li>The top level element in the JSON string must be an Object</li>
     * <li>No nested objects (e.g. a key cannot map to an object)</li>
     * <li>No null values</li>
     * </ul>
     * </p>
     * 
     * @param json
     * @return the converted data
     */
    public static Multimap<String, Object> jsonToJava(String json) {
        try (JsonReader reader = new JsonReader(new StringReader(json))) {
            reader.setLenient(true);
            return jsonToJava(reader);
        }
        catch (IOException e) {
            throw CheckedExceptions.wrapAsRuntimeException(e);
        }
    }

    /**
     * Serialize the {@link list} of {@link Multimap maps} with data to a JSON
     * string that can be batch inserted into Concourse.
     * 
     * @param list the list of data {@link Multimap maps} to include in the JSON
     *            object. This is meant to map to the return value of
     *            {@link #anyJsonToJava(String)}
     * @return the JSON string representation of the {@code list}
     */
    @SuppressWarnings("unchecked")
    public static String mapsToJson(Collection<Multimap<String, Object>> list) {
        // GH-116: The signature declares that the list should contain Multimap
        // instances, but we check the type of each element in case the data is
        // coming from a JVM dynamic language (i.e. Groovy) that has syntactic
        // sugar for a java.util.Map
        StringBuilder sb = new StringBuilder();
        sb.append('[');
        for (Object map : list) {
            if(map instanceof Multimap) {
                Multimap<String, Object> map0 = (Multimap<String, Object>) map;
                sb.append(mapToJson(map0));
                sb.append(",");
            }
            else if(map instanceof Map) {
                Map<String, Object> map0 = (Map<String, Object>) map;
                sb.append(mapToJson(map0));
                sb.append(",");
            }
            else {
                ((Multimap<String, Object>) map).getClass(); // force
                                                             // ClassCastException
                                                             // to be thrown
            }

        }
        sb.setCharAt(sb.length() - 1, ']');
        return sb.toString();
    }

    /**
     * Serialize the {@code map} of data as a JSON object string that can be
     * inserted into Concourse.
     * 
     * @param map data to include in the JSON object.
     * @return the JSON string representation of the {@code map}
     */
    public static String mapToJson(Map<String, ?> map) {
        return DataServices.gson().toJson(map);
    }

    /**
     * Serialize the {@code map} of a data as a JSON string that can inserted
     * into Concourse.
     * 
     * @param map the data to include in the JSON object. This is meant to map
     *            to the return value of {@link #jsonToJava(String)}
     * @return the JSON string representation of the {@code map}
     */
    public static String mapToJson(Multimap<String, Object> map) {
        return mapToJson(map.asMap());
    }

    /**
     * Convert the {@code operator} to a string representation.
     * 
     * @param operator
     * @return the operator string
     */
    public static String operatorToString(Operator operator) {
        String string = "";
        switch (operator) {
        case EQUALS:
            string = "=";
            break;
        case NOT_EQUALS:
            string = "!=";
            break;
        case GREATER_THAN:
            string = ">";
            break;
        case GREATER_THAN_OR_EQUALS:
            string = ">=";
            break;
        case LESS_THAN:
            string = "<";
            break;
        case LESS_THAN_OR_EQUALS:
            string = "<=";
            break;
        case BETWEEN:
            string = "><";
            break;
        default:
            string = operator.name();
            break;

        }
        return string;
    }

    /**
     * For a scalar object that may be a {@link TObject} or a collection of
     * other objects that may contain {@link TObject TObjects}, convert to the
     * appropriate java representation.
     * 
     * @param tobject the possible TObject or collection of TObjects
     * @return the java representation
     */
    @SuppressWarnings("unchecked")
    public static <T> T possibleThriftToJava(Object tobject) {
        if(tobject instanceof TObject) {
            return (T) thriftToJava((TObject) tobject);
        }
        else if(tobject instanceof List) {
            return (T) ((List<?>) tobject).stream()
                    .map(Convert::possibleThriftToJava)
                    .collect(Collectors.toList());
        }
        else if(tobject instanceof Set) {
            return (T) ((Set<?>) tobject).stream()
                    .map(Convert::possibleThriftToJava)
                    .collect(Collectors.toSet());
        }
        else if(tobject instanceof Map) {
            return (T) ((Map<?, ?>) tobject).entrySet().stream()
                    .collect(Collectors.toMap(
                            e -> possibleThriftToJava(e.getKey()),
                            e -> possibleThriftToJava(e.getValue())));
        }
        else {
            return (T) tobject;
        }
    }

    /**
     * Analyze {@code value} and convert it to the appropriate Java primitive or
     * Object.
     * <p>
     * <h1>Conversion Rules</h1>
     * <ul>
     * <li><strong>String</strong> - the value is converted to a string if it
     * starts and ends with matching single (') or double ('') quotes.
     * Alternatively, the value is converted to a string if it cannot be
     * converted to another type</li>
     * <li><strong>{@link ResolvableLink}</strong> - the value is converted to a
     * ResolvableLink if it is a properly formatted specification returned from
     * the {@link #stringToResolvableLinkSpecification(String, String)} method
     * (<strong>NOTE: </strong> this is a rare case)</li>
     * <li><strong>{@link Link}</strong> - the value is converted to a Link if
     * it is an int or long that is prepended by an '@' sign (i.e. @1234)</li>
     * <li><strong>Boolean</strong> - the value is converted to a Boolean if it
     * is equal to 'true', or 'false' regardless of case</li>
     * <li><strong>Double</strong> - the value is converted to a double if and
     * only if it is a decimal number that is immediately followed by a single
     * capital "D" (e.g. 3.14D)</li>
     * <li><strong>Tag</strong> - the value is converted to a Tag if it starts
     * and ends with matching (`) quotes</li>
     * <li><strong>Integer, Long, Float</strong> - the value is converted to a
     * non double number depending upon whether it is a standard integer (e.g.
     * less than {@value java.lang.Integer#MAX_VALUE}), a long, or a floating
     * point decimal</li>
     * <li><strong>Function</strong> - the value is converted to a
     * {@link Function} if it is not quoted and can be parsed as such by the
     * {@link ConcourseCompiler}.</li>
     * </ul>
     * </p>
     * 
     * 
     * @param value
     * @return the converted value
     */
    public static Object stringToJava(String value) {
        if(value.isEmpty()) {
            return value;
        }
        char first = value.charAt(0);
        char last = value.charAt(value.length() - 1);
        Long record;
        if(AnyStrings.isWithinQuotes(value, TAG_MARKER)) {
            // keep value as string since its between single or double quotes
            return value.substring(1, value.length() - 1);
        }
        else if(first == '@' && (record = Longs
                .tryParse(value.substring(1, value.length()))) != null) {
            return Link.to(record);
        }
        else if(first == '@' && last == '@'
                && STRING_RESOLVABLE_LINK_REGEX.matcher(value).matches()) {
            String ccl = value.substring(1, value.length() - 1);
            return ResolvableLink.create(ccl);

        }
        else if(value.equalsIgnoreCase("true")) {
            return true;
        }
        else if(value.equalsIgnoreCase("false")) {
            return false;
        }
        else if(first == TAG_MARKER && last == TAG_MARKER) {
            return Tag.create(value.substring(1, value.length() - 1));
        }
        else if(first == '|' && last == '|') {
            value = value.substring(1, value.length() - 1);
            String[] toks = value.split("\\|");
            Timestamp timestamp;
            if(toks.length == 1) {
                // #value is a timestring that intends to rely on either one of
                // the built-in DateTimeFormatters or the natural language
                // translation in order to figure out the microseconds with
                // which to create the Timestamp
                timestamp = Timestamp
                        .fromMicros(NaturalLanguage.parseMicros(value));
            }
            else {
                // #value looks like timestring|format in which case the second
                // part is the DateTimeFormatter to use for getting the
                // microseconds with which to create the Timestamp
                // Valid formatting options can be found at
                // http://www.joda.org/joda-time/apidocs/org/joda/time/format/DateTimeFormat.html
                DateTimeFormatter formatter = DateTimeFormat
                        .forPattern(toks[1]);
                timestamp = Timestamp.parse(toks[0], formatter);
            }
            return timestamp;
        }
        else {
            if(last == ')') {
                // It is possible that the string is a FunctionValue, so use the
                // Compiler to try to parse it as such. Please note that this
                // method intentionally does not attempt to convert to an
                // ImplictKeyRecordFunction (e.g. key | function) because those
                // cannot serve as a evaluation value
                try {
                    FunctionTree tree = (FunctionTree) ConcourseCompiler.get()
                            .parse(value);
                    FunctionValueSymbol symbol = (FunctionValueSymbol) tree
                            .root();
                    return symbol.function();
                }
                catch (Exception e) {/* ignore */}
            }
            try {
                return MoreObjects
                        .firstNonNull(AnyStrings.tryParseNumber(value), value);
            }
            catch (NumberFormatException e) {
                return value;
            }

        }
    }

    /**
     * Convert the {@code symbol} into the appropriate {@link Operator}.
     * 
     * @param symbol - the string form of a symbol (i.e. =, >, >=, etc) or a
     *            CaSH shortcut (i.e. eq, gt, gte, etc)
     * @return the {@link Operator} that is parsed from the string
     *         {@code symbol}
     */
    public static Operator stringToOperator(String symbol) {
        Operator operator = OPERATOR_STRINGS.get(symbol);
        if(operator == null) {
            throw new IllegalStateException(
                    "Cannot parse " + symbol + " into an operator");
        }
        else {
            return operator;
        }
    }

    /**
     * <p>
     * Users are encouraged to use {@link Link#toWhere(String)} instead of this
     * method.
     * </p>
     * <p>
     * <strong>USE WITH CAUTION: </strong> This conversation is only necessary
     * when bulk inserting data in string form (i.e. importing data from a CSV
     * file) that should have static links dynamically resolved.<strong>
     * <em>Unless you are certain otherwise, you should never need to use this
     * method because there is probably some intermediate function or framework
     * that does this for you!</em></strong>
     * </p>
     * <p>
     * Convert the {@code ccl} string to a {@link ResolvableLink} instruction
     * for the receiver to add links to all the records that match the criteria.
     * </p>
     * <p>
     * Please note that this method only returns a specification and not an
     * actual {@link ResolvableLink} object. Use the
     * {@link #stringToJava(String)} method on the value returned from this
     * method to get the object.
     * </p>
     * 
     * @param ccl - The criteria to use when resolving link targets
     * @return An instruction to create a {@link ResolvableLink}
     */
    public static String stringToResolvableLinkInstruction(String ccl) {
        return AnyStrings.joinSimple(RAW_RESOLVABLE_LINK_SYMBOL_PREPEND, ccl,
                RAW_RESOLVABLE_LINK_SYMBOL_APPEND);
    }

    /**
     * <p>
     * <strong>USE WITH CAUTION: </strong> This conversation is only necessary
     * for applications that import raw data but cannot use the Concourse API
     * directly and therefore cannot explicitly add links (e.g. the
     * import-framework that handles raw string data). <strong>
     * <em>If you have access to the Concourse API, you should not use this
     * method!</em> </strong>
     * </p>
     * Convert the {@code rawValue} into a {@link ResolvableLink} specification
     * that instructs the receiver to add a link to all the records that have
     * {@code rawValue} mapped from {@code key}.
     * <p>
     * Please note that this method only returns a specification and not an
     * actual {@link ResolvableLink} object. Use the
     * {@link #stringToJava(String)} method on the value returned from this
     * method to get the object.
     * </p>
     * 
     * @param key
     * @param rawValue
     * @return the transformed value.
     */
    @Deprecated
    public static String stringToResolvableLinkSpecification(String key,
            String rawValue) {
        return stringToResolvableLinkInstruction(
                AnyStrings.joinWithSpace(key, "=", rawValue));
    }

    /**
     * Return the Java Object that represents {@code object}.
     * 
     * @param object
     * @return the Object
     */
    public static Object thriftToJava(TObject object) {
        Preconditions.checkState(object.getType() != null,
                "Cannot read value because it has been "
                        + "created with a newer version of Concourse "
                        + "Server. Please upgrade this client.");
        Object java = object.getJavaFormat();
        if(java == null) {
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
            case TAG:
                java = ByteBuffers.getUtf8String(buffer);
                break;
            case TIMESTAMP:
                java = Timestamp.fromMicros(buffer.getLong());
                break;
            case FUNCTION:
                FunctionType type = Enums.parseIgnoreCase(FunctionType.class,
                        buffer.get());
                long timestamp = buffer.getLong();
                int nameLength = buffer.getInt();
                String name = ByteBuffers
                        .getUtf8String(ByteBuffers.get(buffer, nameLength));
                int keyLength;
                String key;
                switch (type) {
                case INDEX:
                    key = ByteBuffers.getUtf8String(buffer);
                    java = new IndexFunction(name, key, timestamp);
                    break;
                case KEY_RECORDS:
                    keyLength = buffer.getInt();
                    key = ByteBuffers
                            .getUtf8String(ByteBuffers.get(buffer, keyLength));
                    ArrayBuilder<Long> ab = ArrayBuilder.builder();
                    while (buffer.hasRemaining()) {
                        long record = buffer.getLong();
                        ab.add(record);
                    }
                    java = new KeyRecordsFunction(timestamp, name, key,
                            ab.build());
                    break;
                case KEY_CONDITION:
                    keyLength = buffer.getInt();
                    key = ByteBuffers
                            .getUtf8String(ByteBuffers.get(buffer, keyLength));
                    String condition = ByteBuffers.getUtf8String(buffer);
                    ConditionTree tree = (ConditionTree) ConcourseCompiler.get()
                            .parse(condition);
                    java = new KeyConditionFunction(name, key, tree, timestamp);
                    break;
                }
                break;
            case NULL:
                java = null;
                break;
            default:
                java = ByteBuffers.getUtf8String(buffer);
                break;
            }
            buffer.rewind();
            object.setJavaFormat(java);
        }
        return java;
    }

    /**
     * If {@code value} is a string that represents a function value, convert it
     * to a {@link Function}. If {@code value} is an escaped string that could
     * be interpreted as a function value, unescape it and return it. Otherwise,
     * return the original value.
     * 
     * @param value
     * @return the converted function value, an unescaped version of the
     *         original {@code value} or the original {@code value}
     */
    public static Object toFunctionOrUnescapedValueIfPossible(Object value) {
        if(value instanceof String) {
            Object $value = stringToJava((String) value);
            if($value instanceof Function) {
                value = $value;
            }
            else if($value instanceof String) {
                // It is possible that #stringToJava converted the original
                // value to a more optimized string (e.g. dropping quotes that
                // were used for escaping) so use the new string value
                value = $value;
            }
        }
        return value;
    }

    /**
     * Convert the next JSON object in the {@code reader} to a mapping that
     * associates each key with the Java objects that represent the
     * corresponding values.
     * 
     * <p>
     * This method has the same rules and limitations as
     * {@link #jsonToJava(String)}. It simply uses a {@link JsonReader} to
     * handle reading an array of objects.
     * </p>
     * <p>
     * <strong>This method DOES NOT {@link JsonReader#close()} the
     * {@code reader}.</strong>
     * </p>
     * 
     * @param reader the {@link JsonReader} that contains a stream of JSON
     * @return the JSON data in the form of a {@link Multimap} from keys to
     *         values
     */
    private static Multimap<String, Object> jsonToJava(JsonReader reader) {
        Multimap<String, Object> data = HashMultimap.create();
        try {
            reader.beginObject();
            JsonToken peek0;
            while ((peek0 = reader.peek()) != JsonToken.END_OBJECT) {
                String key = reader.nextName();
                peek0 = reader.peek();
                if(peek0 == JsonToken.BEGIN_ARRAY) {
                    // If we have an array, add the elements individually. If
                    // there are any duplicates in the array, they will be
                    // filtered out by virtue of the fact that a HashMultimap
                    // does not store dupes.
                    reader.beginArray();
                    JsonToken peek = reader.peek();
                    do {
                        Object value;
                        if(peek == JsonToken.BOOLEAN) {
                            value = reader.nextBoolean();
                        }
                        else if(peek == JsonToken.NUMBER) {
                            value = stringToJava(reader.nextString());
                        }
                        else if(peek == JsonToken.STRING) {
                            String orig = reader.nextString();
                            value = stringToJava(orig);
                            if(orig.isEmpty()) {
                                value = orig;
                            }
                            // If the token looks like a string, it MUST be
                            // converted to a Java string unless it is a
                            // masquerading double or an instance of Thrift
                            // translatable class that has a special string
                            // representation (i.e. Tag, Link)
                            else if(orig.charAt(orig.length() - 1) != 'D'
                                    && !CLASSES_WITH_ENCODED_STRING_REPR
                                            .contains(value.getClass())) {
                                value = value.toString();
                            }
                        }
                        else if(peek == JsonToken.NULL) {
                            reader.skipValue();
                            continue;
                        }
                        else {
                            throw new JsonParseException(
                                    "Cannot parse nested object or array within an array");
                        }
                        data.put(key, value);
                    }
                    while ((peek = reader.peek()) != JsonToken.END_ARRAY);
                    reader.endArray();
                }
                else {
                    Object value;
                    if(peek0 == JsonToken.BOOLEAN) {
                        value = reader.nextBoolean();
                    }
                    else if(peek0 == JsonToken.NUMBER) {
                        value = stringToJava(reader.nextString());
                    }
                    else if(peek0 == JsonToken.STRING) {
                        String orig = reader.nextString();
                        value = stringToJava(orig);
                        if(orig.isEmpty()) {
                            value = orig;
                        }
                        // If the token looks like a string, it MUST be
                        // converted to a Java string unless it is a
                        // masquerading double or an instance of Thrift
                        // translatable class that has a special string
                        // representation (i.e. Tag, Link)
                        else if(orig.charAt(orig.length() - 1) != 'D'
                                && !CLASSES_WITH_ENCODED_STRING_REPR
                                        .contains(value.getClass())) {
                            value = value.toString();
                        }
                    }
                    else if(peek0 == JsonToken.NULL) {
                        reader.skipValue();
                        continue;
                    }
                    else {
                        throw new JsonParseException(
                                "Cannot parse nested object to value");
                    }
                    data.put(key, value);
                }
            }
            reader.endObject();
            return data;
        }
        catch (IOException | IllegalStateException e) {
            throw new JsonParseException(e.getMessage());
        }
    }

    private Convert() {/* Utility Class */}

    /**
     * A special class that is used to indicate that the record(s) to which one
     * or more {@link Link links} should point must be resolved by finding all
     * records that match a criteria.
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
     * {@link Convert#stringToResolvableLinkInstruction(String)} on the raw
     * data.
     * </p>
     * 
     * @author Jeff Nelson
     */
    @Immutable
    public static final class ResolvableLink {

        // NOTE: This class does not define #hashCode() or #equals() because the
        // defaults are the desired behaviour

        /**
         * Create a new {@link ResolvableLink} that provides instructions to
         * create links to all the records that match the {@code ccl} string.
         * 
         * @param ccl - The criteria to use when resolving link targets
         * @return the ResolvableLink
         */
        @PackagePrivate
        static ResolvableLink create(String ccl) {
            return new ResolvableLink(ccl);
        }

        /**
         * Create a new {@link ResolvableLink} that provides instructions to
         * create a link to the records which contain {@code value} for
         * {@code key}.
         * 
         * @param key
         * @param value
         * @return the ResolvableLink
         */
        @PackagePrivate
        @Deprecated
        static ResolvableLink newResolvableLink(String key, Object value) {
            return new ResolvableLink(key, value);
        }

        @Deprecated
        protected final String key;

        @Deprecated
        protected final Object value;

        /**
         * The CCL string that should be used when resolving the link targets.
         */
        private final String ccl;

        /**
         * Construct a new instance.
         * 
         * @param ccl - The criteria to use when resolving link targets
         */
        private ResolvableLink(String ccl) {
            this.ccl = ccl;
            this.key = null;
            this.value = null;
        }

        /**
         * Construct a new instance.
         * 
         * @param key
         * @param value
         * @deprecated As of version 0.5.0
         */
        @Deprecated
        private ResolvableLink(String key, Object value) {
            this.ccl = new StringBuilder().append(key).append(" = ")
                    .append(value).toString();
            this.key = key;
            this.value = value;
        }

        /**
         * Return the {@code ccl} string that should be used for resolving link
         * targets.
         * 
         * @return {@link #ccl}
         */
        public String getCcl() {
            return ccl;
        }

        /**
         * Return the associated key.
         * 
         * @return the key
         */
        @Deprecated
        public String getKey() {
            return key;
        }

        /**
         * Return the associated value.
         * 
         * @return the value
         */
        @Deprecated
        public Object getValue() {
            return value;
        }

        @Override
        public String toString() {
            return AnyStrings.format("{} for {}",
                    this.getClass().getSimpleName(), ccl);
        }

    }

    /**
     * An enum that describes the possible function value types.
     *
     * @author Jeff Nelson
     */
    private enum FunctionType {
        INDEX, KEY_RECORDS, KEY_CONDITION
    }

}
