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

import java.util.LinkedHashMap;
import java.util.Map;

import javax.annotation.Nullable;

import com.google.common.base.Strings;

/**
 * A special {@link LinkedHashMap} with a named key and value that returns a
 * pretty {@link #toString()} representation in the form of a two column table
 * where the key/value pairs are listed in different rows :
 * 
 * <pre>
 * +-------------------------------+
 * | Key  | Values                 |
 * +-------------------------------+
 * | age  | [1]                    |
 * | foo  | [1]                    |
 * | name | [Jeff Nelson, jeffery] |
 * +-------------------------------+
 * </pre>
 * <p>
 * A {@link PrettyLinkedHashMap} is suitable to use when displaying information
 * about a single record or document.
 * </p>
 * 
 * @author jnelson
 */
public class PrettyLinkedHashMap<K, V> extends LinkedHashMap<K, V> {

    /**
     * Return an empty {@link PrettyLinkedHashMap} with the default key and
     * value
     * names.
     * 
     * @return the TLinkedHashMap
     */
    public static <K, V> PrettyLinkedHashMap<K, V> newTLinkedHashMap() {
        return new PrettyLinkedHashMap<K, V>(null, null);
    }

    /**
     * Return an empty TLinkedHashMap with the specified {@code keyName} and
     * {@code valueName}.
     * 
     * @param keyName
     * @param valueName
     * @return the TLinkedHashMap.
     */
    public static <K, V> PrettyLinkedHashMap<K, V> newTLinkedHashMap(
            String keyName, String valueName) {
        return new PrettyLinkedHashMap<K, V>(keyName, valueName);
    }

    private static final long serialVersionUID = 1L; // serializability
                                                     // inherited from parent
                                                     // class

    private String keyName = "Key";
    private String valueName = "Value";
    private int keyLength = keyName.length();
    private int valueLength = valueName.length();

    /**
     * Construct a new instance.
     * 
     * @param keyName
     * @param valueName
     */
    private PrettyLinkedHashMap(@Nullable String keyName,
            @Nullable String valueName) {
        if(!Strings.isNullOrEmpty(keyName)) {
            setKeyName(keyName);
        }
        if(!Strings.isNullOrEmpty(valueName)) {
            setValueName(valueName);
        }
    }

    @Override
    public V put(K key, V value) {
        keyLength = Math.max(key.toString().length(), keyLength);
        valueLength = Math.max(value != null ? value.toString().length() : 4,
                valueLength);
        return super.put(key, value);
    }

    /**
     * Set the keyName to {@code name}.
     * 
     * @param name
     * @return this
     */
    public PrettyLinkedHashMap<K, V> setKeyName(String name) {
        keyName = name;
        keyLength = Math.max(name.length(), keyLength);
        return this;
    }

    /**
     * Set the valueName to {@code name}
     * 
     * @param name
     * @return this
     */
    public PrettyLinkedHashMap<K, V> setValueName(String name) {
        valueName = name;
        valueLength = Math.max(name.length(), valueLength);
        return this;
    }

    @Override
    public String toString() {
        String format = "| %-" + keyLength + "s | %-" + valueLength + "s |%n";
        String hr = Strings.padEnd("+", keyLength + valueLength + 6, '-'); // there
                                                                           // are
                                                                           // 6
                                                                           // spaces
                                                                           // in
                                                                           // the
                                                                           // #format
        hr += "+" + System.getProperty("line.separator");
        StringBuilder sb = new StringBuilder();
        sb.append(System.getProperty("line.separator"));
        sb.append(hr);
        sb.append(String.format(format, keyName, valueName));
        sb.append(hr);
        for (Map.Entry<K, V> entry : entrySet()) {
            sb.append(String.format(format, entry.getKey(), entry.getValue()));
        }
        sb.append(hr);
        return sb.toString();
    }
}
