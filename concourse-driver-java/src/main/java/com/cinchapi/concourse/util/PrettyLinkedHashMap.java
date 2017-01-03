/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cinchapi.concourse.util;

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
 * @author Jeff Nelson
 */
public class PrettyLinkedHashMap<K, V> extends LinkedHashMap<K, V> {

    /**
     * Return an empty {@link PrettyLinkedHashMap} with the default key and
     * value
     * names.
     * 
     * @return the PrettyLinkedHashMap
     */
    public static <K, V> PrettyLinkedHashMap<K, V> newPrettyLinkedHashMap() {
        return new PrettyLinkedHashMap<K, V>(null, null);
    }

    /**
     * Return an empty TLinkedHashMap with the specified {@code keyName} and
     * {@code valueName}.
     * 
     * @param keyName
     * @param valueName
     * @return the PrettyLinkedHashMap.
     */
    public static <K, V> PrettyLinkedHashMap<K, V> newPrettyLinkedHashMap(
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
