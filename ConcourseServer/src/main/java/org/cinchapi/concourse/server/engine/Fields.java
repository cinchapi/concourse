/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013 Jeff Nelson, Cinchapi Software Collective
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
package org.cinchapi.concourse.server.engine;

import static org.mockito.Matchers.any;

import java.nio.ByteBuffer;

import org.cinchapi.common.annotate.PackagePrivate;
import org.cinchapi.common.io.ByteBufferOutputStream;
import org.cinchapi.common.io.Byteable;
import org.mockito.Mockito;

import com.google.common.collect.Lists;

/**
 * A collection of utility methods that are used in the {@link Field} class.
 * 
 * @author jnelson
 */
@PackagePrivate
abstract class Fields {

	/**
	 * Encode a Field identified by {@code key} with values belong to
	 * {@code valueClass} and {@code components} into a ByteBuffer. The encoded
	 * format is:
	 * <ol>
	 * <li>keyClassSize</li>
	 * <li>valueClassSize</li>
	 * <li>keySize</li>
	 * <li>component1Size</li>
	 * <li>...</li>
	 * <li>componentNSize</li>
	 * <li>keyClass</li>
	 * <li>valueClass</li>
	 * <li>key</li>
	 * <li>component1</li>
	 * <li>...</li>
	 * <li>componentN</li>
	 * </ol>
	 * 
	 * @param key
	 * @param valueClass
	 * @param components
	 * @return the encoded ByteBuffer
	 */
	@SafeVarargs
	public static <K extends Byteable, V extends Storable> ByteBuffer encodeAsByteBuffer(
			K key, Class<V> valueClass, Field<K, V>.Component... components) {
		ByteBufferOutputStream out = new ByteBufferOutputStream();

		// The key and value class names are stored in the Field so that the
		// components can be deserialized reflectively.
		Text keyClassName = Text.fromString(key.getClass().getName());
		Text valueClassName = valueClass == null ? Text.EMPTY : Text
				.fromString(valueClass.getName());

		// encode sizes
		out.write(keyClassName.size());
		out.write(valueClassName.size());
		out.write(key.size());
		for (Field<K, V>.Component component : components) {
			out.write(component.size());
		}

		// encode data
		out.write(keyClassName);
		out.write(valueClassName);
		out.write(key);
		for (Field<K, V>.Component component : components) {
			out.write(component);
		}
		out.close();
		return out.toByteBuffer();
	}

	/**
	 * Return a <em>mock</em> field of {@code type}. Use this method instead
	 * of mocking {@code type} directly to ensure that the mock is compatible
	 * with the assumptions made in {@link Record}.
	 * 
	 * @param type
	 * @return the {@code field}
	 */
	@SuppressWarnings("unchecked")
	public static <T extends Field<K, V>, K extends Byteable, V extends Storable> T mock(
			Class<T> type) {
		T field = Mockito.mock(type);
		Mockito.doReturn(Lists.<V>newArrayList()).when(field).getValues();
		Mockito.doNothing().when(field).add((V) any(Storable.class));
		Mockito.doNothing().when(field).remove((V) any(Storable.class));
		Mockito.doThrow(UnsupportedOperationException.class).when(field)
				.getBytes();
		Mockito.doThrow(UnsupportedOperationException.class).when(field).size();
		return field;

	}

}
