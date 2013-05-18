/*
 * This project is free software: you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 * 
 * This project is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this project. If not, see <http://www.gnu.org/licenses/>.
 */
package com.cinchapi.concourse.io;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;

/**
 * This class contains utilities for encoding collections of {@link Byteable}
 * objects into a {@link ByteableCollection} compatible byte array or byte
 * buffer.
 * 
 * @author jnelson
 */
public class ByteableCollections {

	/**
	 * Return a byte array that represents the collection and conforms to the
	 * {@link ByteableCollection} interface.
	 * 
	 * @param collection
	 * @return a byte array
	 */
	public static byte[] toByteArray(Collection<? extends Byteable> collection) {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		for (Byteable object : collection) {
			byte[] bytes = object.getBytes();
			try {
				// for some reason writing the length of the array doesn't work
				// properly, so it must be wrapped in a byte buffer :-/
				out.write(ByteBuffer.allocate(4).putInt(bytes.length).array());
				out.write(bytes);
			}
			catch (IOException e) {
				e.printStackTrace();
			}
		}
		return out.toByteArray();
	}

	/**
	 * Return a byte buffer that represents the collection and conforms to the
	 * {@link ByteableCollection} interface.
	 * 
	 * @param collection
	 * @return a byte buffer
	 */
	public static ByteBuffer toByteBuffer(
			Collection<? extends Byteable> collection) {
		return ByteBuffer.wrap(toByteArray(collection));
	}

}
