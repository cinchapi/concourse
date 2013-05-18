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
 * Utility class for fixed size collections of {@link Byteable} objects.
 * 
 * @author jnelson
 */
public class FixedSizedByteableCollections {

	/**
	 * Return a byte array that represents the collection and conforms to the
	 * {@link FixedSizeByteableCollection} interface.
	 * 
	 * @param collection
	 * @param sizePerElement
	 * @return a byte array
	 */
	public static byte[] toByteArray(
			Collection<? extends Byteable> collection, int sizePerElement) {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		out.write(collection.size());
		out.write(sizePerElement);
		for (Byteable object : collection) {
			byte[] bytes = object.getBytes();
			try {
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
	 * {@link FixedSizeByteableCollection} interface.
	 * 
	 * @param collection
	 * @param sizePerElement
	 * @return a byte buffer
	 */
	public static ByteBuffer toByteBuffer(
			Collection<? extends Byteable> collection, int sizePerElement) {
		return ByteBuffer.wrap(toByteArray(collection, sizePerElement));
	}

}
