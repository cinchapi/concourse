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


/**
 * An object that contains a collection of non-primitive variably sized objects
 * that can be encoded as a single byte sequence where each object is itself a
 * subsequence of bytes that is preceded by a 4 byte integer that specifies how
 * many bytes the object subsequence contains.
 * 
 * @author jnelson
 */
public interface ByteableCollection {

	/**
	 * Return a {@link ByteableCollectionIterator} over an array of {@code bytes}
	 * using {@link ByteableCollectionIterator#over(byte[])}.
	 * 
	 * @param bytes
	 * @return the iterator.
	 */
	public ByteableCollectionIterator iterator();
}
