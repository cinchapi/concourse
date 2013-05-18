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
 * An object that can encode its representation as a sequence of bytes.
 * <p>
 * This interface is designed to be a lightweight replacement for Serializable,
 * Externalizable and other third party serialization schemes with the following
 * advantages:
 * <ul>
 * <li>The implementing class has complete control over its serialization
 * format.</li>
 * <li>Faster serialization that does not rely on reflection.</li>
 * <li>No reliance on external dependencies (i.e. declaring the class schema or
 * serialization format in a file).</li>
 * <li>No requirement to have a public no-arg constructor which means immutable
 * classes can declare final fields!</li>
 * </ul>
 * </p>
 * <h2>Serialization</h2>
 * <p>
 * Write the bytes returned by {@link #getBytes()} to a file, stream, etc.
 * </p>
 * <h2>Deserialization</h2>
 * <p>
 * Implement a constructor that takes a sequence of bytes and reconstruct the
 * object by reading the bytes in the order of the scheme defined in
 * {@link #getBytes()}.
 * </p>
 * 
 * @author jnelson
 */
public interface Byteable {

	/**
	 * Returns the total number of bytes used to represent this object.
	 * 
	 * @return the number of bytes.
	 */
	public int size();

	/**
	 * Returns a byte sequence that represents this object.
	 * 
	 * @return the byte sequence.
	 */
	public byte[] getBytes();

}
