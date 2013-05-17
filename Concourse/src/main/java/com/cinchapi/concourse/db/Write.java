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
package com.cinchapi.concourse.db;

import java.nio.ByteBuffer;

import javax.annotation.concurrent.Immutable;

import com.cinchapi.concourse.io.ByteSized;
import com.google.common.base.Objects;

/**
 * <p>
 * A Write is used for representing and transporting data from one
 * solipsistic<sup>1</sup> context to another by associating the relevant
 * record, field, value and operation.
 * </p>
 * <sup>1</sup> - A solipsistic context relies solely on its knowledge of data
 * to answer reads. Concourse stores data in several different contexts (i.e.
 * journals, databases, nodes, etc), and the engine reconciles answers from each
 * during a read operation.
 * 
 * @author jnelson
 */
@Immutable
class Write implements ByteSized, Containable {

	private static final int FIXED_SIZE_IN_BYTES = 3 * (Integer.SIZE / 8); // operation,
																			// fieldSize,
																			// valueSize

	private final PrimaryKey key;
	private final SuperString field;
	private final Value value;
	private final Operation operation;
	private transient int hashCode = 0;
	private transient ByteBuffer buffer = null; // initialize lazily

	/**
	 * Construct a new instance.
	 * 
	 * @param key
	 * @param field
	 * @param value
	 * @param operation
	 */
	protected Write(PrimaryKey key, SuperString field, Value value,
			Operation operation) {
		this.key = key;
		this.field = field;
		this.value = value;
		this.operation = operation;
	}

	/**
	 * This method does not use the {@link #operation} type to determine
	 * equality so that ALL writes to the same bucket slot are considered equal.
	 * For equality that factors in the operation type, use the {@link #matches}
	 * method.
	 */
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof Write) {
			Write other = (Write) obj;
			return Objects.equal(key, other.key)
					&& Objects.equal(field, other.field)
					&& Objects.equal(value, other.value);
		}
		return false;
	}

	@Override
	public byte[] getBytes() {
		return getBuffer().array();
	}

	@Override
	public long getTimestamp() {
		return value.getTimestamp();
	}

	@Override
	public int hashCode() {
		// NOTE: I do not factor in {@link #operation} because I want ALL writes
		// to the same bucket slot to hash to the same place.
		if(hashCode == 0) {
			hashCode = Objects.hashCode(key, field, value);
		}
		return hashCode;
	}

	@Override
	public boolean isForStorage() {
		return Containables.isForStorage(this);
	}

	@Override
	public boolean isNotForStorage() {
		return Containables.isNotForStorage(this);
	}

	/**
	 * Return {@code true} if this write is equal to {@code other} and also has
	 * the same {@code operation} type.
	 * 
	 * @param other
	 * @return {@code true} if the writes match each other
	 */
	public boolean matches(Write other) {
		return equals(other) && operation == other.operation;
	}

	@Override
	public int size() {
		return FIXED_SIZE_IN_BYTES + key.size() + field.size() + value.size();
	}

	/**
	 * <p>
	 * Rewind and return {@link #buffer}. Use this method instead of accessing
	 * the variable directly to ensure that it is rewound.
	 * </p>
	 * <p>
	 * The buffer is encoded with the following order:
	 * <ol>
	 * <li><strong>key</strong> - first 16 bytes</li>
	 * <li><strong>fieldSize</strong> - next 4 bytes</li>
	 * <li><strong>field</strong> - next fieldSize bytes</li>
	 * <li><strong>valueSize</strong> - next 4 bytes</li>
	 * <li><strong>value</strong> - next valueSize bytes</li>
	 * <li><strong>operation</strong> - remaining 4 bytes</li>
	 * </ol>
	 * </p>
	 * 
	 * @return the internal byte buffer representation
	 */
	private ByteBuffer getBuffer() {
		// NOTE: A copy of the buffer is not made for performance/space reasons.
		// I am okay with this because {@link #buffer} is only used internally.
		if(buffer == null) {
			buffer = ByteBuffer.allocate(size());
			buffer.put(key.getBytes());
			buffer.putInt(field.size());
			buffer.put(field.getBytes());
			buffer.putInt(value.size());
			buffer.put(value.getBytes());
			buffer.putInt(operation.ordinal());
		}
		buffer.rewind();
		return buffer;
	}

}
