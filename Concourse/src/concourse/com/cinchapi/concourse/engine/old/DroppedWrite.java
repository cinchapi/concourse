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
package com.cinchapi.concourse.engine.old;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import com.cinchapi.concourse.exception.ConcourseRuntimeException;
import com.cinchapi.concourse.io.Persistable;

/**
 * A {@link Write} that is directly persistable to a file channel.
 * 
 * @author jnelson
 */
public class DroppedWrite extends Write implements Persistable {

	/**
	 * Return the {@link DroppedWrite} that is stored in {@code file}.
	 * 
	 * @param file
	 * @return the dropped write
	 */
	static DroppedWrite fromFile(File file) {
		try {
			ByteBuffer bytes = ByteBuffer.allocate((int) file.length());
			new FileInputStream(file).getChannel().read(bytes);
			bytes.rewind();
			Write write = Write.fromByteSequence(bytes);
			return new DroppedWrite(write, file);
		}
		catch (IOException e) {
			throw new ConcourseRuntimeException(e);
		}
	}

	private final transient File file;

	/**
	 * Construct a new instance and drop {@code write} to {@code file}.
	 * 
	 * @param write
	 * @param file
	 *            - the file into which {@code write} will be dropped. A
	 *            reference to this file <em>is not</em> maintained with the
	 *            dropped write object
	 * @see {@link Write#drop(Write, String)}
	 */
	protected DroppedWrite(Write write, String file) {
		super(write.getColumn(), write.getValue(), write.getRow(), write
				.getType());
		try {
			this.file = new File(file);
			this.file.delete(); // delete the file if it already exists
			this.file.getParentFile().mkdirs();
			this.file.createNewFile();
			writeTo(new FileOutputStream(this.file).getChannel());
		}
		catch (IOException e) {
			throw new ConcourseRuntimeException(e);
		}
	}

	/**
	 * Construct a new instance. This constructor assumes that {@code write} has
	 * already been dropped in {@code file} and therefore it only
	 * assigns a pointer to the file in memory.
	 * 
	 * @param write
	 */
	private DroppedWrite(Write write, File file) {
		super(write.getColumn(), write.getValue(), write.getRow(), write
				.getType());
		this.file = file;
	}

	@Override
	public void writeTo(FileChannel channel) throws IOException {
		Writer.write(this, channel);
	}

	/**
	 * Discard the dropped write by deleting the file that stores it.
	 */
	void discard() {
		file.delete();
	}

}
