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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;


/**
 * An object that can be written to a {@link FileChannel}.
 * 
 * @author jnelson
 */
public interface Persistable extends ByteSized {

	/**
	 * Encodes the object into a sequence for the purpose of writing the bytes
	 * to a {@link FileChannel}. This method is called from
	 * {@link Writer#write(Persistable, FileChannel)}.
	 */
	@Override
	public byte[] getBytes();

	/**
	 * Write the object to a writable {@code channel}. This method should
	 * acquire a lock over the region from the channels current position plus
	 * the {@link #size()} of the value. The caller is responsible for ensuring
	 * that the channel is in the correct position before and after this method
	 * has run. In general, call
	 * {@link Persistable.Writer#write(Persistable, FileChannel)} from this
	 * method.
	 * 
	 * @param channel
	 * @throws IOException
	 * @see {@link Persistable.Writer#write(Persistable, FileChannel)}
	 */
	public void writeTo(FileChannel channel) throws IOException;

	/**
	 * A class that implements the {@link Persistable#writeTo(FileChannel)}
	 * method.
	 * 
	 * @author jnelson
	 */
	public class Writer {

		/**
		 * Write {@code obj} to {@code channel} in accordance with
		 * {@link Persistable#writeTo(FileChannel)}.
		 * 
		 * @param obj
		 * @param channel
		 * @throws IOException
		 */
		public static void write(Persistable obj, FileChannel channel)
				throws IOException {
			channel.lock(channel.position(), obj.size(), false);
			channel.write(ByteBuffer.wrap(obj.getBytes()));
		}
	}

}
