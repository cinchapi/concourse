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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * An object that can be written to a file channel.
 * 
 * @author jnelson
 */
public interface FileChannelPersistable extends ByteSized {

	/**
	 * Write the object to a writable <code>channel</code>. This method should
	 * acquire a lock over the region from the channels current position plus
	 * the {@link #size()} of the value. The caller is responsible for ensuring
	 * that the channel is in the correct position before and after this method
	 * has run. In general, call
	 * {@link FileChannelPersistable.Writer#write(FileChannelPersistable, FileChannel)}
	 * from this method.
	 * 
	 * @param channel
	 * @throws IOException
	 * @see {@link FileChannelPersistable.Writer#write(FileChannelPersistable, FileChannel)}
	 */
	public void writeTo(FileChannel channel) throws IOException;

	/**
	 * A class that implements the
	 * {@link FileChannelPersistable#writeTo(FileChannel)} method.
	 * 
	 * @author jnelson
	 */
	public class Writer {

		/**
		 * Write <code>obj</code> to <code>channel</code> in accordance with
		 * {@link FileChannelPersistable#writeTo(FileChannel)}.
		 * 
		 * @param obj
		 * @param channel
		 * @throws IOException
		 */
		public static void write(FileChannelPersistable obj, FileChannel channel)
				throws IOException {
			channel.lock(channel.position(), obj.size(), false);
			channel.write(ByteBuffer.wrap(obj.getBytes()));
		}
	}

}
