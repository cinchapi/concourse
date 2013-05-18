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
import java.nio.channels.FileLock;

/**
 * A class that implements the {@link Persistable#writeTo(FileChannel)}
 * method.
 * 
 * @author jnelson
 */
public class Persistables {

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
		FileLock lock = channel.lock(channel.position(), obj.size(), false);
		channel.write(ByteBuffer.wrap(obj.getBytes()));
		lock.release();
	}
}