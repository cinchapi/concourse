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
package org.cinchapi.concourse.server.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

import org.cinchapi.concourse.server.io.Files;

import com.google.common.base.Throwables;

/**
 * A collection of utilities for handling binary files.
 * 
 * @author jnelson
 */
public final class BinaryFiles {

	/**
	 * Read bytes from {@code file} <em>sequentially</em> and return the content
	 * as a <strong>read only</strong> {@link ByteBuffer}.
	 * 
	 * @param file
	 * @return the read only ByteBuffer with the content of {@code file}
	 */
	public static ByteBuffer read(String file) {
		FileChannel channel = Files.getChannel(file);
		try {
			MappedByteBuffer data = channel.map(MapMode.READ_ONLY, 0,
					channel.size());
			return data;
		}
		catch (IOException e) {
			throw Throwables.propagate(e);
		}
		finally {
			Files.close(channel);
		}
	}

	private BinaryFiles() {/* Utility Class */}

}
