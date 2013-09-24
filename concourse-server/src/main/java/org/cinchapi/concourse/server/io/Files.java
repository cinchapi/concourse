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
package org.cinchapi.concourse.server.io;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.StandardCopyOption;

import com.google.common.base.Throwables;

/**
 * This class contains several utility methods for performing file based
 * operations without having to deal with the annoyance of checked exceptions.
 * Using this class will help produce more streamlined and readable code.
 * 
 * @author jnelson
 */
public abstract class Files {

	/**
	 * Copy the content of one file to another without throwing a checked
	 * exception.
	 * 
	 * @param from
	 * @param to
	 */
	public static void copy(String from, String to) {
		try {
			com.google.common.io.Files.copy(open(from), open(to));
		}
		catch (IOException e) {
			throw Throwables.propagate(e);
		}
	}

	/**
	 * Delete {@code file} without throwing a checked exception.
	 * 
	 * @param file
	 */
	public static void delete(String file) {
		try {
			File f = new File(file);
			java.nio.file.Files.delete(f.toPath());
		}
		catch (IOException e) {
			throw Throwables.propagate(e);
		}
	}

	public static boolean exists(String file) {
		return new File(file).exists();
	}

	/**
	 * Return a {@link File} object representing {@code file}. This method will
	 * automatically create {@code file} if it does not already exist.
	 * <p>
	 * <strong>NOTE:</strong> This method does not create parent directories if
	 * they do not exist. Call {@link #mkdirs(String)} for that functionality.
	 * </p>
	 * 
	 * @param file
	 * @return the File object for {@code file}
	 */
	public static File open(String file) {
		try {
			File f = new File(file);
			makeParentDirs(file);
			f.createNewFile();
			return f;
		}
		catch (IOException e) {
			System.out.println(e.getMessage());
			throw Throwables.propagate(e);
		}
	}

	/**
	 * Create the directories in {@link path}.
	 * 
	 * @param path
	 * @return {@code true} if the directories are created
	 */
	public static boolean mkdirs(String path) {
		return new File(path).mkdirs();
	}

	public static boolean makeParentDirs(String path) {
		File file = new File(path);
		File parent = file.getParentFile();
		return parent != null ? parent.mkdirs() : false;
	}

	/**
	 * Replace the content of {@code original} with that of {@code replacement}
	 * and delete {@code replacement} in a single atomic operation.
	 * 
	 * @param original
	 * @param replacement
	 */
	public static void replace(String original, String replacement) {
		try {
			java.nio.file.Files.move(new File(replacement).toPath(), new File(
					original).toPath(), StandardCopyOption.ATOMIC_MOVE,
					StandardCopyOption.REPLACE_EXISTING);
		}
		catch (IOException e) {
			throw Throwables.propagate(e);
		}
	}

	/**
	 * Return the random access {@link FileChannel} for {@code file}.
	 * 
	 * @param file
	 * @return the FileChannel for {@code file}
	 */
	public static FileChannel getChannel(String file) {
		try {
			return new RandomAccessFile(open(file), "rwd").getChannel();
		}
		catch (IOException e) {
			System.out.println(e.getMessage());
			throw Throwables.propagate(e);
		}
	}

	/**
	 * Return the length of {@code file}. This method will
	 * automatically create {@code file} if it does not already exist.
	 * 
	 * @param file
	 * @return the size in bytes
	 */
	public static long length(String file) {
		File f = open(file);
		return f.length();
	}

	/**
	 * Return a {@link MappedByteBuffer} for {@code file} in {@code mode}
	 * starting at {@code position} and continuing for {@code size} bytes. This
	 * method will automatically create {@code file} if it does not already
	 * exist.
	 * 
	 * @param file
	 * @param mode
	 * @param position
	 * @param size
	 * @return the MappedByteBuffer
	 */
	public static MappedByteBuffer map(String file, MapMode mode,
			long position, long size) {
		FileChannel channel = getChannel(file);
		try {
			return channel.map(mode, position, size);
		}
		catch (IOException e) {
			System.out.println(e.getMessage());
			throw Throwables.propagate(e);
		}
		finally{
			close(channel);
		}
	}
	
	public static void close(FileChannel channel){
		try {
			channel.close();
		}
		catch (IOException e) {
			throw Throwables.propagate(e);
		}
	}

}
