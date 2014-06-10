/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013-2014 Jeff Nelson, Cinchapi Software Collective
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
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

import org.cinchapi.concourse.util.Logger;

import com.google.common.base.Throwables;

/**
 * Interface to the underlying filesystem which provides methods to perform file
 * based operations without having to deal with the annoyance of checked
 * exceptions. Using this class will help produce more streamlined and readable
 * code.
 * 
 * @author jnelson
 */
public final class FileSystem {

    /**
     * Close {@code channel}.
     * 
     * @param channel
     */
    public static void closeFileChannel(FileChannel channel) {
        try {
            channel.close();
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Copy all the bytes {@code from} one file to {to} another.
     * 
     * @param from
     * @param to
     */
    public static void copyBytes(String from, String to) {
        try {
            Files.copy(Paths.get(from), Files.newOutputStream(Paths.get(to)));
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Delete {@code directory}
     * 
     * @param path
     */
    public static void deleteDirectory(String directory) {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(Paths
                .get(directory))) {
            for (Path path : stream) {
                if(Files.isDirectory(path)) {
                    deleteDirectory(path.toString());
                }
                else {
                    Files.delete(path);
                }
            }
            Files.delete(Paths.get(directory));
        }
        catch (IOException e) {
            if(e.getClass() == DirectoryNotEmptyException.class) {
                Logger.warn("It appears that data was added to directory "
                        + "{} while trying to perform a deletion. "
                        + "Trying again...", directory);
                deleteDirectory(directory);
            }
            else {
                throw Throwables.propagate(e);
            }
        }
    }

    /**
     * Delete {@code file}.
     * 
     * @param file
     */
    public static void deleteFile(String file) {
        try {
            java.nio.file.Files.delete(Paths.get(file));
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
    public static FileChannel getFileChannel(String file) {
        try {
            return new RandomAccessFile(openFile(file), "rwd").getChannel();
        }
        catch (IOException e) {
            System.out.println(e.getMessage());
            throw Throwables.propagate(e);
        }
    }

    /**
     * Return the size of {@code file}. This method will automatically create
     * {@code file} if it does not already exist.
     * 
     * @param file
     * @return the size in bytes
     */
    public static long getFileSize(String file) {
        try {
            openFile(file);
            return Files.size(Paths.get(file));
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Return the simple filename without path information or extension. This
     * method assumes that the filename only contains one extension.
     * 
     * @param filename
     * @return the simple file name
     */
    public static String getSimpleName(String filename) {
        String[] placeholder;
        return (placeholder = (placeholder = filename.split("\\."))[placeholder.length - 2]
                .split(File.separator))[placeholder.length - 1];
    }

    /**
     * Return {@code true} in the filesystem contains {@code dir} and it is
     * a directory.
     * 
     * @param file
     * @return {@code true} if {@code dir} exists
     */
    public static boolean hasDir(String dir) {
        Path path = Paths.get(dir);
        return Files.exists(path) && Files.isDirectory(path);
    }

    /**
     * Return {@code true} in the filesystem contains {@code file} and it is not
     * a directory.
     * 
     * @param file
     * @return {@code true} if {@code file} exists
     */
    public static boolean hasFile(String file) {
        Path path = Paths.get(file);
        return Files.exists(path) && !Files.isDirectory(path);
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
        FileChannel channel = getFileChannel(file);
        try {
            return channel.map(mode, position, size);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
        finally {
            closeFileChannel(channel);
        }
    }

    /**
     * Create the directories in {@link path}.
     * 
     * @param path
     */
    public static void mkdirs(String path) {
        try {
            Files.createDirectories(Paths.get(path));
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Open {@code file} and return a {@link File} handle. This method will
     * create a new file if and only if it does not already exist.
     * 
     * @param file
     */
    public static File openFile(String file) {
        try {
            File f = new File(file);
            if(f.getParentFile() != null) {
                f.getParentFile().mkdirs();
            }
            f.createNewFile();
            return f;
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Read bytes from {@code file} <em>sequentially</em> and return the content
     * as a <strong>read only</strong> {@link ByteBuffer}.
     * 
     * @param file
     * @return the read only ByteBuffer with the content of {@code file}
     */
    public static ByteBuffer readBytes(String file) {
        FileChannel channel = getFileChannel(file);
        try {
            MappedByteBuffer data = channel.map(MapMode.READ_ONLY, 0,
                    channel.size());
            return data;
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
        finally {
            closeFileChannel(channel);
        }
    }

    /**
     * Replace the content of {@code original} with that of {@code replacement}
     * and delete {@code replacement} in a single atomic operation.
     * 
     * @param original
     * @param replacement
     */
    public static void replaceFile(String original, String replacement) {
        try {
            java.nio.file.Files.move(Paths.get(replacement),
                    Paths.get(original), StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    private FileSystem() {}

}
