/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013-2015 Jeff Nelson, Cinchapi Software Collective
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
package org.cinchapi.concourse.util;

/**
 * A utility class that assists with extracting information about the underlying
 * platform (i.e. the operating system, architecture, etc)
 * 
 * @author jnelson
 */
public final class Platform {
	
	/**
	 * The name of the operating system on which the current JVM is running.
	 */
	public static String OPERATING_SYSTEM = System.getProperty("os.name");
	
	/**
	 * Return {@code true} if current platform is Windows based.
	 * @return {@code true} if this is Windows
	 */
	public static boolean isWindows(){
		return OPERATING_SYSTEM.toLowerCase().contains("windows");
	}
	
	/**
	 * Return {@code true} if current platform is OS X based.
	 * @return {@code true} if this is OS X
	 */
	public static boolean isMacOsX(){
		return OPERATING_SYSTEM.toLowerCase().contains("os x");
	}
	
	/**
	 * Return {@code true} if current platform is Linux based.
	 * @return {@code true} if this is Linux
	 */
	public static boolean isLinux(){
		String os = OPERATING_SYSTEM.toLowerCase();
		return os.contains("nix") || os.contains("nux") || os.contains("aix");
	}
	
	/**
	 * Return {@code true} if current platform is Solaris based.
	 * @return {@code true} if this is Solaris
	 */
	public static boolean isSolaris(){
		return OPERATING_SYSTEM.toLowerCase().contains("solaris");
	}
	
	private Platform() {/* noop */
	}

}
