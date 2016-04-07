/*
 * Copyright (c) 2013-2016 Cinchapi Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cinchapi.concourse.util;

/**
 * A utility class that assists with extracting information about the underlying
 * platform (i.e. the operating system, architecture, etc)
 * 
 * @author Jeff Nelson
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
