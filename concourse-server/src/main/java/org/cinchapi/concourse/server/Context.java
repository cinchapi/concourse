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
package org.cinchapi.concourse.server;

/**
 * The {@link Context} contains global configuration and state that must be
 * accessible to various parts of the Server. A single Context is created in
 * {@link ConcourseServer} and passed to and around the Engine components}.
 * 
 * @author jnelson
 */
public final class Context {

	/**
	 * Return the current {@link Context}.
	 * 
	 * @return the Context
	 */
	public static Context getContext() {
		if(context == null) {
			context = new Context();
		}
		return context;
	}

	/**
	 * Enforces the singleton pattern.
	 */
	private static Context context = null;

	/**
	 * Construct a new instance.
	 */
	private Context() { /* restricted */
		// TODO do stuff like load prefs and setup bloom filters
	}

}
