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

import org.cinchapi.concourse.server.GlobalState;

/**
 * A collection of {@link String} related tools.
 * 
 * @author jnelson
 */
public final class StringTools {

	/**
	 * Return a copy of {@code string} with all of the stopwords removed. This
	 * method depends on the stopwords defined in {@link GlobalState#STOPWORDS}.
	 * 
	 * @param string
	 * @return A copy of {@code string} without stopwords
	 */
	public static String stripStopWords(String string) {
		String[] toks = string.split(" ");
		StringBuilder sb = new StringBuilder();
		for (String tok : toks) {
			if(!GlobalState.STOPWORDS.contains(tok)) {
				sb.append(tok);
				sb.append(" ");
			}
		}
		return sb.toString().trim();
	}

	private StringTools() {/* utility class */}

}
