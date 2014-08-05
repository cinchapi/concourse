/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2014 Jeff Nelson, Cinchapi Software Collective
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

import java.util.List;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;

/**
 * A collection of utility methods for dealing with version numbers.
 * 
 * @author jnelson
 */
public final class Versions {

    /**
     * Convert a numerical version in decimal form (i.e. 0.4.1) to a long
     * that respects natural version number ordering. For example, the long
     * conversion for 0.5.0 is guaranteed to be greater than the long
     * conversion for 0.4.1.
     * 
     * @param version
     * @return the long representation of the version
     */
    public static long toLongRepresentation(String version) {
        return toLongRepresentation(version, 3); // this should be safe
                                                 // right?, no version number
                                                 // component can possibly be
                                                 // larger than 999?
    }

    /**
     * Convert a numerical version in decimal form (i.e. 0.4.1) to a long
     * that respects natural version number ordering. For example, the long
     * conversion for 0.5.0 is guaranteed to be greater than the long
     * conversion for 0.4.1.
     * <p>
     * The {@code maxComponentLength} parameter is used to ensure that each
     * component of the version has the necessary padding. You should specify
     * this value to be the largest possible length of a version number
     * component in your scheme (i.e. for version A.B.C if component C may
     * possible go up as high as 999, then you should specify 3 for the
     * maxComponentLength value).
     * </p>
     * <p>
     * <strong>Warning:</strong>If one or more of the component lengths is
     * greater than {@code maxComponentLength} then this method will use the
     * larger component length in its calculation. This may potentially lead to
     * unexpected results, so its best to specify the {@code maxComponentLength}
     * to be sufficiently large.
     * </p>
     * 
     * @param version
     * @param maxComponentLength
     * @return the long representation of the version
     */
    public static long toLongRepresentation(String version,
            int maxComponentLength) {
        String[] toks = version.split("\\.");
        int n = maxComponentLength;
        List<Integer> parts = Lists.newArrayList();

        // figure out max component length
        for (String tok : toks) {
            n = Math.max(n, tok.length());
        }

        // do any padding and parse to int
        for (String tok : toks) {
            parts.add(Integer.parseInt(Strings.padStart(tok, n,
                    Character.forDigit(0, 10))));
        }
        long sum = 0;
        for (int i = 0; i < parts.size(); i++) {
            sum += parts.get(i) * Math.pow(10, (n * parts.size() - (i + 1)));
        }
        return sum;
    }

    private Versions() {/* noop */}
}
