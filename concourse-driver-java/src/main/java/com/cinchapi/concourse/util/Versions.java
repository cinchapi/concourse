/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
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

import java.util.List;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;

/**
 * A collection of utility methods for dealing with version numbers.
 * 
 * @author Jeff Nelson
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
        for (int i = 0; i < parts.size(); ++i) {
            sum += parts.get(i) * Math.pow(10, (n * parts.size() - (i + 1)));
        }
        return sum;
    }

    private Versions() {/* noop */}
}
