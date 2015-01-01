/*
 * The MIT License (MIT)
 * 
 * 2013-2015 Jeff Nelson, Cinchapi Software Collective
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

import com.google.common.base.Preconditions;

/**
 * Utilities for Lists.
 * 
 * @author jnelson
 */
public final class TLists {

    /**
     * Modify each list to retain only the elements that are contained in all of
     * the lists.
     * 
     * @param lists
     */
    @SafeVarargs
    public static void retainIntersection(List<?>... lists) {
        Preconditions.checkArgument(lists.length > 0);
        List<?> intersection = lists[0];
        for (int i = 1; i < lists.length; ++i) {
            intersection.retainAll(lists[i]);
        }
        for (int i = 1; i < lists.length; ++i) {
            lists[i].retainAll(intersection);
        }
    }

    private TLists() {/* noop */}

}
