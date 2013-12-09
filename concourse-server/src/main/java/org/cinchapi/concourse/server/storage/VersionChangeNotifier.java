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
package org.cinchapi.concourse.server.storage;

import org.cinchapi.concourse.server.concurrent.Token;

/**
 * An object that notifies listeners about a version change so that they can
 * respond appropriately.
 * 
 * @author jnelson
 */
public interface VersionChangeNotifier {

    /**
     * Add {@code listener} to the list to be notified about version changes for
     * {@code token}.
     * 
     * @param token
     * @param listener
     */
    public void addVersionChangeListener(Token token,
            VersionChangeListener listener);

    /**
     * Remove {@code listener} from the list to be notified about version
     * changes for {@code token}.
     * 
     * @param token
     * @param listener
     */
    public void removeVersionChangeListener(Token token,
            VersionChangeListener listener);

    /**
     * Notify all relevant listeners about a version change for {@code token}.
     * 
     * @param token
     */
    public void notifyVersionChange(Token token);

}
