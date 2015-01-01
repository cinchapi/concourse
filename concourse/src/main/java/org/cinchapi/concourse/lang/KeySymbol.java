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
package org.cinchapi.concourse.lang;

/**
 * A {@link Symbol} that represents a key in a {@link Criteria}.
 * 
 * @author jnelson
 */
public class KeySymbol extends AbstractSymbol {

    /**
     * Create a new {@link KeySymbol} for the given {@code key}.
     * 
     * @param key
     * @return the symbol
     */
    public static KeySymbol create(String key) {
        return new KeySymbol(key);
    }

    /**
     * Parse a {@link KeySymbol} from the given {@code string}.
     * 
     * @param string
     * @return the symbol
     */
    public static KeySymbol parse(String string) {
        return new KeySymbol(string);
    }

    /**
     * The associated key.
     */
    private final String key;

    /**
     * Construct a new instance.
     * 
     * @param key
     */
    private KeySymbol(String key) {
        this.key = key;
    }

    /**
     * Return the key associated with this {@link Symbol}.
     * 
     * @return the key
     */
    public String getKey() {
        return key;
    }

    @Override
    public String toString() {
        return key;
    }

}
