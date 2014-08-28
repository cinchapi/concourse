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
package org.cinchapi.concourse.lang;

import java.text.MessageFormat;
import java.util.List;

import org.cinchapi.concourse.thrift.TCriteria;
import org.cinchapi.concourse.thrift.TSymbol;
import org.cinchapi.concourse.thrift.TSymbolType;

import com.google.common.collect.Lists;

/**
 * Tools for translating aspects of the language.
 * 
 * @author jnelson
 */
public final class Translate {

    /**
     * Translate the {@link tsymbol} to its Java analog.
     * 
     * @param tsymbol
     * @return the analogous Symbol
     */
    public static Symbol fromThrift(TSymbol tsymbol) {
        if(tsymbol.getType() == TSymbolType.CONJUNCTION) {
            return ConjunctionSymbol.valueOf(tsymbol.getSymbol().toUpperCase());
        }
        else if(tsymbol.getType() == TSymbolType.KEY) {
            return KeySymbol.parse(tsymbol.getSymbol());
        }
        else if(tsymbol.getType() == TSymbolType.VALUE) {
            return ValueSymbol.parse(tsymbol.getSymbol());
        }
        else if(tsymbol.getType() == TSymbolType.PARENTHESIS) {
            return ParenthesisSymbol.parse(tsymbol.getSymbol());
        }
        else if(tsymbol.getType() == TSymbolType.OPERATOR) {
            return OperatorSymbol.parse(tsymbol.getSymbol());
        }
        else if(tsymbol.getType() == TSymbolType.TIMESTAMP) {
            return TimestampSymbol.parse(tsymbol.getSymbol());
        }
        else {
            throw new IllegalArgumentException("Unrecognized TSymbol "
                    + tsymbol);
        }
    }

    /**
     * Translate the {@code criteria} to its Thrift analog.
     * 
     * @param criteria
     * @return the analogous TCriteria
     */
    public static TCriteria toThrift(Criteria criteria) {
        List<TSymbol> symbols = Lists.newArrayList();
        for (Symbol symbol : criteria.getSymbols()) {
            symbols.add(toThrift(symbol));
        }
        return new TCriteria(symbols);
    }

    /**
     * Translate {@code symbol} to its Thrift analog.
     * 
     * @param symbol
     * @return The analogous TSymbol
     */
    public static TSymbol toThrift(Symbol symbol) {
        if(symbol.getClass() == ConjunctionSymbol.class) {
            return new TSymbol(TSymbolType.CONJUNCTION, symbol.toString());
        }
        else if(symbol.getClass() == KeySymbol.class) {
            return new TSymbol(TSymbolType.KEY, symbol.toString());
        }
        else if(symbol.getClass() == ValueSymbol.class) {
            return new TSymbol(TSymbolType.VALUE, escape(symbol.toString()));
        }
        else if(symbol.getClass() == ParenthesisSymbol.class) {
            return new TSymbol(TSymbolType.PARENTHESIS, symbol.toString());
        }
        else if(symbol.getClass() == OperatorSymbol.class) {
            return new TSymbol(TSymbolType.OPERATOR, symbol.toString());
        }
        else if(symbol.getClass() == TimestampSymbol.class) {
            return new TSymbol(TSymbolType.TIMESTAMP, symbol.toString());
        }
        else {
            throw new IllegalArgumentException(MessageFormat.format(
                    "Cannot translate {0} to Thrift", symbol));
        }
    }

    /**
     * Do any escaping of the {@code value} in order to preserve it during the
     * translation.
     * 
     * @param value
     * @return the escaped value
     */
    private static String escape(String value) {
        if(value.matches("`([^`]+)`")) { // CON-167: surround by quotes so the
                                         // backticks are not interpreted as
                                         // indicators of an encoded Tag. This
                                         // case would happen if the user
                                         // manually placed text wrapped in
                                         // backticks in the Criteria instead of
                                         // using the #Tag.create() method.
            return "\"" + value + "\"";
        }
        else {
            return value;
        }
    }

    private Translate() {/* noop */}

}
