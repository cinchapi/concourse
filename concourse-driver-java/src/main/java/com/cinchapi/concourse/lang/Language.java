/*
 * Copyright (c) 2013-2019 Cinchapi Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cinchapi.concourse.lang;

import java.text.MessageFormat;
import java.util.List;

import com.cinchapi.ccl.grammar.ConjunctionSymbol;
import com.cinchapi.ccl.grammar.KeySymbol;
import com.cinchapi.ccl.grammar.OperatorSymbol;
import com.cinchapi.ccl.grammar.ParenthesisSymbol;
import com.cinchapi.ccl.grammar.Symbol;
import com.cinchapi.ccl.grammar.TimestampSymbol;
import com.cinchapi.ccl.grammar.ValueSymbol;
import com.cinchapi.common.base.AnyStrings;
import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.concourse.thrift.TCriteria;
import com.cinchapi.concourse.thrift.TSymbol;
import com.cinchapi.concourse.thrift.TSymbolType;
import com.cinchapi.concourse.util.Convert;
import com.google.common.collect.Lists;

/**
 * Tools for translating aspects of the language.
 * 
 * @author Jeff Nelson
 */
public final class Language {

    /**
     * The character that indicates a String should be treated as a
     * {@link com.cinchapi.concourse.Tag}.
     */
    private static final char TAG_MARKER = Reflection.getStatic("TAG_MARKER",
            Convert.class); // (authorized)

    /**
     * Translate the {@link TSymbol} to its Java analog.
     * 
     * @param tsymbol
     * @return the analogous Symbol
     */
    public static Symbol translateFromThriftSymbol(TSymbol tsymbol) {
        if(tsymbol.getType() == TSymbolType.CONJUNCTION) {
            return ConjunctionSymbol.valueOf(tsymbol.getSymbol().toUpperCase());
        }
        else if(tsymbol.getType() == TSymbolType.KEY) {
            return new KeySymbol(tsymbol.getSymbol());
        }
        else if(tsymbol.getType() == TSymbolType.VALUE) {
            Object symbol = Convert.stringToJava(tsymbol.getSymbol());
            if(symbol instanceof String && !symbol.equals(tsymbol.getSymbol())
                    && AnyStrings.isWithinQuotes(tsymbol.getSymbol(),
                            TAG_MARKER)) {
                // CON-634: This is an obscure corner case where the surrounding
                // quotes on the original tsymbol were necessary to escape a
                // keyword, but got dropped because of the logic in
                // Convert#stringToJava
                symbol = AnyStrings.ensureWithinQuotes(symbol.toString());
            }
            return new ValueSymbol(symbol);
        }
        else if(tsymbol.getType() == TSymbolType.PARENTHESIS) {
            return ParenthesisSymbol.parse(tsymbol.getSymbol());
        }
        else if(tsymbol.getType() == TSymbolType.OPERATOR) {
            return new OperatorSymbol(
                    Convert.stringToOperator(tsymbol.getSymbol()));
        }
        else if(tsymbol.getType() == TSymbolType.TIMESTAMP) {
            // NOTE: This depends on knowledge that TimestampSymbol#toString in
            // the ccl library prepends "at " before the microseconds. This is
            // brittle and we need a better solution in case the ccl library
            // changes the toString format.
            long micros = Long
                    .parseLong(tsymbol.getSymbol().replace("at ", ""));
            return new TimestampSymbol(micros);
        }
        else {
            throw new IllegalArgumentException(
                    "Unrecognized TSymbol " + tsymbol);
        }
    }

    /**
     * Translate the {@code criteria} to its Thrift analog.
     * 
     * @param criteria
     * @return the analogous TCriteria
     */
    public static TCriteria translateToThriftCriteria(Criteria criteria) {
        List<TSymbol> symbols = Lists.newArrayList();
        for (Symbol symbol : criteria.getSymbols()) {
            symbols.add(translateToThriftSymbol(symbol));
        }
        return new TCriteria(symbols);
    }

    /**
     * Translate the {@code tcriteria} to its Java analog.
     * 
     * @param tcriteria
     * @return the analogous Java {@link Criteria}
     */
    public static Criteria translateFromThriftCriteria(TCriteria tcriteria) {
        Criteria criteria = new Criteria();
        for (TSymbol tsymbol : tcriteria.getSymbols()) {
            criteria.add(translateFromThriftSymbol(tsymbol));
        }
        return criteria;
    }

    /**
     * Translate {@code symbol} to its Thrift analog.
     * 
     * @param symbol
     * @return The analogous TSymbol
     */
    public static TSymbol translateToThriftSymbol(Symbol symbol) {
        if(symbol.getClass() == ConjunctionSymbol.class) {
            return new TSymbol(TSymbolType.CONJUNCTION, symbol.toString());
        }
        else if(symbol.getClass() == KeySymbol.class) {
            return new TSymbol(TSymbolType.KEY, symbol.toString());
        }
        else if(symbol.getClass() == ValueSymbol.class) {
            return new TSymbol(TSymbolType.VALUE, symbol.toString());
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
            throw new IllegalArgumentException(MessageFormat
                    .format("Cannot translate {0} to Thrift", symbol));
        }
    }

    private Language() {/* noop */}

}
