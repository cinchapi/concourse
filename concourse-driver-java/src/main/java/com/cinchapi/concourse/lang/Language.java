/*
 * Copyright (c) 2013-2017 Cinchapi Inc.
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

import com.cinchapi.concourse.thrift.TCriteria;
import com.cinchapi.concourse.thrift.TSymbol;
import com.cinchapi.concourse.thrift.TSymbolType;
import com.google.common.collect.Lists;

/**
 * Tools for translating aspects of the language.
 * 
 * @author Jeff Nelson
 */
public final class Language {

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

    private Language() {/* noop */}

}
