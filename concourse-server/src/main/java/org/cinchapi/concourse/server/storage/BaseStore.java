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
package org.cinchapi.concourse.server.storage;

import java.util.Set;

import org.cinchapi.concourse.Link;
import org.cinchapi.concourse.thrift.Operator;
import org.cinchapi.concourse.thrift.TObject;
import org.cinchapi.concourse.util.Convert;

/**
 * The {@link Store} that provides basic functionality to all of its children.
 * 
 * @author jnelson
 */
public abstract class BaseStore implements Store {

	
	/**
	 * doFind {@code key} {@code operator} {@code values} at {@code timestamp}
	 * <p>
	 * Subclasses can implement find {@code key} {@code operator} {@code values } {@code timestamp} 
	 * without doing any normalization
	 * <p>
	 *  @param timestamp
	 * @param key
	 * @param operator
	 * @param values
	 */
	protected abstract Set<Long> doFind(long timestamp, String key, Operator operator, TObject...values);
	
	/**
	 * doFind {@code key} {@code operator} {@code values} 
	 * <p>
	 * Subclasses can implement find {@code key} {@code operator} {@code values }  
	 * without doing any normalization
	 * <p>
	 * @param key
	 * @param operator
	 * @param values
	 */
	protected abstract Set<Long> doFind(String key, Operator operator, TObject...values);
	
	
	@Override
    public final Set<String> describe(long record) {
        return browse(record).keySet();
    }


    @Override
    public final Set<String> describe(long record, long timestamp) {
        return browse(record, timestamp).keySet();
    }
    
    @Override
    public final Set<Long> find(long timestamp, String key, Operator operator,
            TObject... values){
    	for(int i=0;i<values.length;i++){
    		values[i] = normalize(operator,values[i]);
    	}
    	operator = normalize(operator);
    	return doFind(timestamp, key, operator, values);
    }
    
    @Override
    public final Set<Long> find(String key, Operator operator, TObject... values){
    	for(int i=0;i<values.length;i++){
    		values[i] = normalize(operator,values[i]);
    	}
    	operator = normalize(operator);
    	return doFind(key,operator,values);
    }
    /**
     * Perform any necessary normalization on all the parameters IN PLACE (e.g. convert a utility operator to a functional one, etc)
     * @param operator
     * @param values
     */
    protected static TObject normalize(Operator operator, TObject value){//viisible for testing
    	if(operator == Operator.LINKS_TO){
    		return Convert.javaToThrift(Link.to(((Number)Convert.thriftToJava(value)).longValue()));
    	}
    	return value;
    }
    
    /**
     * If Operator is LINKS_TO normalize it to EQUALS
     * @param operator
     * @return
     */
    protected static Operator normalize(Operator operator){
    	if(operator == Operator.LINKS_TO){
    		operator = Operator.EQUALS;
    	}
    	return operator;
    }

}
