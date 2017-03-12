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
package com.cinchapi.concourse.server.storage;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

import com.cinchapi.concourse.Link;
import com.cinchapi.concourse.server.storage.Stores;
import com.cinchapi.concourse.thrift.Operator;
import com.cinchapi.concourse.thrift.TObject;
import com.cinchapi.concourse.util.Convert;
import com.cinchapi.concourse.util.TestData;

/**
 * Unit tests for the {@link Stores} utilities.
 * 
 * @author Jeff Nelson
 */
@RunWith(Theories.class)
public class StoresTest {

    @DataPoints
    public static Operator[] operators = Operator.values();

    @Test
    @Theory
    public void testNormalizeOperator(Operator operator) {
        Operator expected = null;
        switch (operator) {
        case LIKE:
            expected = Operator.REGEX;
        case NOT_LIKE:
            expected = Operator.NOT_REGEX;
        case LINKS_TO:
            operator = Operator.EQUALS;
        default:
            expected = operator;
        }
        Assert.assertEquals(expected, Stores.normalizeOperator(operator));
    }

    @Test
    @Theory
    public void testNormalizeValue(Operator operator) {
        long num =  TestData.getLong();
        Object value=null;
        Object expected=null;
        switch(operator){
        case REGEX :
        case NOT_REGEX :
        	value=putNumberWithinPercentSign(num);
        	expected=putNumberWithinStarSign(num);
        	break;
        case LINKS_TO : 
        	value=num;
        	expected=Link.to(num);
        	break;
        default : 
        	value=num;
        	expected=num;
        	break;
        }
        Assert.assertEquals(Convert.javaToThrift(expected),
                Stores.normalizeValue(operator, Convert.javaToThrift(value)));
    }

    @Test
    public void testNormalizeLinksToNotLong() {
        TObject value = Convert.javaToThrift(TestData.getString());
        Assert.assertEquals(value,
                Stores.normalizeValue(Operator.LINKS_TO, value));
    }
    
    /**
     * This method will convert {@link long} into String. It will put % (percent) Sign at the both
     * end and \\% in the middle of {@link String}. 
     * 
     * @param num
     * @return {@link String}
     */
    private String putNumberWithinPercentSign(long num){
    	String str = String.valueOf(num);
    	return "%"+str.substring(0, str.length()/2)+"\\%"+str.substring(str.length()/2, str.length())+"%";    	
    }
    
    /**
     * This method will convert {@link long} into {@link String}. It will put * (percent) sign at the both
     * end and % in the middle of  {@link String}.
     * 
     * @param num
     * @return {@link String}
     */
    private String putNumberWithinStarSign(long num){
    	String str=String.valueOf(num);
    	return ".*"+str.substring(0, str.length()/2)+"%"+str.substring(str.length()/2, str.length())+".*";    	
    }

}
