/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013-2015 Jeff Nelson, Cinchapi Software Collective
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

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link Reflection} utility class.
 * 
 * @author jnelson
 */
@SuppressWarnings("unused")
public class ReflectionTest {
    
    @Test
    public void testInheritedGetValueFromSuperClass(){
        int expected = Random.getInt();
        B b = new B(expected);
        Assert.assertEquals("default", Reflection.get("string", b));
    }
    
    @Test
    public void testCallSuperClassMethod(){
        B b = new B(Random.getInt());
        Assert.assertEquals("default", Reflection.call(b, "string"));
        Assert.assertEquals("defaultdefaultdefault", Reflection.call(b, "string", 3));
    }
    
    @Test
    public void testGetValueFromClassA(){
        String expected = Random.getString();
        A a = new A(expected);
        Assert.assertEquals(expected, Reflection.get("string", a));
    }
    
    @Test
    public void testCallMethodInClassA(){
        String expected = Random.getString();
        A a = new A(expected);
        Assert.assertEquals(expected, Reflection.call(a, "string"));
        Assert.assertEquals(expected+expected+expected, Reflection.call(a, "string", 3));
    }
    
    @Test
    public void testCallMethodInClassB(){
        int expected = Random.getInt();
        B b = new B(expected);
        Assert.assertEquals((long) (expected * 10), Reflection.call(b, "integer", 10));
        
    }
    
    @Test
    public void testGetValueFromClassB(){
        int expected = Random.getInt();
        B b = new B(expected);
        Assert.assertEquals(expected, Reflection.get("integer", b));
    }
    
    @Test(expected = RuntimeException.class)
    public void testAttemptToGetValueForNonExistingFieldThrowsException(){
        A a = new A(Random.getString());
        Reflection.get("foo", a);
    }
    
    private static class A {
        
        private final String string;
        
        public A(String string){
            this.string = string;
        }
        
        private String string(){
            return string;
        }
        
        private String string(int count){
            String result = "";
            for(int i = 0; i < count; i++){
                result+= string;
            }
            return result;
        }
    }
    
    private static class B extends A {
        
        private final int integer;
        
        public B(int integer){
            super("default");
            this.integer = integer;
        }
        
        private long integer(int multiple){
            return multiple * integer;
        }
    }

}
