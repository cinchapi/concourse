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

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import javax.annotation.Nullable;

import com.google.common.base.Throwables;

/**
 * A collection of tools for using reflection to access or modify objects. Use
 * with caution.
 * 
 * @author jnelson
 */
public final class Reflection {

    /**
     * Use reflection to get the value of {@code variableName} from {@code obj}.
     * This is useful in situations when it is necessary to access an instance
     * variable that is out of scope.
     * 
     * @param variableName
     * @param obj
     * @return the value of {@code variableName} in {@code obj} if it exists.
     */
    @Nullable
    @SuppressWarnings("unchecked")
    public static <T> T get(String variableName, Object obj) {
        try {
            Class<?> clazz = obj.getClass();
            Field field = null;
            while (clazz != null && field == null) {
                try {
                    field = clazz.getDeclaredField(variableName);
                }
                catch (NoSuchFieldException e) { // check the parent to see if
                                                 // the field was defined there
                    clazz = clazz.getSuperclass();
                }
            }
            if(field != null) {
                field.setAccessible(true);
                return (T) field.get(obj);
            }
            else {
                throw new NoSuchFieldException();
            }
        }
        catch (ReflectiveOperationException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Return the alternative class for the input {@code clazz}. This is usually
     * a class that represents a primitive for an autoboxed wrapper class.
     * Otherwise, the input {@code clazz} is returned.
     * 
     * @param clazz
     * @return the alt class
     */
    private static Class<?> getAltClass(Class<?> clazz) {
        if(clazz == Integer.class) {
            return int.class;
        }
        else if(clazz == Long.class) {
            return long.class;
        }
        else if(clazz == Byte.class) {
            return byte.class;
        }
        else if(clazz == Short.class) {
            return short.class;
        }
        else if(clazz == Float.class) {
            return float.class;
        }
        else if(clazz == Double.class) {
            return double.class;
        }
        else if(clazz == Boolean.class) {
            return boolean.class;
        }
        else if(clazz == Character.class) {
            return char.class;
        }
        else {
            return clazz;
        }
    }

    /**
     * Use reflection to call an instance method on {@code obj} with the
     * specified {@code args}.
     * 
     * @param obj
     * @param methodName
     * @param args
     * @return the result of calling the method
     */
    @SuppressWarnings("unchecked")
    public static <T> T call(Object obj, String methodName, Object... args) {
        // TODO cache method instances
        try {
            Class<?> clazz = obj.getClass();
            Class<?>[] parameterTypes = new Class<?>[args.length];
            Class<?>[] altParameterTypes = new Class<?>[args.length];
            for (int i = 0; i < args.length; i++) {
                parameterTypes[i] = args[i].getClass();
                altParameterTypes[i] = getAltClass(args[i].getClass());
            }
            Method method = null;
            while (clazz != null && method == null) {
                try {
                    method = clazz
                            .getDeclaredMethod(methodName, parameterTypes);
                }
                catch (NoSuchMethodException e) {
                    try {
                        // Attempt to find a method using the alt param types.
                        // This will usually bear fruit in cases where a method
                        // has a primitive type parameter and Java autoboxing
                        // causes the passed in parameters to have a wrapper
                        // type instead of the appropriate primitive type.
                        method = clazz.getDeclaredMethod(methodName,
                                altParameterTypes);
                    }
                    catch (NoSuchMethodException e2) {
                        clazz = clazz.getSuperclass();
                    }
                }
            }
            if(method != null) {
                method.setAccessible(true);
                return (T) method.invoke(obj, args);
            }
            else {
                throw new NoSuchMethodException();
            }
        }
        catch (ReflectiveOperationException e) {
            throw Throwables.propagate(e);
        }
    }

    private Reflection() {/* noop */}

}
