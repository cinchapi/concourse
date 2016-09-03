/*
 * Copyright (c) 2013-2016 Cinchapi Inc.
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
package com.cinchapi.concourse.util;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.Function;

import javax.annotation.Nullable;

import com.cinchapi.common.base.CheckedExceptions;

/**
 * A collection of tools for using reflection to access or modify objects.
 * <strong>Use with caution.</strong>
 * <p>
 * <em>This class helps you do some naughty things. Think carefully about whether
 * you should be using reflection in your application (its usage is usually a
 * sign of broken design, but there are some legitimate cases where its
 * necessary).</em>
 * </p>
 * <p>
 * Warning aside, if you're going to do reflection anyway, these functions
 * abstract away a lot of the bolierplate. They allow you to access variables,
 * constructors and methods no matter what visibility they are declared with and
 * no matter how far they are declared in the object hirearchy.
 * </p>
 * <h2>Known Limitations</h2>
 * <p>
 * <ul>
 * <li>Calling methods and constructors that take a mix of primitive types and
 * wrapper types does not work. Methods should be declared to take all of one or
 * the other.</li>
 * </ul>
 * </p>
 * 
 * @author Jeff Nelson
 */
public final class Reflection {

    /**
     * Use reflection to call an instance method on {@code obj} with the
     * specified {@code args}.
     * 
     * @param obj
     * @param methodName
     * @param args
     * @return the result of calling the method
     */
    public static <T> T call(Object obj, String methodName, Object... args) {
        return call(true, obj, methodName, args);
    }

    /**
     * Return a {@link Callable} that can execute
     * {@link #call(Object, String, Object...)} asynchronously.
     * 
     * @param obj the object on which to call the method
     * @param methodName the name of the method to call
     * @param args the method parameters
     * @return a {@link Callable} that can be used for asynchronous execution
     */
    public static <T> Callable<T> callable(final Object obj,
            final String methodName, final Object... args) {
        return new Callable<T>() {

            @Override
            public T call() throws Exception {
                return Reflection.call(obj, methodName, args);
            }

        };
    }

    /**
     * Call {@code methodName} on the specified {@code obj} with the specified
     * {@code args} if and only if the {@code evaluate} function returns
     * {@code true}.
     * 
     * @param evaluate the {@link Function} that is given the (possibly cached)
     *            {@link Method} instance that corresponds to {@code methodName}
     *            ; use this to evaluate whether the method should be called
     * @param obj the Object on which the method is called
     * @param methodName the name of the method to call
     * @param args the args to pass to the method
     * @return the result of calling the method
     */
    @SuppressWarnings("unchecked")
    public static <T> T callIf(Function<Method, Boolean> evaluate, Object obj,
            String methodName, Object... args) {
        Method method = getMethod(false, methodName, obj.getClass(), args);
        if(evaluate.apply(method)) {
            try {
                method.setAccessible(true);
                return (T) method.invoke(obj, args);
            }
            catch (ReflectiveOperationException e) {
                throw CheckedExceptions.throwAsRuntimeException(e);
            }
        }
        else {
            throw new IllegalStateException("Cannot call " + method
                    + " reflectively because "
                    + "the evaluation function returned false");
        }
    }

    /**
     * Use reflection to call an instance method on {@code obj} with the
     * specified {@code args} if and only if that method is natively accessible
     * according to java language access rules.
     * 
     * @param obj
     * @param methodName
     * @param args
     * @return the result of calling the method
     */
    public static <T> T callIfAccessible(Object obj, String methodName,
            Object... args) {
        return call(false, obj, methodName, args);
    }

    /**
     * Use reflection to call a static method in {@code clazz} with the
     * specified {@code args}.
     * 
     * @param clazz the {@link Class} instance
     * @param method the method name
     * @param args the args to pass to the method upon invocation
     * @return the result of the method invocation
     */
    public static <T> T callStatic(Class<?> clazz, String method,
            Object... args) {
        return callStatic(true, clazz, method, args);
    }

    /**
     * Use reflection to call a static method in {@code clazz} with the
     * specified {@code args} if and only if that method is natively accessible
     * according to java language access rules.
     * 
     * @param clazz the {@link Class} instance
     * @param method the method name
     * @param args the args to pass to the method upon invocation
     * @return the result of the method invocation
     */
    public static <T> T callStaticIfAccessible(Class<?> clazz, String method,
            Object... args) {
        return callStatic(false, clazz, method, args);
    }

    /**
     * Retrieve the value of {@code variable} from the {@code obj}.
     * <p>
     * This is useful in situations when it is necessary to access an instance
     * variable that is not visible (i.e. when hacking the internals of a 3rd
     * party library).
     * </p>
     * 
     * @param variable the name of the variable to retrieve
     * @param obj the object from which to retrieve the value
     * @return the value of the {@code variable} on {@code obj}, if it exists;
     *         otherwise {@code null}
     */
    @Nullable
    @SuppressWarnings("unchecked")
    public static <T> T get(String variable, Object obj) {
        try {
            Field field = getField(variable, obj);
            return (T) field.get(obj);
        }
        catch (ReflectiveOperationException e) {
            throw CheckedExceptions.throwAsRuntimeException(e);
        }
    }

    /**
     * Given an object, return an array containing all the {@link Field} objects
     * that represent those declared within {@code obj's} entire class hierarchy
     * after the base {@link Object}.
     * 
     * @return the array of declared fields
     */
    public static Field[] getAllDeclaredFields(Object obj) {
        List<Field> fields = new ArrayList<Field>();
        Class<?> clazz = obj.getClass();
        while (clazz != Object.class) {
            for (Field field : clazz.getDeclaredFields()) {
                if(!field.getName().equalsIgnoreCase("fields0")
                        && !field.isSynthetic()
                        && !Modifier.isStatic(field.getModifiers())) {
                    field.setAccessible(true);
                    fields.add(field);
                }
            }
            clazz = clazz.getSuperclass();
        }
        return fields.toArray(new Field[] {});
    }

    /**
     * Reflectively get the value of the {@code field} from the provided
     * {@code object} and attempt an automatic type cast.
     * 
     * @param field the {@link Field} object representing the desired variable
     * @param object the object whose value for the {@code field} should be
     *            retrieved
     * @return the value of {@code field} in {@code object} if it exists,
     *         otherwise {@code null}
     */
    @Nullable
    @SuppressWarnings("unchecked")
    public static <T> T getCasted(Field field, Object object) {
        try {
            field.setAccessible(true);
            return (T) field.get(object);
        }
        catch (ReflectiveOperationException e) {
            throw CheckedExceptions.throwAsRuntimeException(e);
        }
    }

    /**
     * This is literally just syntactic sugar for {@link Class#forName(String)}
     * that doesn't throw a checked exception.
     * 
     * @param name the name of the class
     * @return the {@link Class} object if can be found
     */
    @SuppressWarnings("unchecked")
    public static <T> Class<T> getClassCasted(String name) {
        try {
            return (Class<T>) Class.forName(name);
        }
        catch (ClassNotFoundException e) {
            throw CheckedExceptions.throwAsRuntimeException(e);
        }
    }

    /**
     * Return a {@link Method} instance from {@code clazz} named {@code method}
     * (that takes arguments of {@code paramTypes} respectively)
     * while making a best effort attempt to unbox primitive parameter types
     * 
     * @param clazz the class instance in which the method is contained
     * @param method the name of the method
     * @param paramTypes the types for the respective paramters
     * @return a {@link Method} instance that has been set to be accessible
     */
    public static Method getMethodUnboxed(Class<?> clazz, String method,
            Class<?>... paramTypes) {
        Class<?>[] altParamTypes = new Class<?>[paramTypes.length];
        for (int i = 0; i < altParamTypes.length; ++i) {
            altParamTypes[i] = unbox(paramTypes[i]);
        }
        try {
            Method m = clazz.getDeclaredMethod(method, paramTypes);
            m.setAccessible(true);
            return m;
        }
        catch (NoSuchMethodException e) {
            try {
                // Attempt to find a method using the alt param types.
                // This will usually bear fruit in cases where a method
                // has a primitive type parameter and Java autoboxing
                // causes the passed in parameters to have a wrapper
                // type instead of the appropriate primitive type.
                Method m = clazz.getDeclaredMethod(method, altParamTypes);
                m.setAccessible(true);
                return m;
            }
            catch (NoSuchMethodException e2) {
                throw CheckedExceptions.throwAsRuntimeException(e);
            }
        }
    }

    @Nullable
    @SuppressWarnings("unchecked")
    public static <T> T getStatic(String variable, Class<?> clazz) {
        try {
            Field field = getField(variable, clazz);
            return (T) field.get(null);
        }
        catch (ReflectiveOperationException e) {
            throw CheckedExceptions.throwAsRuntimeException(e);
        }
    }

    /**
     * Given a {@link Class}, create a new instance by calling the appropriate
     * constructor for the given {@code args}.
     * 
     * @param clazz the type of instance to construct
     * @param args the parameters to pass to the constructor
     * @return the new instance
     */
    @SuppressWarnings("unchecked")
    public static <T> T newInstance(Class<? extends T> clazz, Object... args) {
        try {
            Constructor<? extends T> toCall = null;
            outer: for (Constructor<?> constructor : clazz
                    .getDeclaredConstructors()) {
                Class<?>[] paramTypes = constructor.getParameterTypes();
                if(paramTypes == null && args == null) { // Handle no arg
                                                         // constructors
                    toCall = (Constructor<? extends T>) constructor;
                    break;
                }
                else if(args == null || paramTypes == null
                        || args.length != paramTypes.length) {
                    continue;
                }
                else {
                    for (int i = 0; i < args.length; ++i) {
                        Object arg = args[i];
                        Class<?> type = paramTypes[i];
                        Class<?> altType = getAltType(type);
                        if(!type.isAssignableFrom(arg.getClass())
                                && !altType.isAssignableFrom(arg.getClass())) {
                            continue outer;
                        }
                    }
                    toCall = (Constructor<? extends T>) constructor;
                    break;
                }
            }
            if(toCall != null) {
                toCall.setAccessible(true);
                return (T) toCall.newInstance(args);
            }
            else {
                throw new NoSuchMethodException("No constructor for " + clazz
                        + " accepts arguments: " + Arrays.toString(args));
            }
        }
        catch (ReflectiveOperationException e) {
            throw CheckedExceptions.throwAsRuntimeException(e);
        }
    }

    /**
     * Call {@code constructor} with {@code args} and return a new instance of
     * type {@code T}.
     * 
     * @param constructor the {@link Constructor} to use for creation
     * @param args the initialization args to pass to the constructor
     * @return an instance of the class to which the {@code constructor} belongs
     */
    public static <T> T newInstance(Constructor<? extends T> constructor,
            Object... args) {
        try {
            constructor.setAccessible(true);
            return constructor.newInstance(args);
        }
        catch (ReflectiveOperationException e) {
            throw CheckedExceptions.throwAsRuntimeException(e);
        }
    }

    /**
     * Edit the value of {@code variable} on {@code obj}.
     * <p>
     * <strong>NOTE:</strong> This can have grave unintended consequences if you
     * alter the value of a variable after construction. Especially if other
     * parts of the code assume that {@code obj} is immutable, or the
     * {@code variable} is used in a multi threaded context.
     * </p>
     * 
     * @param variable the name of the variable to set
     * @param value the value to set
     * @param obj the object on which to set the value
     */
    public static void set(String variable, Object value, Object obj) {
        try {
            Field field = getField(variable, obj);
            field.set(obj, value);
        }
        catch (ReflectiveOperationException e) {
            throw CheckedExceptions.throwAsRuntimeException(e);
        }
    }

    /**
     * Use reflection to call an instance method on {@code obj} with the
     * specified {@code args}.
     * 
     * @param setAccessible an indication as to whether the reflective call
     *            should suppress Java language access checks or not
     * @param obj
     * @param methodName
     * @param args
     * @return the result of calling the method
     */
    @SuppressWarnings("unchecked")
    private static <T> T call(boolean setAccessible, Object obj,
            String methodName, Object... args) {
        Method method = getMethod(true, methodName, obj.getClass(), args);
        try {
            return (T) method.invoke(obj, args);
        }
        catch (ReflectiveOperationException e) {
            throw CheckedExceptions.throwAsRuntimeException(e);
        }
    }

    /**
     * Do the work to use reflection to call a static method in {@code clazz}
     * with the specified {@code args} while optionally obeying the native java
     * language access rules.
     * 
     * @param setAccessible a flag that determines whether java language access
     *            rules will be overridden
     * @param clazz the {@link Class} instance
     * @param methodName the method name
     * @param args the args to pass to the method upon invocation
     * @return the result of the method invocation
     */
    @SuppressWarnings("unchecked")
    private static <T> T callStatic(boolean setAccessible, Class<?> clazz,
            String methodName, Object... args) {
        Method method = getMethod(setAccessible, methodName, clazz, args);
        try {
            return (T) method.invoke(null, args);
        }
        catch (ReflectiveOperationException e) {
            throw CheckedExceptions.throwAsRuntimeException(e);
        }
    }

    /**
     * Return the boxed version of {@code clazz} if it is a primitive, or the
     * unboxed version if it is a wrapper.
     * 
     * @param clazz the class for which the alt type is returned
     * @return the alt type
     */
    private static Class<?> getAltType(Class<?> clazz) {
        if(clazz.isPrimitive()) {
            if(clazz == int.class) {
                return Integer.class;
            }
            else if(clazz == long.class) {
                return Long.class;
            }
            else if(clazz == float.class) {
                return Float.class;
            }
            else if(clazz == double.class) {
                return Double.class;
            }
            else if(clazz == short.class) {
                return Short.class;
            }
            else if(clazz == byte.class) {
                return Byte.class;
            }
            else if(clazz == char.class) {
                return Character.class;
            }
            else if(clazz == boolean.class) {
                return Boolean.class;
            }
            else {
                return clazz;
            }
        }
        else {
            return unbox(clazz);
        }
    }

    /**
     * Return the value of the {@link Field} called {@code name} in
     * {@code clazz} from the specified {@code obj}.
     * 
     * @param name the name of the field
     * @param clazz the {@link Class} in which the field is defined
     * @return the associated {@link Field} object
     */
    private static Field getField(String name, Class<?> clazz) {
        try {
            Field field = null;
            while (clazz != null && field == null) {
                try {
                    field = clazz.getDeclaredField(name);
                }
                catch (NoSuchFieldException e) { // check the parent to see if
                                                 // the field was defined there
                    clazz = clazz.getSuperclass();
                }
            }
            if(field != null) {
                field.setAccessible(true);
                return field;
            }
            else {
                throw new NoSuchFieldException("No field name " + name
                        + " exists in the hirearchy of " + clazz);
            }
        }
        catch (ReflectiveOperationException e) {
            throw CheckedExceptions.throwAsRuntimeException(e);
        }
    }

    /**
     * Return the {@link Field} object that holds the variable with {@code name}
     * in {@code obj}, if it exists. Otherwise a
     * NoSuchFieldException is thrown.
     * <p>
     * This method will take care of making the field accessible.
     * </p>
     * 
     * @param name the name of the field to get
     * @param obj the object from which to get the field
     * @return the {@link Field} object
     * @throws NoSuchFieldException
     */
    private static Field getField(String name, Object obj) {
        return getField(name, obj.getClass());
    }

    /**
     * Return the {@link Method} object called {@code name} in {@code clazz}
     * that accepts the specified {@code args} and optionally ignore the native
     * java language access rules.
     * 
     * @param setAccessible a flag that indicates whether the native java
     *            language access rules should be ignored
     * @param name the method name
     * @param clazz the {@link Class} in which the method is defined
     * @param args the parameters defined in the method's signature
     * @return the associated {@link Method} object
     */
    private static Method getMethod(boolean setAccessible, String name,
            Class<?> clazz, Object... args) {
        // TODO cache method instances
        try {
            Class<?>[] parameterTypes = new Class<?>[args.length];
            Class<?>[] altParameterTypes = new Class<?>[args.length];
            for (int i = 0; i < args.length; i++) {
                parameterTypes[i] = args[i].getClass();
                altParameterTypes[i] = unbox(args[i].getClass());
            }
            Method method = null;
            while (clazz != null && method == null) {
                try {
                    method = clazz.getDeclaredMethod(name, parameterTypes);
                }
                catch (NoSuchMethodException e) {
                    try {
                        // Attempt to find a method using the alt param types.
                        // This will usually bear fruit in cases where a method
                        // has a primitive type parameter and Java autoboxing
                        // causes the passed in parameters to have a wrapper
                        // type instead of the appropriate primitive type.
                        method = clazz.getDeclaredMethod(name,
                                altParameterTypes);
                    }
                    catch (NoSuchMethodException e2) {
                        clazz = clazz.getSuperclass();
                    }
                }
            }
            if(method != null) {
                method.setAccessible(setAccessible);
                return method;
            }
            else {
                throw new NoSuchMethodException();
            }
        }
        catch (ReflectiveOperationException e) {
            throw CheckedExceptions.throwAsRuntimeException(e);
        }
    }

    /**
     * Return the unboxed version of the input {@code clazz}. This is usually
     * a class that represents a primitive for an autoboxed wrapper class.
     * Otherwise, the input {@code clazz} is returned.
     * 
     * @param clazz the {@link Class} to unbox
     * @return the unboxed class
     */
    private static Class<?> unbox(Class<?> clazz) {
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

    private Reflection() {/* noinit */}

}
