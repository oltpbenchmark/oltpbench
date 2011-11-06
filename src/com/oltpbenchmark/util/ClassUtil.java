/*******************************************************************************
 * oltpbenchmark.com
 *  
 *  Project Info:  http://oltpbenchmark.com
 *  Project Members:    Carlo Curino <carlo.curino@gmail.com>
 *              Evan Jones <ej@evanjones.ca>
 *              DIFALLAH Djellel Eddine <djelleleddine.difallah@unifr.ch>
 *              Andy Pavlo <pavlo@cs.brown.edu>
 *              CUDRE-MAUROUX Philippe <philippe.cudre-mauroux@unifr.ch>  
 *                  Yang Zhang <yaaang@gmail.com> 
 * 
 *  This library is free software; you can redistribute it and/or modify it under the terms
 *  of the GNU General Public License as published by the Free Software Foundation;
 *  either version 3.0 of the License, or (at your option) any later version.
 * 
 *  This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *  See the GNU Lesser General Public License for more details.
 ******************************************************************************/
/***************************************************************************
 *  Copyright (C) 2009 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  http://hstore.cs.brown.edu                                             *
 *                                                                         *
 *  Permission is hereby granted, free of charge, to any person obtaining  *
 *  a copy of this software and associated documentation files (the        *
 *  "Software"), to deal in the Software without restriction, including    *
 *  without limitation the rights to use, copy, modify, merge, publish,    *
 *  distribute, sublicense, and/or sell copies of the Software, and to     *
 *  permit persons to whom the Software is furnished to do so, subject to  *
 *  the following conditions:                                              *
 *                                                                         *
 *  The above copyright notice and this permission notice shall be         *
 *  included in all copies or substantial portions of the Software.        *
 *                                                                         *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,        *
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF     *
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. *
 *  IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR      *
 *  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,  *
 *  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR  *
 *  OTHER DEALINGS IN THE SOFTWARE.                                        *
 ***************************************************************************/
package com.oltpbenchmark.util;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.ClassUtils;

/**
 * 
 * @author pavlo
 *
 */
public abstract class ClassUtil {
    
    private static final Map<Class<?>, List<Class<?>>> CACHE_getSuperClasses = new HashMap<Class<?>, List<Class<?>>>(); 
    private static final Map<Class<?>, Set<Class<?>>> CACHE_getInterfaceClasses = new HashMap<Class<?>, Set<Class<?>>>();

    /**
     * Check if the given object is an array (primitve or native).
     * http://www.java2s.com/Code/Java/Reflection/Checkifthegivenobjectisanarrayprimitveornative.htm
     * @param obj  Object to test.
     * @return     True of the object is an array.
     */
    public static boolean isArray(final Object obj) {
        return (obj != null ? obj.getClass().isArray() : false);
    }
    
    public static boolean[] isArray(final Object objs[]) {
        boolean is_array[] = new boolean[objs.length];
        for (int i = 0; i < objs.length; i++) {
            is_array[i] = ClassUtil.isArray(objs[i]);
        } // FOR
        return (is_array);
    }
    
    /**
     * Convert a Enum array to a Field array
     * This assumes that the name of each Enum element corresponds to a data member in the clas
     * @param <E>
     * @param clazz
     * @param members
     * @return
     * @throws NoSuchFieldException
     */
    public static <E extends Enum<?>> Field[] getFieldsFromMembersEnum(Class<?> clazz, E members[]) throws NoSuchFieldException {
        Field fields[] = new Field[members.length];
        for (int i = 0; i < members.length; i++) {
            fields[i] = clazz.getDeclaredField(members[i].name().toLowerCase());
        } // FOR
        return (fields);
    }

    /**
     * Get the generic types for the given field
     * @param field
     * @return
     */
    @SuppressWarnings("unchecked")
    public static List<Class> getGenericTypes(Field field) {
        ArrayList<Class> generic_classes = new ArrayList<Class>();
        Type gtype = field.getGenericType();
        if (gtype instanceof ParameterizedType) {
            ParameterizedType ptype = (ParameterizedType)gtype;
            getGenericTypesImpl(ptype, generic_classes);
        }
        return (generic_classes);
    }
        
    @SuppressWarnings("unchecked")
    private static void getGenericTypesImpl(ParameterizedType ptype, List<Class> classes) {
        // list the actual type arguments
        for (Type t : ptype.getActualTypeArguments()) {
            if (t instanceof Class) {
//                System.err.println("C: " + t);
                classes.add((Class)t);
            } else if (t instanceof ParameterizedType) {
                ParameterizedType next = (ParameterizedType)t;
//                System.err.println("PT: " + next);
                classes.add((Class)next.getRawType());
                getGenericTypesImpl(next, classes);
            }
        } // FOR
        return;
    }
    
    /**
     * Return an ordered list of all the sub-classes for a given class
     * Useful when dealing with generics
     * @param element_class
     * @return
     */
    public static List<Class<?>> getSuperClasses(Class<?> element_class) {
        List<Class<?>> ret = ClassUtil.CACHE_getSuperClasses.get(element_class);
        if (ret == null) {
            ret = new ArrayList<Class<?>>();
            while (element_class != null) {
                ret.add(element_class);
                element_class = element_class.getSuperclass();
            } // WHILE
            ret = Collections.unmodifiableList(ret);
            ClassUtil.CACHE_getSuperClasses.put(element_class, ret);
        }
        return (ret);
    }
    
    /**
     * Get a set of all of the interfaces that the element_class implements
     * @param element_class
     * @return
     */
    @SuppressWarnings("unchecked")
    public static Collection<Class<?>> getInterfaces(Class<?> element_class) {
        Set<Class<?>> ret = ClassUtil.CACHE_getInterfaceClasses.get(element_class);
        if (ret == null) {
//            ret = new HashSet<Class<?>>();
//            Queue<Class<?>> queue = new LinkedList<Class<?>>();
//            queue.add(element_class);
//            while (!queue.isEmpty()) {
//                Class<?> current = queue.poll();
//                for (Class<?> i : current.getInterfaces()) {
//                    ret.add(i);
//                    queue.add(i);
//                } // FOR
//            } // WHILE
            ret = new HashSet<Class<?>>(ClassUtils.getAllInterfaces(element_class));
            if (element_class.isInterface()) ret.add(element_class);
            ret = Collections.unmodifiableSet(ret);
            ClassUtil.CACHE_getInterfaceClasses.put(element_class, ret);
        }
        return (ret);
    }
    
    @SuppressWarnings("unchecked")
    public static <T> T newInstance(String class_name, Object params[], Class<?> classes[]) {
        return ((T)ClassUtil.newInstance(ClassUtil.getClass(class_name), params, classes));
    }

    
    public static <T> T newInstance(Class<T> target_class, Object params[], Class<?> classes[]) {
//        Class<?> const_params[] = new Class<?>[params.length];
//        for (int i = 0; i < params.length; i++) {
//            const_params[i] = params[i].getClass();
//            System.err.println("[" + i + "] " + params[i] + " " + params[i].getClass());
//        } // FOR
        
        Constructor<T> constructor = ClassUtil.getConstructor(target_class, classes);
        T ret = null;
        try {
            ret = constructor.newInstance(params);
        } catch (Exception ex) {
            throw new RuntimeException("Failed to create new instance of " + target_class.getSimpleName(), ex);
        }
        return (ret);
    }
    
    /**
     * 
     * @param <T>
     * @param target_class
     * @param params
     * @return
     */
    public static <T> Constructor<T> getConstructor(Class<T> target_class, Class<?>...params) {
        Constructor<T> constructor = null;
        try {
//            System.err.println("Looking for constructor: " + target_class);
//            System.err.print("Parameters: ");
//            String add = "";
//            for (Class<?> p_class : params) {
//                System.err.print(add + p_class.getSimpleName());
//                add = ", ";
//            }
//            System.err.println();
            
            constructor = target_class.getConstructor(params); 
        } catch (Exception ex) {
            System.err.println("TARGET_CLASS: " + target_class);
            System.err.println("PARAMS: " + Arrays.toString(params));
            System.err.println("------");
            for (Constructor<?> c : target_class.getConstructors()) {
                System.err.println(c);
            }
            
            throw new RuntimeException("Failed to retrieve constructor for " + target_class.getSimpleName(), ex);
        }
        return (constructor);
    }
    
    /**
     * 
     * @param class_name
     * @return
     */
    public static Class<?> getClass(String class_name) {
        Class<?> target_class = null;
        try {
            ClassLoader loader = ClassLoader.getSystemClassLoader();
            target_class = (Class<?>)loader.loadClass(class_name);
        } catch (Exception ex) {
            throw new RuntimeException("Failed to retrieve class for " + class_name, ex);
        }
        return (target_class);
 
    }
}
