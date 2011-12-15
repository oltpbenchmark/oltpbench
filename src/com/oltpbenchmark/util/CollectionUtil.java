/***************************************************************************
 *  Copyright (C) 2011 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  http://hstore.cs.brown.edu/                                            *
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

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;

import org.apache.commons.collections15.set.ListOrderedSet;
import org.apache.commons.lang.NotImplementedException;

/**
 * 
 * @author pavlo
 */
public abstract class CollectionUtil {
    
    private static final Random RANDOM = new Random();
    
    /**
     * Put all of the elements in items into the given array
     * This assumes that the array has been pre-allocated
     * @param <T>
     * @param items
     * @param array
     */
    public static <T> void toArray(Collection<T> items, Object array[], boolean convert_to_primitive) {
        assert(items.size() == array.length);
        
        int i = 0;
        for (T t : items) {
            if (convert_to_primitive) {
                if (t instanceof Long) {
                    array[i] = ((Long)t).longValue();
                } else if (t instanceof Integer) {
                    array[i] = ((Integer)t).intValue();
                } else if (t instanceof Double) {
                    array[i] = ((Double)t).doubleValue();
                } else if (t instanceof Boolean) {
                    array[i] = ((Boolean)t).booleanValue();
                }
            } else {
                array[i] = t;
            }
        }
        return;
    }
    
    public static int[] toIntArray(Collection<Integer> items) {
        int ret[] = new int[items.size()];
        int idx = 0;
        for (Integer i : items) {
            assert(i != null);
            ret[idx++] = i.intValue();
        } // FOR
        return (ret);
    }
    
    /**
     * Put all the values of an Iterator into a List
     * @param <T>
     * @param it
     * @return
     */
    public static <T> List<T> list(Iterator<T> it) {
        List<T> list = new ArrayList<T>();
        CollectionUtil.addAll(list, it);
        return (list);
    }
    /**
     * Put all of the values of an Enumeration into a new List
     * @param <T>
     * @param e
     * @return
     */
    public static <T> List<T> list(Enumeration<T> e) {
        return (list(iterable(e)));
    }
    /**
     * Put all of the values of an Iterable into a new List
     * @param <T>
     * @param it
     * @return
     */
    public static <T> List<T> list(Iterable<T> it) {
        return (list(it.iterator()));
    }
    
    /**
     * Put all the values of an Iterator into a Set
     * @param <T>
     * @param it
     * @return
     */
    public static <T> Set<T> set(Iterator<T> it) {
        Set<T> set = new HashSet<T>();
        CollectionUtil.addAll(set, it);
        return (set);
    }
    /**
     * Put all of the values of an Iterable into a new Set
     * @param <T>
     * @param it
     * @return
     */
    public static <T> Set<T> set(Iterable<T> it) {
        return (set(it.iterator()));
    }
    
    /**
     * Returns a list containing the string representations of the elements in the collection
     * @param <T>
     * @param data
     * @return
     */
    public static <T> List<String> toStringList(Collection<T> data) {
        List<String> ret = new ArrayList<String>();
        for (T t : data) ret.add(t.toString());
        return (ret);
    }
    
    /**
     * Returns a set containing the string representations of the elements in the collection 
     * @param <T>
     * @param data
     * @return
     */
    public static <T> Set<String> toStringSet(Collection<T> data) {
        Set<String> ret = new HashSet<String>();
        for (T t : data) ret.add(t.toString());
        return (ret);
    }
    
    /**
     * Return a random value from the given Collection
     * @param <T>
     * @param items
     */
    public static <T> T random(Collection<T> items) {
        return (CollectionUtil.random(items, RANDOM));
    }
    
    /**
     * Return a random value from the given Collection
     * @param <T>
     * @param items
     * @param rand
     * @return
     */
    public static <T> T random(Collection<T> items, Random rand) {
        int idx = rand.nextInt(items.size());
        return (CollectionUtil.get(items, idx));
    }
    
    /**
     * Return a random value from the given Iterable
     * @param <T>
     * @param it
     * @return
     */
    public static <T> T random(Iterable<T> it) {
        return (CollectionUtil.random(it, RANDOM));
    }
    
    /**
     * Return a random value from the given Iterable
     * @param <T>
     * @param it
     * @param rand
     * @return
     */
    public static <T> T random(Iterable<T> it, Random rand) { 
        List<T> list = new ArrayList<T>();
        for (T t : it) {
            list.add(t);
        } // FOR
        return (CollectionUtil.random(list, rand));
    }
    
    public static <E extends Enum<?>> Set<E> getAllExcluding(E elements[], E...excluding) {
        Set<E> exclude_set = new HashSet<E>();
        for (E e : excluding) exclude_set.add(e);
        
        Set<E> elements_set = new HashSet<E>();
        for (int i = 0; i < elements.length; i++) {
            if (!exclude_set.contains(elements[i])) elements_set.add(elements[i]);
        } // FOR
        return (elements_set);
//      Crappy java....
//        Object new_elements[] = new Object[elements_set.size()];
//        elements_set.toArray(new_elements);
//        return ((E[])new_elements);
    }
    
    /**
     * Add all the items in the array to a Collection
     * @param <T>
     * @param data
     * @param items
     */
    public static <T> Collection<T> addAll(Collection<T> data, T...items) {
        for (T i : (T[])items) {
            data.add(i);
        }
        return (data);
    }
    
    /**
     * Add all the items in the Enumeration into a Collection
     * @param <T>
     * @param data
     * @param items
     */
    public static <T> Collection<T> addAll(Collection<T> data, Enumeration<T> items) {
        while (items.hasMoreElements()) {
            data.add(items.nextElement());
        } // WHILE
        return (data);
    }
    
    /**
     * Add all of the items from the Iterable into the given collection
     * @param <T>
     * @param data
     * @param items
     */
    public static <T> Collection<T> addAll(Collection<T> data, Iterable<T> items) {
        return (CollectionUtil.addAll(data, items.iterator()));
    }
    
    /**
     * Add all of the items from the Iterator into the given collection
     * @param <T>
     * @param data
     * @param items
     */
    public static <T> Collection<T> addAll(Collection<T> data, Iterator<T> items) {
        while (items.hasNext()) {
            data.add(items.next());
        } // WHILE
        return (data);
    }
    
    /**
     * 
     * @param <T>
     * @param <U>
     * @param map
     * @return
     */
    public static <T, U extends Comparable<U>> T getGreatest(Map<T, U> map) {
        T max_key = null;
        U max_value = null;
        for (T key : map.keySet()) {
            U value = map.get(key);
            if (max_value == null || value.compareTo(max_value) > 0) {
                max_value = value;
                max_key = key;
            }
        } // FOR
        return (max_key);
    }
    
    /**
     * Return the first item in a Iterable
     * @param <T>
     * @param items
     * @return
     */
    public static <T> T first(Iterable<T> items) {
        return (CollectionUtil.get(items, 0));
    }
    
    /**
     * Return the first item in a Iterator
     * @param <T>
     * @param items
     * @return
     */
    public static <T> T first(Iterator<T> items) {
        return (items.hasNext() ? items.next() : null);
    }
    
    /**
     * Returns the first item in an Enumeration
     * @param <T>
     * @param items
     * @return
     */
    public static <T> T first(Enumeration<T> items) {
        return (items.hasMoreElements() ? items.nextElement() : null);
    }
    
    /**
     * Return the ith element of a set. Super lame
     * @param <T>
     * @param items
     * @param idx
     * @return
     */
    public static <T> T get(Iterable<T> items, int idx) {
        if (items instanceof AbstractList<?>) {
            return ((AbstractList<T>)items).get(idx);
        } else if (items instanceof ListOrderedSet<?>) {
            return ((ListOrderedSet<T>)items).get(idx);
        }
        int ctr = 0;
        for (T t : items) {
            if (ctr++ == idx) return (t);
        }
        return (null);
    }
    
    /**
     * Return the last item in an Iterable
     * @param <T>
     * @param items
     * @return
     */
    public static <T> T last(Iterable<T> items) {
        T last = null;
        if (items instanceof AbstractList<?>) {
            AbstractList<T> list = (AbstractList<T>)items;
            last = (list.isEmpty() ? null : list.get(list.size() - 1));
        } else {
            for (T t : items) {
                last = t;
            }
        }
        return (last);
    }
    
    /**
     * Return the last item in an array
     * @param <T>
     * @param items
     * @return
     */
    public static <T> T last(T...items) {
        if (items != null && items.length > 0) {
            return (items[items.length-1]);
        }
        return (null);
    }

    /**
     * 
     * @param <K>
     * @param <V>
     * @param map
     * @return
     */
    public static <K extends Comparable<?>, V> List<V> getSortedList(SortedMap<K, Collection<V>> map) {
        List<V> ret = new ArrayList<V>();
        for (K key : map.keySet()) {
            ret.addAll(map.get(key));
        } // FOR
        return (ret);
    }
    
    /**
     * Wrap an Iterable around an Iterator
     * @param <T>
     * @param it
     * @return
     */
    public static <T> Iterable<T> iterable(final Iterator<T> it) {
        return (new Iterable<T>() {
            @Override
            public Iterator<T> iterator() {
                return (it);
            }
        });
    }
    
    public static <T> Iterable<T> iterable(final T values[]) {
        return (new Iterable<T>() {
            @Override
            public Iterator<T> iterator() {
                return new Iterator<T>() {
                    private int idx = 0;
                    @Override
                    public boolean hasNext() {
                        return (this.idx < values.length);
                    }
                    @Override
                    public T next() {
                        return (values[this.idx++]);
                    }
                    @Override
                    public void remove() {
                        throw new NotImplementedException();
                    }
                };
            }
        });
    }
    
    /**
     * Wrap an Iterable around an Enumeration
     * @param <T>
     * @param e
     * @return
     */
    public static <T> Iterable<T> iterable(final Enumeration<T> e) {
        return (new Iterable<T>() {
            @Override
            public Iterator<T> iterator() {
                return new Iterator<T>() {
                    @Override
                    public boolean hasNext() {
                        return (e.hasMoreElements());
                    }
                    @Override
                    public T next() {
                        return (e.nextElement());
                    }
                    @Override
                    public void remove() {
                        throw new NotImplementedException();
                    }
                };
            }
        });
    }
    
    public static <T> T pop(Collection<T> items) {
        T t = CollectionUtil.first(items);
        if (t != null) {
            boolean ret = items.remove(t);
            assert(ret);
        }
        return (t);
    }
}
