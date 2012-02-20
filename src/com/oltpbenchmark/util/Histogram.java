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

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import com.oltpbenchmark.util.json.*;

/**
 * A very nice and simple generic Histogram
 * @author svelagap
 * @author pavlo
 */
public class Histogram<X> implements JSONSerializable {
    private static final Logger LOG = Logger.getLogger(Histogram.class);
    
    private static final String MARKER = "*";
    private static final Integer MAX_CHARS = 80;
    private static final Integer MAX_VALUE_LENGTH = 20;

    public enum Members {
        VALUE_TYPE,
        HISTOGRAM,
        NUM_SAMPLES,
        KEEP_ZERO_ENTRIES,
    }
    
    protected final SortedMap<X, Integer> histogram = new TreeMap<X, Integer>();
    protected int num_samples = 0;
    
    /**
     * 
     */
    protected transient Map<Object, String> debug_names; 
    
    /**
     * The Min/Max values are the smallest/greatest values we have seen based
     * on some natural ordering
     */
    protected Comparable<X> min_value;
    protected Comparable<X> max_value;
    
    /**
     * The Min/Max counts are the values that have the smallest/greatest number of
     * occurences in the histogram
     */
    protected int min_count = 0;
    protected List<X> min_count_values;
    protected int max_count = 0;
    protected List<X> max_count_values;
    
    /**
     * A switchable flag that determines whether non-zero entries are kept or removed
     */
    protected boolean keep_zero_entries = false;
    
    /**
     * Constructor
     */
    public Histogram() {
        // Nothing...
    }
    
    /**
     * Constructor
     * @param keepZeroEntries
     */
    public Histogram(boolean keepZeroEntries) {
        this.keep_zero_entries = keepZeroEntries;
    }
    
    /**
     * Copy Constructor
     * @param other
     */
    public Histogram(Histogram<X> other) {
        assert(other != null);
        this.putHistogram(other);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Histogram<?>) {
            Histogram<?> other = (Histogram<?>)obj;
            return (this.histogram.equals(other.histogram));
        }
        return (false);
    }
    
    /**
     * Helper method used for replacing the object's toString() output with labels
     * @param names_map
     */
    public Histogram<X> setDebugLabels(Map<?, String> names_map) {
        if (this.debug_names == null) {
            synchronized (this) {
                if (this.debug_names == null) {
                    this.debug_names = new HashMap<Object, String>();
                }
            } // SYNCH
        }
        this.debug_names.putAll(names_map);
        return (this);
    }
    public boolean hasDebugLabels() {
        return (this.debug_names != null && this.debug_names.isEmpty() == false);
    }
    

    /**
     * Set whether this histogram is allowed to retain zero count entries
     * If the flag switches from true to false, then all zero count entries will be removed
     * Default is false
     * @param flag
     */
    public void setKeepZeroEntries(boolean flag) {
        // When this option is disabled, we need to remove all of the zeroed entries
        if (!flag && this.keep_zero_entries) {
            synchronized (this) {
                Iterator<X> it = this.histogram.keySet().iterator();
                int ctr = 0;
                while (it.hasNext()) {
                    X key = it.next();
                    if (this.histogram.get(key) == 0) {
                        it.remove();
                        ctr++;
                    }
                } // WHILE
                if (ctr > 0) {
                    LOG.debug("Removed " + ctr + " zero entries from histogram");
                    this.calculateInternalValues();
                }
            } // SYNCHRONIZED
        }
        this.keep_zero_entries = flag;
    }
    
    public boolean isZeroEntriesEnabled() {
        return this.keep_zero_entries;
    }
    
    /**
     * The main method that updates a value in the histogram with a given sample count
     * This should be called by one of the public interface methods that are synchronized
     * This method is not synchronized on purpose for performance
     * @param value
     * @param count
     */
    private void _put(X value, int count) {
        if (value == null) return;
        this.num_samples += count;
        
        // If we already have this value in our histogram, then add the new count
        // to its existing total
        if (this.histogram.containsKey(value)) {
            count += this.histogram.get(value);
        }
        assert(count >= 0) : "Invalid negative count for key '" + value + "' [count=" + count + "]";
        // If the new count is zero, then completely remove it if we're not allowed to have zero entries
        if (count == 0 && !this.keep_zero_entries) {
            this.histogram.remove(value);
        } else {
            this.histogram.put(value, count);
        }
    }

    /**
     * Recalculate the min/max count value sets
     * Since this is expensive, this should only be done whenever that information is needed 
     */
    @SuppressWarnings("unchecked")
    private synchronized void calculateInternalValues() {
        // New Min/Max Counts
        // The reason we have to loop through and check every time is that our 
        // value may be the current min/max count and thus it may or may not still
        // be after the count is changed
        this.max_count = 0;
        this.min_count = Integer.MAX_VALUE;
        this.min_value = null;
        this.max_value = null;
        
        if (this.min_count_values == null) this.min_count_values = new ArrayList<X>();
        if (this.max_count_values == null) this.max_count_values = new ArrayList<X>();
        
        for (Entry<X, Integer> e : this.histogram.entrySet()) {
            X value = e.getKey();
            int cnt = e.getValue().intValue();
            
            // Is this value the new min/max values?
            if (this.min_value == null || this.min_value.compareTo(value) > 0) {
                this.min_value = (Comparable<X>)value;
            } else if (this.max_value == null || this.max_value.compareTo(value) < 0) {
                this.max_value = (Comparable<X>)value;
            }
            
            if (cnt <= this.min_count) {
                if (cnt < this.min_count) this.min_count_values.clear();
                this.min_count_values.add(value);
                this.min_count = cnt;
            }
            if (cnt >= this.max_count) {
                if (cnt > this.max_count) this.max_count_values.clear();
                this.max_count_values.add(value);
                this.max_count = cnt;
            }
        } // FOR
    }
    
    
    /**
     * Get the number of samples entered into the histogram using the put methods
     * @return
     */
    public int getSampleCount() {
        return (this.num_samples);
    }
    /**
     * Get the number of unique values entered into the histogram 
     * @return
     */
    public int getValueCount() {
        return (this.histogram.values().size());
    }
    
    /**
     * Get the smallest value entered into the histogram
     * This assumes that the values implement the Comparable interface
     * @return
     */
    @SuppressWarnings("unchecked")
    public X getMinValue() {
        this.calculateInternalValues();
        return ((X)this.min_value);
    }
    /**
     * Get the largest value entered into the histogram
     * This assumes that the values implement the Comparable interface
     * @return
     */
    @SuppressWarnings("unchecked")
    public X getMaxValue() {
        this.calculateInternalValues();
        return ((X)this.max_value);
    }

    /**
     * Return the number of samples for the value with the smallest number of samples in the histogram
     * @return
     */
    public int getMinCount() {
        this.calculateInternalValues();
        return (this.min_count);
    }
    /**
     * Return the set values with the smallest number of samples
     * @return
     */
    public Collection<X> getMinCountValues() {
        this.calculateInternalValues();
        return (this.min_count_values);
    }
    /**
     * Return the number of samples for the value with the greatest number of samples in the histogram
     * @return
     */
    public int getMaxCount() {
        this.calculateInternalValues();
        return (this.max_count);
    }
    /**
     * Return the set values with the greatest number of samples
     * @return
     */
    public Collection<X> getMaxCountValues() {
        this.calculateInternalValues();
        return (this.max_count_values);
    }

    /**
     * Return all the values stored in the histogram
     * @return
     */
    public Collection<X> values() {
        return (Collections.unmodifiableCollection(this.histogram.keySet()));
    }
    
    /**
     * Returns the list of values sorted in descending order by cardinality
     * @return
     */
    public SortedSet<X> sortedValues() {
        SortedSet<X> sorted = new TreeSet<X>(new Comparator<X>() {
            public int compare(final X item0, final X item1) {
                final Integer v0 = Histogram.this.get(item0);
                final Integer v1 = Histogram.this.get(item1);
                if (v0.equals(v1)) return (-1);
                return (v1.compareTo(v0));
              }
        });
        sorted.addAll(this.histogram.keySet());
        return (sorted);
    }
    
    /**
     * Return the set of values from the histogram that have the matching count in the histogram
     * @param <T>
     * @param count
     * @return
     */
    public Set<X> getValuesForCount(int count) {
        Set<X> ret = new HashSet<X>();
        for (Entry<X, Integer> e : this.histogram.entrySet()) {
            if (e.getValue().intValue() == count) ret.add(e.getKey());
        } // FOR
        return (ret);
    }
    
    /**
     * Reset the histogram's internal data
     */
    public synchronized void clear() {
        this.histogram.clear();
        this.num_samples = 0;
        this.min_count = 0;
        if (this.min_count_values != null) this.min_count_values.clear();
        this.min_value = null;
        this.max_count = 0;
        if (this.max_count_values != null) this.max_count_values.clear();
        this.max_value = null;
        assert(this.histogram.isEmpty());
    }
    
    /**
     * Clear all the values stored in the histogram. The keys are only kept if
     * KeepZeroEntries is enabled, otherwise it does the same thing as clear()
     */
    public synchronized void clearValues() {
        if (this.keep_zero_entries) {
            for (Entry<X, Integer> e : this.histogram.entrySet()) {
                this.histogram.put(e.getKey(), 0);
            } // FOR
            this.num_samples = 0;
            this.min_count = 0;
            if (this.min_count_values != null) this.min_count_values.clear();
            this.min_value = null;
            this.max_count = 0;
            if (this.max_count_values != null) this.max_count_values.clear();
            this.max_value = null;
        } else {
            this.clear();
        }
    }
    
    /**
     * 
     * @return
     */
    public boolean isEmpty() {
        return (this.histogram.isEmpty());
    }
    
    /**
     * Increments the number of occurrences of this particular value i
     * @param value the value to be added to the histogram
     * 
     */
    public synchronized void put(X value, int i) {
        this._put(value, i);
    }
    
    /**
     * Set the number of occurrences of this particular value i
     * @param value the value to be added to the histogram
     * 
     */
    public synchronized void set(X value, int i) {
        Integer orig = this.get(value);
        if (orig != null && orig != i) {
            i = (orig > i ? -1*(orig - i) : i - orig);
        }
        this._put(value, i);
    }
    
    /**
     * Increments the number of occurrences of this particular value i
     * @param value the value to be added to the histogram
     * 
     */
    public synchronized void put(X value) {
        this._put(value, 1);
    }
    
    /**
     * Increment all values in the histogram by one
     */
    public void putAll() {
        this.putAll(this.histogram.keySet(), 1);
    }
    
    /**
     * Increment multiple values by one
     * @param values
     */
    public void putAll(Collection<X> values) {
        this.putAll(values, 1);
    }
    
    /**
     * Increment multiple values by the given count
     * @param values
     * @param count
     */
    public synchronized void putAll(Collection<X> values, int count) {
        for (X v : values) {
            this._put(v, count);
        } // FOR
    }
    
    /**
     * Add all the entries from the provided Histogram into this objects totals
     * @param other
     */
    public synchronized void putHistogram(Histogram<X> other) {
        for (Entry<X, Integer> e : other.histogram.entrySet()) {
            if (e.getValue() > 0) this._put(e.getKey(), e.getValue());
        } // FOR
    }
    
    /**
     * Remove the given count from the total of the value
     * @param value
     * @param count
     */
    public synchronized void remove(X value, int count) {
        assert(this.histogram.containsKey(value));
        this._put(value, count * -1);
//        this.calculateInternalValues();
    }
    
    /**
     * Decrement the count for the given value by one in the histogram
     * @param value
     */
    public synchronized void remove(X value) {
        this._put(value, -1);
        this.calculateInternalValues();
    }
    
    /**
     * Remove the entire count for the given value
     * @param value
     */
    public synchronized void removeAll(X value) {
        Integer cnt = this.histogram.get(value);
        if (cnt != null && cnt > 0) {
            this._put(value, cnt * -1);
//            this.calculateInternalValues();
        }
    }
    
    /**
     * For each value in the given collection, decrement their count by one for each
     * @param <T>
     * @param values
     */
    public synchronized void removeValues(Collection<X> values) {
        this.removeValues(values, 1);
    }

    /**
     * For each value in the given collection, decrement their count by the given delta
     * @param values
     * @param delta
     */
    public synchronized void removeValues(Collection<X> values, int delta) {
        for (X v : values) {
            this._put(v, -1 * delta);
        } // FOR
    }
    
    /**
     * Decrement all the entries in the other histogram by their counter
     * @param <T>
     * @param values
     */
    public synchronized void removeHistogram(Histogram<X> other) {
        for (Entry<X, Integer> e : other.histogram.entrySet()) {
            if (e.getValue() > 0) this._put(e.getKey(), -1 * e.getValue());
        } // FOR
//        this.calculateInternalValues();
    }

    /**
     * Returns the current count for the given value
     * If the value was never entered into the histogram, then the count will be null
     * @param value
     * @return
     */
    public Integer get(X value) {
        Integer count = histogram.get(value); 
        return (count); //  == null ? 0 : count);
    }
    
    /**
     * Returns the current count for the given value.
     * If that value was nevered entered in the histogram, then the value returned will be value_if_null 
     * @param value
     * @param value_if_null
     * @return
     */
    public int get(X value, int value_if_null) {
        Integer count = histogram.get(value);
        return (count == null ? value_if_null : count);
    }
    
    /**
     * Returns true if this histogram contains the specified key.
     * @param value
     * @return
     */
    public boolean contains(X value) {
        return (this.histogram.containsKey(value));
    }
    
    // ----------------------------------------------------------------------------
    // DEBUG METHODS
    // ----------------------------------------------------------------------------

    /**
     * Histogram Pretty Print
     */
    public String toString() {
        return (this.toString(MAX_CHARS, MAX_VALUE_LENGTH));
    }
    
    /**
     * Histogram Pretty Print
     * @param max_chars size of the bars
     * @return
     */
    public String toString(Integer max_chars) {
        return (this.toString(max_chars, MAX_VALUE_LENGTH));
    }
        
    /**
     * Histogram Pretty Print
     * @param max_chars
     * @param max_length
     * @return
     */
    public synchronized String toString(Integer max_chars, Integer max_length) {
        StringBuilder s = new StringBuilder();
        if (max_length == null) max_length = MAX_VALUE_LENGTH;
        
        this.calculateInternalValues();
        
        // Figure out the max size of the counts
        int max_ctr_length = 4;
        for (Integer ctr : this.histogram.values()) {
            max_ctr_length = Math.max(max_ctr_length, ctr.toString().length());
        } // FOR
        
        // Don't let anything go longer than MAX_VALUE_LENGTH chars
        String f = "%-" + max_length + "s [%" + max_ctr_length + "d] ";
        boolean first = true;
        boolean has_labels = this.hasDebugLabels();
        for (Object value : this.histogram.keySet()) {
            if (!first) s.append("\n");
            String str = null;
            if (has_labels) str = this.debug_names.get(value);
            if (str == null) str = (value != null ? value.toString() : "null");
            int value_str_len = str.length();
            if (value_str_len > max_length) str = str.substring(0, max_length - 3) + "...";
            
            int cnt = (value != null ? this.histogram.get(value).intValue() : 0);
            int chars = (int)((cnt / (double)this.max_count) * max_chars);
            s.append(String.format(f, str, cnt));
            for (int i = 0; i < chars; i++) s.append(MARKER);
            first = false;
        } // FOR
        if (this.histogram.isEmpty()) s.append("<EMPTY>");
        return (s.toString());
    }
    
    // ----------------------------------------------------------------------------
    // SERIALIZATION METHODS
    // ----------------------------------------------------------------------------

    @Override
    public void load(String input_path) throws IOException {
         JSONUtil.load(this, input_path);
    }
    
    @Override
    public void save(String output_path) throws IOException {
        JSONUtil.save(this, output_path);
    }
    
    @Override
    public String toJSONString() {
        return (JSONUtil.toJSONString(this));
    }
    
    @Override
    public void toJSON(JSONStringer stringer) throws JSONException {
        Class<?> value_type = null;
        for (Members element : Histogram.Members.values()) {
            if (element == Histogram.Members.VALUE_TYPE) continue;
            try {
                Field field = Histogram.class.getDeclaredField(element.toString().toLowerCase());
                if (element == Members.HISTOGRAM) {
                    stringer.key(Members.HISTOGRAM.name()).object();
                    for (Object value : this.histogram.keySet()) {
                        if (value != null && value_type == null) value_type = value.getClass();
                        stringer.key(value.toString()).value(this.histogram.get(value));
                    } // FOR
                    stringer.endObject();
                } else if (element == Members.KEEP_ZERO_ENTRIES) {
                    if (this.keep_zero_entries) stringer.key(element.name()).value(this.keep_zero_entries);
                } else {
                    stringer.key(element.name()).value(field.get(this));
                }
            } catch (Exception ex) {
                ex.printStackTrace();
                System.exit(1);
            }
        } // FOR
        if (value_type != null) {
            stringer.key(Histogram.Members.VALUE_TYPE.name()).value(value_type.getCanonicalName());
        }
    }
    
    @Override
    public void fromJSON(JSONObject object) throws JSONException {
        if (object.has(Members.KEEP_ZERO_ENTRIES.name())) {
            this.setKeepZeroEntries(object.getBoolean(Members.KEEP_ZERO_ENTRIES.name()));
        }
        Class<?> value_type = null;
        if (object.has(Members.VALUE_TYPE.name())) {
            String className = object.getString(Members.VALUE_TYPE.name());
            value_type = ClassUtil.getClass(className);
            assert(value_type != null) : "Invalid VALUE_TYPE '" + className + "'";
        }
        
        // This code sucks ass...
        for (Members element : Histogram.Members.values()) {
            if (element == Members.KEEP_ZERO_ENTRIES || element == Members.VALUE_TYPE) continue;
            try {
                String field_name = element.toString().toLowerCase();
                Field field = Histogram.class.getDeclaredField(field_name);
                if (element == Members.HISTOGRAM) {
                    JSONObject jsonObject = object.getJSONObject(Members.HISTOGRAM.name());
                    Iterator<String> keys = jsonObject.keys();
                    while (keys.hasNext()) {
                        String key_name = keys.next();
                        @SuppressWarnings("unchecked")
                        X key_value = (X)JSONUtil.getPrimitiveValue(key_name, value_type);
                        int count = jsonObject.getInt(key_name);
                        this.histogram.put(key_value, count);
                    } // WHILE
                } else {
                    field.set(this, object.getInt(element.name()));
                }
            } catch (Exception ex) {
                ex.printStackTrace();
                System.exit(1);
            }
        } // FOR
        
        this.calculateInternalValues();
    }
}
