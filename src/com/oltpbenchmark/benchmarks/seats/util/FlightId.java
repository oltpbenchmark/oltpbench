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
package com.oltpbenchmark.benchmarks.seats.util;

import java.sql.Date;

import com.oltpbenchmark.benchmarks.seats.SEATSConstants;

public class FlightId {
    
    private static final int MAX_VALUE = 65535; // 2^14 - 1
    private static final int VALUE_OFFSET = 16;

    /** 
     * The airline for this flight
     */
    private long airline_id;
    /**
     * The id of the departure airport
     */
    private long depart_airport_id;
    /**
     * The id of the arrival airport
     */
    private long arrive_airport_id;
    /**
     * This is the departure time of the flight in minutes since the benchmark start date
     * @see SEATSBaseClient.getFlightTimeInMinutes()
     */
    private long depart_date;

    private transient int hashCode = -1;
    
    public FlightId() {
        // Nothing...
    }
    
    /**
     * Constructor
     * @param airline_id - The airline for this flight
     * @param depart_airport_id - the id of the departure airport
     * @param arrive_airport_id - the id of the arrival airport
     * @param benchmark_start - the base date of when the benchmark data starts (including past days)
     * @param flight_date - when departure date of the flight
     */
    public FlightId(long airline_id, long depart_airport_id, long arrive_airport_id, Date benchmark_start, Date flight_date) {
        this.airline_id = airline_id;
        this.depart_airport_id = depart_airport_id;
        this.arrive_airport_id = arrive_airport_id;
        this.depart_date = FlightId.calculateFlightDate(benchmark_start, flight_date);
    }
    
    /**
     * Constructor. Converts a composite id generated by encode() into the full object
     * @param composite_id
     */
    public FlightId(long composite_id) {
        long values[] = FlightId.decode(composite_id);
        this.airline_id = values[0];
        this.depart_airport_id = values[1];
        this.arrive_airport_id = values[2];
        this.depart_date = values[3];
    }
    
    /**
     * 
     * @param benchmark_start
     * @param flight_date
     * @return
     */
    protected static final long calculateFlightDate(Date benchmark_start, Date flight_date) {
        return (flight_date.getTime() - benchmark_start.getTime()) / 3600000; // 60s * 60m * 1000
    }
    
    /**
     * @return the id
     */
    public long getSEATSId() {
        return airline_id;
    }

    /**
     * @return the depart_airport_id
     */
    public long getDepartAirportId() {
        return depart_airport_id;
    }

    /**
     * @return the arrive_airport_id
     */
    public long getArriveAirportId() {
        return arrive_airport_id;
    }

    /**
     * @return the flight departure date
     */
    public Date getDepartDate(Date benchmark_start) {
        return (new Date(benchmark_start.getTime() + (this.depart_date * SEATSConstants.MILLISECONDS_PER_MINUTE * 60)));
    }
    
    public boolean isUpcoming(Date benchmark_start, long past_days) {
        Date depart_date = this.getDepartDate(benchmark_start);
        return ((depart_date.getTime() - benchmark_start.getTime()) >= (past_days * SEATSConstants.MILLISECONDS_PER_DAY)); 
    }
    
    public long encode() {
        return FlightId.encode(new long[]{ this.airline_id,
                                           this.depart_airport_id,
                                           this.arrive_airport_id,
                                           this.depart_date});
    }

    public static long encode(long...values) {
        assert(values.length == 4);
        for (int i = 0; i < values.length; i++) {
            assert(values[i] >= 0) : "FlightId value at position " + i + " is " + values[i];
            assert(values[i] < MAX_VALUE) : "FlightId value at position " + i + " is " + values[i] + ". Max value is " + MAX_VALUE;
        } // FOR
        
        long id = values[0];
        int offset = VALUE_OFFSET;
        // System.out.println("0: " + id);
        for (int i = 1; i < values.length; i++) {
            id = id | values[i]<<offset;
            // System.out.println(id + ": " + id + "  [offset=" + offset + ", value=" + values[i] + "]");
            offset += VALUE_OFFSET;
        }
        return (id);
    }
    
    public static long[] decode(long composite_id) {
        long values[] = new long[4];
        int offset = 0;
        for (int i = 0; i < values.length; i++) {
            values[i] = composite_id>>offset & MAX_VALUE;
            offset += VALUE_OFFSET;
        } // FOR
        return (values);
    }
    
    private void internalDecode(long composite_id) {
        long vals[] = decode(composite_id);
        this.airline_id = vals[0];
        this.depart_airport_id = vals[1];
        this.arrive_airport_id = vals[2];
        this.depart_date = vals[3];
    }
    
    @Override
    public String toString() {
        return String.format("FlightId{airline=%d,depart=%d,arrive=%d,date=%s}",
                             this.airline_id, this.depart_airport_id, this.arrive_airport_id, this.depart_date);
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof FlightId) {
            FlightId o = (FlightId)obj;
            return (this.airline_id == o.airline_id &&
                    this.depart_airport_id == o.depart_airport_id &&
                    this.arrive_airport_id == o.arrive_airport_id &&
                    this.depart_date == o.depart_date);
        }
        return (false);
    }
    
    @Override
    public int hashCode() {
        if (this.hashCode == -1) {
            this.hashCode = new Long(this.encode()).hashCode();
        }
        return (this.hashCode);
    }
    
    // -----------------------------------------------------------------
    // SERIALIZATION
    // -----------------------------------------------------------------
    
//    @Override
//    public void load(String input_path, Database catalog_db) throws IOException {
//        JSONUtil.load(this, catalog_db, input_path);
//    }
//    @Override
//    public void save(String output_path) throws IOException {
//        JSONUtil.save(this, output_path);
//    }
//    @Override
//    public String toJSONString() {
//        return (JSONUtil.toJSONString(this));
//    }
//    @Override
//    public void toJSON(JSONStringer stringer) throws JSONException {
//        stringer.key("ID").value(this.encode());
//    }
//    @Override
//    public void fromJSON(JSONObject json_object, Database catalog_db) throws JSONException {
//        this.internalDecode(json_object.getLong("ID"));
//    }
}
