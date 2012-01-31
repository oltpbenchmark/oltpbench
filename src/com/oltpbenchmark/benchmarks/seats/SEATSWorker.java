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
/* This file is part of VoltDB. 
 * Copyright (C) 2009 Vertica Systems Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 * 
 * The above copyright notice and this permission notice shall be 
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR 
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.                       
 */

package com.oltpbenchmark.benchmarks.seats;

import java.sql.Date;
import java.sql.SQLException;
import java.util.*;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.log4j.Logger;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.seats.procedures.DeleteReservation;
import com.oltpbenchmark.benchmarks.seats.procedures.FindFlights;
import com.oltpbenchmark.benchmarks.seats.procedures.FindOpenSeats;
import com.oltpbenchmark.benchmarks.seats.procedures.NewReservation;
import com.oltpbenchmark.benchmarks.seats.procedures.UpdateCustomer;
import com.oltpbenchmark.benchmarks.seats.procedures.UpdateReservation;
import com.oltpbenchmark.benchmarks.seats.util.CustomerId;
import com.oltpbenchmark.benchmarks.seats.util.FlightId;
import com.oltpbenchmark.types.TransactionStatus;
import com.oltpbenchmark.util.RandomGenerator;
import com.oltpbenchmark.util.StringUtil;

public class SEATSWorker extends Worker {
    private static final Logger LOG = Logger.getLogger(SEATSWorker.class);

    /**
     * Airline Benchmark Transactions
     */
    public static enum Transaction {
        DeleteReservation   (DeleteReservation.class),
        FindFlights         (FindFlights.class),
        FindOpenSeats       (FindOpenSeats.class),
        NewReservation      (NewReservation.class),
        UpdateCustomer      (UpdateCustomer.class),
        UpdateReservation   (UpdateReservation.class);
        
        private Transaction(Class<? extends Procedure> proc_class) {
            this.proc_class = proc_class;
            this.execName = proc_class.getSimpleName();
            this.displayName = StringUtil.title(this.name().replace("_", " "));
        }

        public final Class<? extends Procedure> proc_class;
        public final String displayName;
        public final String execName;
        
        protected static final Map<Integer, Transaction> idx_lookup = new HashMap<Integer, Transaction>();
        protected static final Map<String, Transaction> name_lookup = new HashMap<String, Transaction>();
        static {
            for (Transaction vt : EnumSet.allOf(Transaction.class)) {
                Transaction.idx_lookup.put(vt.ordinal(), vt);
                Transaction.name_lookup.put(vt.name().toLowerCase().intern(), vt);
            }
        }
        
        public static Transaction get(Integer idx) {
            assert(idx >= 0);
            return (Transaction.idx_lookup.get(idx));
        }

        public static Transaction get(String name) {
            return (Transaction.name_lookup.get(name.toLowerCase().intern()));
        }
        public String getDisplayName() {
            return (this.displayName);
        }
        public String getExecName() {
            return (this.execName);
        }
    }
    
    // -----------------------------------------------------------------
    // RESERVED SEAT BITMAPS
    // -----------------------------------------------------------------
    
    public enum CacheType {
        PENDING_INSERTS     (SEATSConstants.CACHE_LIMIT_PENDING_INSERTS),
        PENDING_UPDATES     (SEATSConstants.CACHE_LIMIT_PENDING_UPDATES),
        PENDING_DELETES     (SEATSConstants.CACHE_LIMIT_PENDING_DELETES),
        ;
        
        private CacheType(int limit) {
            this.limit = limit;
//            this.lock = new ReentrantLock();
        }
        
        private final int limit;
//        private final ReentrantLock lock;
    }
    
    protected final Map<CacheType, LinkedList<Reservation>> CACHE_RESERVATIONS = new HashMap<SEATSWorker.CacheType, LinkedList<Reservation>>();
    {
        for (CacheType ctype : CacheType.values()) {
            CACHE_RESERVATIONS.put(ctype, new LinkedList<Reservation>());
        } // FOR
    } // STATIC 
    
    
    protected final Map<CustomerId, Set<FlightId>> CACHE_CUSTOMER_BOOKED_FLIGHTS = new HashMap<CustomerId, Set<FlightId>>();
    protected final Map<FlightId, BitSet> CACHE_BOOKED_SEATS = new HashMap<FlightId, BitSet>();

    private static final BitSet FULL_FLIGHT_BITSET = new BitSet(SEATSConstants.NUM_SEATS_PER_FLIGHT);
    static {
        for (int i = 0; i < SEATSConstants.NUM_SEATS_PER_FLIGHT; i++)
            FULL_FLIGHT_BITSET.set(i);
    } // STATIC
    
    protected BitSet getSeatsBitSet(FlightId flight_id) {
        BitSet seats = CACHE_BOOKED_SEATS.get(flight_id);
        if (seats == null) {
//            synchronized (CACHE_BOOKED_SEATS) {
                seats = CACHE_BOOKED_SEATS.get(flight_id);
                if (seats == null) {
                    seats = new BitSet(SEATSConstants.NUM_SEATS_PER_FLIGHT);
                    CACHE_BOOKED_SEATS.put(flight_id, seats);
                }
//            } // SYNCH
        }
        return (seats);
    }
    
    /**
     * Returns true if the given BitSet for a Flight has all of its seats reserved 
     * @param seats
     * @return
     */
    protected boolean isFlightFull(BitSet seats) {
        assert(FULL_FLIGHT_BITSET.size() == seats.size());
        return FULL_FLIGHT_BITSET.equals(seats);
    }
    
    /**
     * Returns true if the given Customer already has a reservation booked on the target Flight
     * @param customer_id
     * @param flight_id
     * @return
     */
    protected boolean isCustomerBookedOnFlight(CustomerId customer_id, FlightId flight_id) {
        Set<FlightId> flights = CACHE_CUSTOMER_BOOKED_FLIGHTS.get(customer_id);
        return (flights != null && flights.contains(flight_id));
    }

    /**
     * Returns the set of Customers that are waiting to be added the given Flight
     * @param flight_id
     * @return
     */
//    protected Set<CustomerId> getPendingCustomers(FlightId flight_id) {
        
//        return (this.tmp_customers);
//    }
    
    /**
     * Returns true if the given Customer is pending to be booked on the given Flight
     * @param customer_id
     * @param flight_id
     * @return
     */
    protected boolean isCustomerPendingOnFlight(CustomerId customer_id, FlightId flight_id) {
        for (Reservation r : CACHE_RESERVATIONS.get(CacheType.PENDING_INSERTS)) {
            if (r.flight_id.equals(flight_id) && r.customer_id.equals(customer_id)) {
                return (true);
            }
        } // FOR
        return (false);
    }
    
    protected Set<FlightId> getCustomerBookedFlights(CustomerId customer_id) {
        Set<FlightId> f_ids = CACHE_CUSTOMER_BOOKED_FLIGHTS.get(customer_id);
        if (f_ids == null) {
//            synchronized (CACHE_CUSTOMER_BOOKED_FLIGHTS) {
                f_ids = CACHE_CUSTOMER_BOOKED_FLIGHTS.get(customer_id);
                if (f_ids == null) {
                    f_ids = new HashSet<FlightId>();
                    CACHE_CUSTOMER_BOOKED_FLIGHTS.put(customer_id, f_ids);
                }
//            } // SYNCH
        }
        return (f_ids);
    }
    
    @Override
    public String toString() {
        Map<String, Object> m = new ListOrderedMap<String, Object>();
        for (CacheType ctype : CACHE_RESERVATIONS.keySet()) {
            m.put(ctype.name(), CACHE_RESERVATIONS.get(ctype));
        } // FOR
        m.put("CACHE_CUSTOMER_BOOKED_FLIGHTS", CACHE_CUSTOMER_BOOKED_FLIGHTS); 
        m.put("CACHE_BOOKED_SEATS", CACHE_BOOKED_SEATS); 
        
        return StringUtil.formatMaps(m);
    }
    
    // -----------------------------------------------------------------
    // ADDITIONAL DATA MEMBERS
    // -----------------------------------------------------------------
    
    private final SEATSProfile profile;
    private final RandomGenerator rng;
    private final Set<CustomerId> tmp_customers = new HashSet<CustomerId>();
    private final List<Reservation> tmp_reservations = new ArrayList<Reservation>();
    private final Set<Integer> tmp_emptySeats = new HashSet<Integer>();
    
    /**
     * When a customer looks for an open seat, they will then attempt to book that seat in
     * a new reservation. Some of them will want to change their seats. This data structure
     * represents a customer that is queued to change their seat. 
     */
    protected static class Reservation {
        public final long id;
        public final FlightId flight_id;
        public final CustomerId customer_id;
        public final int seatnum;
        
        public Reservation(long id, FlightId flight_id, CustomerId customer_id, int seatnum) {
            this.id = id;
            this.flight_id = flight_id;
            this.customer_id = customer_id;
            this.seatnum = seatnum;
            assert(this.seatnum >= 0) : "Invalid seat number\n" + this;
            assert(this.seatnum < SEATSConstants.NUM_SEATS_PER_FLIGHT) : "Invalid seat number\n" + this;
        }
        
        @Override
        public boolean equals(Object obj) {
            if (obj instanceof Reservation) {
                Reservation r = (Reservation)obj;
                // Ignore id!
                return (this.seatnum == r.seatnum &&
                        this.flight_id.equals(r.flight_id) &&
                        this.customer_id.equals(r.customer_id));
                        
            }
            return (false);
        }
        
        @Override
        public String toString() {
            return String.format("{Id:%d / %s / %s / SeatNum:%d}",
                                 this.id, this.flight_id, this.customer_id, this.seatnum);
        }
    } // END CLASS

    // -----------------------------------------------------------------
    // REQUIRED METHODS
    // -----------------------------------------------------------------

    public SEATSWorker(int id, SEATSBenchmark benchmark) {
        super(benchmark, id);

        this.rng = benchmark.getRandomGenerator();
        this.profile = new SEATSProfile(benchmark, rng); 
    }
    
    protected void initialize() {
        try {
            this.profile.loadProfile(this.conn);
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
        if (LOG.isTraceEnabled()) LOG.trace("Airport Max Customer Id:\n" + this.profile.airport_max_customer_id);
        
        // Make sure we have the information we need in the BenchmarkProfile
        String error_msg = null;
        if (this.profile.getFlightIdCount() == 0) {
            error_msg = "The benchmark profile does not have any flight ids.";
        } else if (this.profile.getCustomerIdCount() == 0) {
            error_msg = "The benchmark profile does not have any customer ids.";
        } else if (this.profile.getFlightStartDate() == null) {
            error_msg = "The benchmark profile does not have a valid flight start date.";
        }
        if (error_msg != null) throw new RuntimeException(error_msg);
        
        // Fire off a FindOpenSeats so that we can prime ourselves
        FindOpenSeats proc = this.getProcedure(FindOpenSeats.class);
        try {
            boolean ret = this.executeFindOpenSeats(proc);
            assert(ret);
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    protected TransactionStatus executeWork(TransactionType txnType) throws UserAbortException, SQLException {
        Transaction txn = Transaction.get(txnType.getName());
        assert(txn != null) : "Unexpected " + txnType;
        
        // Get the Procedure handle
        Procedure proc = this.getProcedure(txnType);
        assert(proc != null) : String.format("Failed to get Procedure handle for %s.%s",
                                             this.benchmarkModule.getBenchmarkName(), txnType);
        if (LOG.isDebugEnabled())
            LOG.debug("Attempting to execute " + proc);
        boolean ret = false;
        switch (txn) {
            case DeleteReservation: {
                ret = this.executeDeleteReservation((DeleteReservation)proc);
                break;
            }
            case FindFlights: {
                ret = this.executeFindFlights((FindFlights)proc);
                break;
            }
            case FindOpenSeats: {
                ret = this.executeFindOpenSeats((FindOpenSeats)proc);
                break;
            }
            case NewReservation: {
                ret = this.executeNewReservation((NewReservation)proc);
                break;
            }
            case UpdateCustomer: {
                ret = this.executeUpdateCustomer((UpdateCustomer)proc);
                break;
            }
            case UpdateReservation: {
                ret = this.executeUpdateReservation((UpdateReservation)proc);
                break;
            }
            default:
                assert(false) : "Unexpected transaction: " + txn; 
        } // SWITCH
        if (ret == false) {
            if (LOG.isDebugEnabled())
                LOG.debug("Unable to execute " + proc + " right now");
            return (TransactionStatus.RETRY_DIFFERENT);
        }
        
        if (ret && LOG.isDebugEnabled())
            LOG.debug("Executed a new invocation of " + txn);
        return (TransactionStatus.SUCCESS);
    }
    
//    @Override
//    public void tick(int counter) {
//        super.tick(counter);
//        for (CacheType ctype : CACHE_RESERVATIONS.keySet()) {
//            ctype.lock.lock();
//            try {
//                List<Reservation> cache = CACHE_RESERVATIONS.get(ctype);
//                int before = cache.size();
//                if (before > ctype.limit) {
//                    Collections.shuffle(cache, rng);
//                    while (cache.size() > ctype.limit) {
//                        cache.remove(0);
//                    } // WHILE
//                    if (LOG.isDebugEnabled()) LOG.debug(String.format("Pruned records from cache [newSize=%d, origSize=%d]",
//                                               cache.size(), before));
//                } // IF
//            } finally {
//                ctype.lock.unlock();
//            } // SYNCH
//        } // FOR
//        
//        if (this.getId() == 0) {
//            LOG.info("NewReservation Errors:\n" + newReservationErrors);
//            newReservationErrors.clear();
//        }
//    }
    
    /**
     * Take an existing Reservation that we know is legit and randomly decide to 
     * either queue it for a later update or delete transaction 
     * @param r
     */
    protected void requeueReservation(Reservation r) {
        CacheType ctype = null;
        
        // Queue this motha trucka up for a deletin'
        if (rng.nextBoolean()) {
            ctype = CacheType.PENDING_DELETES;
        } else {
            ctype = CacheType.PENDING_UPDATES;
        }
        assert(ctype != null);
        
        LinkedList<Reservation> cache = CACHE_RESERVATIONS.get(ctype);
        assert(cache != null);
        cache.add(r);
        if (LOG.isDebugEnabled())
            LOG.debug(String.format("Queued %s for %s [cache=%d]", r, ctype, cache.size()));
        
        while (cache.size() > ctype.limit) {
            cache.remove();
        }
    }
    
    // -----------------------------------------------------------------
    // DeleteReservation
    // -----------------------------------------------------------------

    private boolean executeDeleteReservation(DeleteReservation proc) throws SQLException {
        // Pull off the first cached reservation and drop it on the cluster...
        final Reservation r = CACHE_RESERVATIONS.get(CacheType.PENDING_DELETES).poll();
        if (r == null) {
            return (false);
        }
        int rand = rng.number(1, 100);
        
        // Parameters
        long f_id = r.flight_id.encode();
        Long c_id = null;
        String c_id_str = null;
        String ff_c_id_str = null;
        Long ff_al_id = null;
        
        // Delete with the Customer's id as a string 
        if (rand <= SEATSConstants.PROB_DELETE_WITH_CUSTOMER_ID_STR) {
            c_id_str = Long.toString(r.customer_id.encode());
        }
        // Delete using their FrequentFlyer information
        else if (rand <= SEATSConstants.PROB_DELETE_WITH_CUSTOMER_ID_STR + SEATSConstants.PROB_DELETE_WITH_FREQUENTFLYER_ID_STR) {
            ff_c_id_str = Long.toString(r.customer_id.encode());
            ff_al_id = r.flight_id.getAirlineId();
        }
        // Delete using their Customer id
        else {
            c_id = r.customer_id.encode();
        }
        
        if (LOG.isTraceEnabled()) LOG.trace("Calling " + proc);
        proc.run(conn, f_id, c_id, c_id_str, ff_c_id_str, ff_al_id);
        conn.commit();
        
        // We can remove this from our set of full flights because know that there is now a free seat
        BitSet seats = getSeatsBitSet(r.flight_id);
        seats.set(r.seatnum, false);
      
        // And then put it up for a pending insert
        if (rng.nextInt(100) < SEATSConstants.PROB_REQUEUE_DELETED_RESERVATION) {
//            CacheType.PENDING_INSERTS.lock.lock();
            try {
                CACHE_RESERVATIONS.get(CacheType.PENDING_INSERTS).add(r);
            } finally {
//                CacheType.PENDING_INSERTS.lock.unlock();
            } // SYNCH
        }

        return (true);
    }
    
    // ----------------------------------------------------------------
    // FindFlights
    // ----------------------------------------------------------------
    
    /**
     * Execute one of the FindFlight transactions
     * @param txn
     * @throws SQLException
     */
    private boolean executeFindFlights(FindFlights proc) throws SQLException {
        long depart_airport_id;
        long arrive_airport_id;
        Date start_date;
        Date stop_date;
        
        // Select two random airport ids
        if (rng.nextInt(100) < SEATSConstants.PROB_FIND_FLIGHTS_RANDOM_AIRPORTS) {
            // Does it matter whether the one airport actually flies to the other one?
            depart_airport_id = this.profile.getRandomAirportId();
            arrive_airport_id = this.profile.getRandomOtherAirport(depart_airport_id);
            
            // Select a random date from our upcoming dates
            start_date = this.profile.getRandomUpcomingDate();
            stop_date = new Date(start_date.getTime() + (SEATSConstants.MILLISECONDS_PER_DAY * 2));
        }
        
        // Use an existing flight so that we guaranteed to get back results
        else {
            FlightId flight_id = this.profile.getRandomFlightId();
            depart_airport_id = flight_id.getDepartAirportId();
            arrive_airport_id = flight_id.getArriveAirportId();
            
            Date flightDate = flight_id.getDepartDate(this.profile.getFlightStartDate());
            long range = Math.round(SEATSConstants.MILLISECONDS_PER_DAY * 0.5);
            start_date = new Date(flightDate.getTime() - range);
            stop_date = new Date(flightDate.getTime() + range);
            
            if (LOG.isDebugEnabled())
                LOG.debug(String.format("Using %s as look up in %s: %d / %s",
                                        flight_id, proc, flight_id.encode(), flightDate));
        }
        
        // If distance is greater than zero, then we will also get flights from nearby airports
        long distance = -1;
        if (rng.nextInt(100) < SEATSConstants.PROB_FIND_FLIGHTS_NEARBY_AIRPORT) {
            distance = SEATSConstants.DISTANCES[rng.nextInt(SEATSConstants.DISTANCES.length)];
        }
        
        if (LOG.isTraceEnabled()) LOG.trace("Calling " + proc);
        List<Object[]> results = proc.run(conn,
                                          depart_airport_id,
                                          arrive_airport_id,
                                          start_date,
                                          stop_date,
                                          distance);
        conn.commit();
        
        if (results.size() > 1) {
            // Convert the data into a FlightIds that other transactions can use
            int ctr = 0;
            for (Object row[] : results) {
                FlightId flight_id = new FlightId((Long)row[0]);
                assert(flight_id != null);
                boolean added = profile.addFlightId(flight_id);
                if (added) ctr++;
            } // WHILE
            if (LOG.isDebugEnabled()) LOG.debug(String.format("Added %d out of %d FlightIds to local cache",
                                                ctr, results.size()));
        }
        return (true);
    }

    // ----------------------------------------------------------------
    // FindOpenSeats
    // ----------------------------------------------------------------

    /**
     * Execute the FindOpenSeat procedure
     * @throws SQLException
     */
    private boolean executeFindOpenSeats(FindOpenSeats proc) throws SQLException {
        final FlightId search_flight = this.profile.getRandomFlightId();
        assert(search_flight != null);
        
        if (LOG.isTraceEnabled()) LOG.trace("Calling " + proc);
        Object[][] results = proc.run(conn, search_flight.encode());
        conn.commit();
        
        int rowCount = results.length;
        assert (rowCount <= SEATSConstants.NUM_SEATS_PER_FLIGHT) :
            String.format("Unexpected %d open seats returned for %s", rowCount, search_flight);
    
        // there is some tiny probability of an empty flight .. maybe 1/(20**150)
        // if you hit this assert (with valid code), play the lottery!
        if (rowCount == 0) return (true);

        LinkedList<Reservation> cache = CACHE_RESERVATIONS.get(CacheType.PENDING_INSERTS);
        assert(cache != null) : "Unexpected " + CacheType.PENDING_INSERTS;
        
        // Store pending reservations in our queue for a later transaction            
        tmp_emptySeats.clear();
        tmp_reservations.clear();
//        this.tmp_customers.clear();
//        for (Reservation r : cache) {
//            if (r.flight_id.equals(search_flight)) {
//                this.tmp_customers.add(r.customer_id);
//            }
//        } // FOR
//        cache.removeAll(tmp_customers);
        
        FlightId flight_id = new FlightId();
        for (Object row[] : results) {
            if (row == null) continue; //  && rng.nextInt(100) < 75) continue; // HACK
            
            flight_id.set((Long)row[0]);
            assert(flight_id.equals(search_flight));
            Integer seatnum = (Integer)row[1];
            long airport_depart_id = flight_id.getDepartAirportId();
          
            // We first try to get a CustomerId based at this departure airport
            if (LOG.isTraceEnabled())
                LOG.trace("Looking for a random customer to fly on " + flight_id);
            CustomerId customer_id = profile.getRandomCustomerId(airport_depart_id);
          
            // We will go for a random one if:
            //  (1) The Customer is already booked on this Flight
            //  (2) We already made a new Reservation just now for this Customer
            int tries = SEATSConstants.NUM_SEATS_PER_FLIGHT;
            while (tries-- > 0 && (customer_id == null || isCustomerBookedOnFlight(customer_id, flight_id))) {
                customer_id = profile.getRandomCustomerId();
                if (LOG.isTraceEnabled())
                    LOG.trace("RANDOM CUSTOMER: " + customer_id);
            } // WHILE
            assert(customer_id != null) :
                String.format("Failed to find a unique Customer to reserve for seat #%d on %s", seatnum, flight_id);
    
//            tmp_customers.add(customer_id);
            tmp_emptySeats.add(seatnum);
            tmp_reservations.add(new Reservation(profile.getNextReservationId(getId()), flight_id, customer_id, (int)seatnum));
            if (LOG.isTraceEnabled())
                LOG.trace("QUEUED INSERT: " + flight_id + " / " + flight_id.encode() + " -> " + customer_id);
        } // WHILE
      
        if (tmp_reservations.isEmpty() == false) {
            Collections.shuffle(tmp_reservations);
            cache.addAll(tmp_reservations);
            while (cache.size() > SEATSConstants.CACHE_LIMIT_PENDING_INSERTS) {
                cache.remove();
            } // WHILE
            if (LOG.isDebugEnabled())
                LOG.debug(String.format("Stored %d pending inserts for %s [totalPendingInserts=%d]",
                          tmp_reservations.size(), search_flight, cache.size()));
        }
        BitSet seats = getSeatsBitSet(search_flight);
        for (int i = 0; i < SEATSConstants.NUM_SEATS_PER_FLIGHT; i++) {
            if (tmp_emptySeats.contains(i) == false) {
                seats.set(i);
            }
        } // FOR
        
        return (true);
    }
    
    // ----------------------------------------------------------------
    // NewReservation
    // ----------------------------------------------------------------
    
    private boolean executeNewReservation(NewReservation proc) throws SQLException {
        Reservation reservation = null;
        BitSet seats = null;
        LinkedList<Reservation> cache = CACHE_RESERVATIONS.get(CacheType.PENDING_INSERTS);
        assert(cache != null) : "Unexpected " + CacheType.PENDING_INSERTS;
        
        if (LOG.isDebugEnabled()) LOG.debug(String.format("Attempting to get a new pending insert Reservation [totalPendingInserts=%d]",
                                                 cache.size()));
        while (reservation == null) {
            Reservation r = null;
//            CacheType.PENDING_INSERTS.lock.lock();
            try {
                r = cache.poll();
            } finally {
//                CacheType.PENDING_INSERTS.lock.unlock();
            } // SYNCH
            if (r == null) break;
            
            seats = getSeatsBitSet(r.flight_id);
            
            if (isFlightFull(seats)) {
                if (LOG.isDebugEnabled()) LOG.debug(String.format("%s is full", r.flight_id));
                continue;
            }
            else if (seats.get(r.seatnum)) {
                if (LOG.isDebugEnabled()) LOG.debug(String.format("Seat #%d on %s is already booked", r.seatnum, r.flight_id));
                continue;
            }
            else if (isCustomerBookedOnFlight(r.customer_id, r.flight_id)) {
                if (LOG.isDebugEnabled()) LOG.debug(String.format("%s is already booked on %s", r.customer_id, r.flight_id));
                continue;
            }
            reservation = r; 
        } // WHILE
        if (reservation == null) {
            if (LOG.isDebugEnabled()) LOG.debug("Failed to find a valid pending insert Reservation\n" + this.toString());
            return (false);
        }
        
        // Generate a random price for now
        double price = 2.0 * rng.number(SEATSConstants.MIN_RESERVATION_PRICE,
                                        SEATSConstants.MAX_RESERVATION_PRICE);
        
        // Generate random attributes
        long attributes[] = new long[9];
        for (int i = 0; i < attributes.length; i++) {
            attributes[i] = rng.nextLong();
        } // FOR
        
        if (LOG.isTraceEnabled()) LOG.trace("Calling " + proc);
        proc.run(conn, reservation.id,
                       reservation.customer_id.encode(),
                       reservation.flight_id.encode(),
                       reservation.seatnum,
                       price,
                       attributes);
        conn.commit();
        
        // Mark this seat as successfully reserved
        seats.set(reservation.seatnum);
        
        // Set it up so we can play with it later
        this.requeueReservation(reservation);
        
        return (true);
    }

    // ----------------------------------------------------------------
    // UpdateCustomer
    // ----------------------------------------------------------------
    
    private boolean executeUpdateCustomer(UpdateCustomer proc) throws SQLException {
        // Pick a random customer and then have at it!
        CustomerId customer_id = this.profile.getRandomCustomerId();
        
        Long c_id = null;
        String c_id_str = null;
        long attr0 = this.rng.nextLong();
        long attr1 = this.rng.nextLong();
        long update_ff = (rng.number(1, 100) <= SEATSConstants.PROB_UPDATE_FREQUENT_FLYER ? 1 : 0);
        
        // Update with the Customer's id as a string 
        if (rng.nextInt(100) < SEATSConstants.PROB_UPDATE_WITH_CUSTOMER_ID_STR) {
            c_id_str = Long.toString(customer_id.encode());
        }
        // Update using their Customer id
        else {
            c_id = customer_id.encode();
        }

        if (LOG.isTraceEnabled()) LOG.trace("Calling " + proc);
        proc.run(conn, c_id, c_id_str, update_ff, attr0, attr1);
        conn.commit();
        
        return (true);
    }

    // ----------------------------------------------------------------
    // UpdateReservation
    // ----------------------------------------------------------------

    private boolean executeUpdateReservation(UpdateReservation proc) throws SQLException {
        LinkedList<Reservation> cache = CACHE_RESERVATIONS.get(CacheType.PENDING_UPDATES);
        assert(cache != null) : "Unexpected " + CacheType.PENDING_UPDATES;
        
        if (LOG.isTraceEnabled())
            LOG.trace("Let's look for a Reservation that we can update");
        
        // Pull off the first pending seat change and throw that ma at the server
        Reservation r = null;
        try {
            r = cache.poll();
        } catch (Throwable ex) {
            // Nothing
        }
        if (r == null) {
            if (LOG.isDebugEnabled())
                LOG.warn(String.format("Failed to find Reservation to update [cache=%d]", cache.size()));
            return (false);
        }
        if (LOG.isTraceEnabled())
            LOG.trace("Ok let's try to update " + r);
        
        long value = rng.number(1, 1 << 20);
        long attribute_idx = rng.nextInt(UpdateReservation.NUM_UPDATES);
        long seatnum = rng.number(0, SEATSConstants.NUM_SEATS_PER_FLIGHT-1);

        if (LOG.isTraceEnabled()) LOG.trace("Calling " + proc);
        proc.run(conn, r.id,
                       r.flight_id.encode(),
                       r.customer_id.encode(),
                       seatnum,
                       attribute_idx,
                       value);
        conn.commit();
        
        SEATSWorker.this.requeueReservation(r);
        return (true);
    }

}