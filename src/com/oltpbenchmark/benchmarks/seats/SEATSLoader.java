/******************************************************************************
 *  Copyright 2015 by OLTPBenchmark Project                                   *
 *                                                                            *
 *  Licensed under the Apache License, Version 2.0 (the "License");           *
 *  you may not use this file except in compliance with the License.          *
 *  You may obtain a copy of the License at                                   *
 *                                                                            *
 *    http://www.apache.org/licenses/LICENSE-2.0                              *
 *                                                                            *
 *  Unless required by applicable law or agreed to in writing, software       *
 *  distributed under the License is distributed on an "AS IS" BASIS,         *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  *
 *  See the License for the specific language governing permissions and       *
 *  limitations under the License.                                            *
 ******************************************************************************/

package com.oltpbenchmark.benchmarks.seats;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.commons.collections15.set.ListOrderedSet;
import org.apache.log4j.Logger;

import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.benchmarks.seats.util.CustomerId;
import com.oltpbenchmark.benchmarks.seats.util.CustomerIdIterable;
import com.oltpbenchmark.benchmarks.seats.util.DistanceUtil;
import com.oltpbenchmark.benchmarks.seats.util.FlightId;
import com.oltpbenchmark.benchmarks.seats.util.ReturnFlight;
import com.oltpbenchmark.benchmarks.seats.util.SEATSHistogramUtil;
import com.oltpbenchmark.catalog.Column;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.util.CollectionUtil;
import com.oltpbenchmark.util.Histogram;
import com.oltpbenchmark.util.Pair;
import com.oltpbenchmark.util.RandomDistribution;
import com.oltpbenchmark.util.RandomDistribution.Flat;
import com.oltpbenchmark.util.RandomDistribution.FlatHistogram;
import com.oltpbenchmark.util.RandomDistribution.Gaussian;
import com.oltpbenchmark.util.RandomDistribution.Zipf;
import com.oltpbenchmark.util.RandomGenerator;
import com.oltpbenchmark.util.SQLUtil;
import com.oltpbenchmark.util.StringUtil;
import com.oltpbenchmark.util.TableDataIterable;

public class SEATSLoader extends Loader<SEATSBenchmark> {
    private static final Logger LOG = Logger.getLogger(SEATSLoader.class);

    // -----------------------------------------------------------------
    // INTERNAL DATA MEMBERS
    // -----------------------------------------------------------------

    protected final SEATSProfile profile;

    /**
     * Mapping from Airports to their geolocation coordinates AirportCode ->
     * <Latitude, Longitude>
     */
    private final ListOrderedMap<String, Pair<Double, Double>> airport_locations = new ListOrderedMap<String, Pair<Double, Double>>();

    /**
     * AirportCode -> Set<AirportCode, Distance> Only store the records for
     * those airports in HISTOGRAM_FLIGHTS_PER_AIRPORT
     */
    private final Map<String, Map<String, Short>> airport_distances = new HashMap<String, Map<String, Short>>();

    /**
     * Store a list of FlightIds and the number of seats remaining for a
     * particular flight.
     */
    private final ListOrderedMap<FlightId, Short> seats_remaining = new ListOrderedMap<FlightId, Short>();

    /**
     * Counter for the number of tables that we have finished loading
     */
    private final AtomicInteger finished = new AtomicInteger(0);

    /**
     * A histogram of the number of flights in the database per airline code
     */
    private final Histogram<String> flights_per_airline = new Histogram<String>(true);

    private final RandomGenerator rng; // FIXME

    // -----------------------------------------------------------------
    // INITIALIZATION
    // -----------------------------------------------------------------

    public SEATSLoader(SEATSBenchmark benchmark, Connection c) {
        super(benchmark, c);

        this.rng = benchmark.getRandomGenerator();
        // TODO: Sync with the base class rng
        this.profile = new SEATSProfile(benchmark, this.rng);

        if (LOG.isDebugEnabled()) {
            LOG.debug("CONSTRUCTOR: " + SEATSLoader.class.getName());
        }
    }

    // -----------------------------------------------------------------
    // LOADING METHODS
    // -----------------------------------------------------------------

    @Override
    public List<LoaderThread> createLoaderThreads() throws SQLException {
        List<LoaderThread> threads = new ArrayList<LoaderThread>();

        // High level locking overview, where step N+1 depends on step N
        // and latches are countDown()'d from top to bottom:
        //
        // 1. [histLatch] Histograms will be loaded on their own
        //
        // FIXED TABLES [fixedLatch]
        // 2.
        // [countryLatch] Country will be loaded on their own
        // AIRPORT depends on COUNTRY
        // AIRLINE depends on COUNTRY
        //
        // 3. [scalePrepLatch]
        // We need to load fixed table data into histograms before we
        // start to load scaling tables
        //
        // SCALING TABLES
        // 4.
        // [custLatch] CUSTOMER depends on AIRPORT
        // [distanceLatch] AIRPORT_DISTANCE depends on AIRPORT
        // [flightLatch] FLIGHT depends on AIRLINE, AIRPORT, AIRPORT_DISTANCE
        //
        // 5. [loadLatch]
        // RESERVATIONS depends on FLIGHT, CUSTOMER
        // FREQUENT_FLYER depends on FLIGHT, CUSTOMER, AIRLINE
        //
        // Important note: FLIGHT must come before FREQUENT_FLYER so that
        // we can use the flights_per_airline histogram when
        // selecting an airline to create a new FREQUENT_FLYER
        // account for a CUSTOMER
        //
        // 6. Then we save the profile

        final CountDownLatch histLatch = new CountDownLatch(1);

        // 1. [histLatch] HISTOGRAMS
        threads.add(new LoaderThread() {
            @Override
            public void load(Connection conn) throws SQLException {
                SEATSLoader.this.loadHistograms();
                histLatch.countDown();
            }
        });

        final CountDownLatch fixedLatch = new CountDownLatch(3);
        final CountDownLatch countryLatch = new CountDownLatch(1);

        // 2. [countryLatch] COUNTRY
        threads.add(new LoaderThread() {
            @Override
            public void load(Connection conn) throws SQLException {
                try {
                    histLatch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }

                SEATSLoader.this.loadFixedTable(conn, SEATSConstants.TABLENAME_COUNTRY);
                fixedLatch.countDown();
                countryLatch.countDown();
            }
        });

        // 2. AIRPORT depends on COUNTRY
        threads.add(new LoaderThread() {
            @Override
            public void load(Connection conn) throws SQLException {
                try {
                    countryLatch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }

                SEATSLoader.this.loadFixedTable(conn, SEATSConstants.TABLENAME_AIRPORT);
                fixedLatch.countDown();
            }
        });

        // 2. AIRLINE depends on COUNTRY
        threads.add(new LoaderThread() {
            @Override
            public void load(Connection conn) throws SQLException {
                try {
                    countryLatch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }

                SEATSLoader.this.loadFixedTable(conn, SEATSConstants.TABLENAME_AIRLINE);
                fixedLatch.countDown();
            }
        });

        final CountDownLatch scalingPrepLatch = new CountDownLatch(1);

        // 3. [scalingPrepLatch] guards all of the fixed tables and should
        // be used from this point onwards instead of individual fixed locks
        threads.add(new LoaderThread() {
            @Override
            public void load(Connection conn) throws SQLException {
                try {
                    fixedLatch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }

                // Setup the # of flights per airline
                SEATSLoader.this.flights_per_airline.putAll(SEATSLoader.this.profile.getAirlineCodes(), 0);
                scalingPrepLatch.countDown();
            }
        });

        final CountDownLatch custLatch = new CountDownLatch(1);
        final CountDownLatch distanceLatch = new CountDownLatch(1);
        final CountDownLatch flightLatch = new CountDownLatch(1);

        // 4. [custLatch] CUSTOMER depends on AIRPORT
        threads.add(new LoaderThread() {
            @Override
            public void load(Connection conn) throws SQLException {
                try {
                    scalingPrepLatch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }

                SEATSLoader.this.loadScalingTable(conn, SEATSConstants.TABLENAME_CUSTOMER);
                custLatch.countDown();
            }
        });

        // 4. AIRPORT_DISTANCE depends on AIRPORT
        threads.add(new LoaderThread() {
            @Override
            public void load(Connection conn) throws SQLException {
                try {
                    scalingPrepLatch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }

                SEATSLoader.this.loadScalingTable(conn, SEATSConstants.TABLENAME_AIRPORT_DISTANCE);
                distanceLatch.countDown();
            }
        });

        // 4. [flightLatch] FLIGHT depends on AIRPORT_DISTANCE, AIRLINE, AIRPORT
        threads.add(new LoaderThread() {
            @Override
            public void load(Connection conn) throws SQLException {
                try {
                    distanceLatch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }

                SEATSLoader.this.loadScalingTable(conn, SEATSConstants.TABLENAME_FLIGHT);
                flightLatch.countDown();
            }
        });

        final CountDownLatch loadLatch = new CountDownLatch(2);

        // 5. RESERVATIONS depends on FLIGHT, CUSTOMER
        threads.add(new LoaderThread() {
            @Override
            public void load(Connection conn) throws SQLException {
                try {
                    flightLatch.await();
                    custLatch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }

                SEATSLoader.this.loadScalingTable(conn, SEATSConstants.TABLENAME_RESERVATION);
                loadLatch.countDown();
            }
        });

        // 5. FREQUENT_FLYER depends on FLIGHT, CUSTOMER, AIRLINE
        threads.add(new LoaderThread() {
            @Override
            public void load(Connection conn) throws SQLException {
                try {
                    flightLatch.await();
                    custLatch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }

                SEATSLoader.this.loadScalingTable(conn, SEATSConstants.TABLENAME_FREQUENT_FLYER);
                loadLatch.countDown();
            }
        });

        threads.add(new LoaderThread() {
            @Override
            public void load(Connection conn) throws SQLException {
                try {
                    loadLatch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }

                SEATSLoader.this.profile.saveProfile(conn);
            }
        });

        return threads;
    }

    /**
     * Load all the histograms used in the benchmark
     */
    protected void loadHistograms() {
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Loading in %d histograms from files stored in '%s'", SEATSConstants.HISTOGRAM_DATA_FILES.length, this.profile.airline_data_dir));
        }

        // Now load in the histograms that we will need for generating the
        // flight data
        for (String histogramName : SEATSConstants.HISTOGRAM_DATA_FILES) {
            if (this.profile.histograms.containsKey(histogramName)) {
                if (LOG.isDebugEnabled()) {
                    LOG.warn("Already loaded histogram '" + histogramName + "'. Skipping...");
                }
                continue;
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("Loading in histogram data file for '" + histogramName + "'");
            }
            Histogram<String> hist = null;

            try {
                // The Flights_Per_Airport histogram is actually a serialized
                // map that has a histogram
                // of the departing flights from each airport to all the others
                if (histogramName.equals(SEATSConstants.HISTOGRAM_FLIGHTS_PER_AIRPORT)) {
                    Map<String, Histogram<String>> m = SEATSHistogramUtil.loadAirportFlights(this.profile.airline_data_dir);
                    assert (m != null);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(String.format("Loaded %d airport flight histograms", m.size()));
                    }

                    // Store the airport codes information
                    this.profile.airport_histograms.putAll(m);

                    // We then need to flatten all of the histograms in this map
                    // into a single histogram that just counts the number of
                    // departing flights per airport. We will use this
                    // to get the distribution of where Customers are located
                    hist = new Histogram<String>();
                    for (Entry<String, Histogram<String>> e : m.entrySet()) {
                        hist.put(e.getKey(), e.getValue().getSampleCount());
                    } // FOR

                    // All other histograms are just serialized and can be
                    // loaded directly
                } else {
                    hist = SEATSHistogramUtil.loadHistogram(histogramName, this.profile.airline_data_dir, true);
                }
            } catch (Exception ex) {
                throw new RuntimeException("Failed to load histogram '" + histogramName + "'", ex);
            }
            assert (hist != null);
            this.profile.histograms.put(histogramName, hist);
            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format("Loaded histogram '%s' [sampleCount=%d, valueCount=%d]", histogramName, hist.getSampleCount(), hist.getValueCount()));
            }
        } // FOR

    }

    /**
     * The fixed tables are those that are generated from the static data files
     * The number of tuples in these tables will not change based on the scale
     * factor.
     *
     * @param catalog_db
     */
    protected void loadFixedTable(Connection conn, String table_name) {
        LOG.debug(String.format("Loading table '%s' from fixed file", table_name));
        try {
            Table catalog_tbl = this.benchmark.getTableCatalog(table_name);
            assert (catalog_tbl != null);
            Iterable<Object[]> iterable = this.getFixedIterable(catalog_tbl);
            this.loadTable(conn, catalog_tbl, iterable, 5000);
        } catch (Throwable ex) {
            throw new RuntimeException("Failed to load data files for fixed-sized table '" + table_name + "'", ex);
        }
    }

    /**
     * The scaling tables are things that we will scale the number of tuples
     * based on the given scaling factor at runtime
     *
     * @param catalog_db
     */
    protected void loadScalingTable(Connection conn, String table_name) {
        try {
            Table catalog_tbl = this.benchmark.getTableCatalog(table_name);
            assert (catalog_tbl != null);
            Iterable<Object[]> iterable = this.getScalingIterable(catalog_tbl);
            this.loadTable(conn, catalog_tbl, iterable, 5000);
        } catch (Throwable ex) {
            throw new RuntimeException("Failed to load data files for scaling-sized table '" + table_name + "'", ex);
        }
    }

    /**
     * @param catalog_tbl
     */
    public void loadTable(Connection conn, Table catalog_tbl, Iterable<Object[]> iterable, int batch_size) {
        // Special Case: Airport Locations
        final boolean is_airport = catalog_tbl.getName().equals(SEATSConstants.TABLENAME_AIRPORT);

        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Generating new records for table %s [batchSize=%d]", catalog_tbl.getName(), batch_size));
        }
        final List<Column> columns = catalog_tbl.getColumns();

        // Check whether we have any special mappings that we need to maintain
        Map<Integer, Integer> code_2_id = new HashMap<Integer, Integer>();
        Map<Integer, Map<String, Long>> mapping_columns = new HashMap<Integer, Map<String, Long>>();
        for (int col_code_idx = 0, cnt = columns.size(); col_code_idx < cnt; col_code_idx++) {
            Column catalog_col = columns.get(col_code_idx);
            String col_name = catalog_col.getName();

            // Code Column -> Id Column Mapping
            // Check to see whether this table has columns that we need to map
            // their
            // code values to tuple ids
            String col_id_name = this.profile.code_columns.get(col_name);
            if (col_id_name != null) {
                Column catalog_id_col = catalog_tbl.getColumnByName(col_id_name);
                assert (catalog_id_col != null) : "The id column " + catalog_tbl.getName() + "." + col_id_name + " is missing";
                int col_id_idx = catalog_tbl.getColumnIndex(catalog_id_col);
                code_2_id.put(col_code_idx, col_id_idx);
            }

            // Foreign Key Column to Code->Id Mapping
            // If this columns references a foreign key that is used in the
            // Code->Id mapping that we generating above,
            // then we need to know when we should change the
            // column value from a code to the id stored in our lookup table
            if (this.profile.fkey_value_xref.containsKey(col_name)) {
                String col_fkey_name = this.profile.fkey_value_xref.get(col_name);
                mapping_columns.put(col_code_idx, this.profile.code_id_xref.get(col_fkey_name));
            }
        } // FOR

        int row_idx = 0;
        int row_batch = 0;

        try {
            String insert_sql = SQLUtil.getInsertSQL(catalog_tbl, this.getDatabaseType());
            PreparedStatement insert_stmt = conn.prepareStatement(insert_sql);
            int sqlTypes[] = catalog_tbl.getColumnTypes();

            for (Object tuple[] : iterable) {
                assert (tuple[0] != null) : "The primary key for " + catalog_tbl.getName() + " is null";

                // AIRPORT
                if (is_airport) {
                    // Skip any airport that does not have flights
                    int col_code_idx = catalog_tbl.getColumnByName("AP_CODE").getIndex();
                    if (this.profile.hasFlights((String) tuple[col_code_idx]) == false) {
                        if (LOG.isTraceEnabled()) {
                            LOG.trace(String.format("Skipping AIRPORT '%s' because it does not have any flights", tuple[col_code_idx]));
                        }
                        continue;
                    }

                    // Update the row # so that it matches
                    // what we're actually loading
                    int col_id_idx = catalog_tbl.getColumnByName("AP_ID").getIndex();
                    tuple[col_id_idx] = (long) (row_idx + 1);

                    // Store Locations
                    int col_lat_idx = catalog_tbl.getColumnByName("AP_LATITUDE").getIndex();
                    int col_lon_idx = catalog_tbl.getColumnByName("AP_LONGITUDE").getIndex();
                    Pair<Double, Double> coords = Pair.of((Double) tuple[col_lat_idx], (Double) tuple[col_lon_idx]);
                    if (coords.first == null || coords.second == null) {
                        LOG.error(Arrays.toString(tuple));
                    }
                    assert (coords.first != null) : String.format("Unexpected null latitude for airport '%s' [%d]", tuple[col_code_idx], col_lat_idx);
                    assert (coords.second != null) : String.format("Unexpected null longitude for airport '%s' [%d]", tuple[col_code_idx], col_lon_idx);
                    this.airport_locations.put(tuple[col_code_idx].toString(), coords);
                    if (LOG.isTraceEnabled()) {
                        LOG.trace(String.format("Storing location for '%s': %s", tuple[col_code_idx], coords));
                    }
                }

                // Code Column -> Id Column
                for (int col_code_idx : code_2_id.keySet()) {
                    assert (tuple[col_code_idx] != null) : String.format("The value of the code column at '%d' is null for %s\n%s", col_code_idx, catalog_tbl.getName(), Arrays.toString(tuple));
                    String code = tuple[col_code_idx].toString().trim();
                    if (code.length() > 0) {
                        Column from_column = columns.get(col_code_idx);
                        assert (from_column != null);
                        Column to_column = columns.get(code_2_id.get(col_code_idx));
                        assert (to_column != null) : String.format("Invalid column %s.%s", catalog_tbl.getName(), code_2_id.get(col_code_idx));
                        long id = (Long) tuple[code_2_id.get(col_code_idx)];
                        if (LOG.isTraceEnabled()) {
                            LOG.trace(String.format("Mapping %s '%s' -> %s '%d'", from_column.fullName(), code, to_column.fullName(), id));
                        }
                        this.profile.code_id_xref.get(to_column.getName()).put(code, id);
                    }
                } // FOR

                // Foreign Key Code -> Foreign Key Id
                for (int col_code_idx : mapping_columns.keySet()) {
                    Column catalog_col = columns.get(col_code_idx);
                    assert (tuple[col_code_idx] != null || catalog_col.isNullable()) : String.format("The code %s column at '%d' is null for %s id=%s\n%s", catalog_col.fullName(), col_code_idx,
                            catalog_tbl.getName(), tuple[0], Arrays.toString(tuple));
                    if (tuple[col_code_idx] != null) {
                        String code = tuple[col_code_idx].toString();
                        tuple[col_code_idx] = mapping_columns.get(col_code_idx).get(code);
                        if (LOG.isTraceEnabled()) {
                            LOG.trace(String.format("Mapped %s '%s' -> %s '%s'", catalog_col.fullName(), code, catalog_col.getForeignKey().fullName(), tuple[col_code_idx]));
                        }
                    }
                } // FOR

                for (int i = 0; i < tuple.length; i++) {
                    try {
                        if (tuple[i] != null) {
                            insert_stmt.setObject(i + 1, tuple[i]);
                        } else {
                            insert_stmt.setNull(i + 1, sqlTypes[i]);
                        }
                    } catch (SQLDataException ex) {
                        LOG.error("INVALID " + catalog_tbl.getName() + " TUPLE: " + Arrays.toString(tuple));
                        throw new RuntimeException("Failed to set value for " + catalog_tbl.getColumn(i).fullName(), ex);
                    }
                } // FOR
                insert_stmt.addBatch();
                row_idx++;

                if (++row_batch >= batch_size) {
                    LOG.debug(String.format("Loading %s batch [total=%d]", catalog_tbl.getName(), row_idx));
                    insert_stmt.executeBatch();
                    conn.commit();
                    insert_stmt.clearBatch();
                    row_batch = 0;
                }

            } // FOR

            if (row_batch > 0) {
                insert_stmt.executeBatch();
                conn.commit();
            }
            insert_stmt.close();
        } catch (Exception ex) {
            throw new RuntimeException("Failed to load table " + catalog_tbl.getName(), ex);
        }

        if (is_airport) {
            assert (this.profile.getAirportCount() == row_idx) : String.format("%d != %d", this.profile.getAirportCount(), row_idx);
        }

        // Record the number of tuples that we loaded for this table in the
        // profile
        if (catalog_tbl.getName().equals(SEATSConstants.TABLENAME_RESERVATION)) {
            this.profile.num_reservations = row_idx + 1;
        }

        LOG.info(String.format("Finished loading all %d tuples for %s [%d / %d]", row_idx, catalog_tbl.getName(), this.finished.incrementAndGet(), this.getCatalog().getTableCount()));
        return;
    }

    // ----------------------------------------------------------------
    // FIXED-SIZE TABLE DATA GENERATION
    // ----------------------------------------------------------------

    /**
     * @param catalog_tbl
     * @return
     * @throws Exception
     */
    protected Iterable<Object[]> getFixedIterable(Table catalog_tbl) throws Exception {
        File f = SEATSBenchmark.getTableDataFile(this.profile.airline_data_dir, catalog_tbl);
        TableDataIterable iterable = new FixedDataIterable(catalog_tbl, f);
        return (iterable);
    }

    /**
     * Wrapper around TableDataIterable that will populate additional random
     * fields
     */
    protected class FixedDataIterable extends TableDataIterable {
        private final Set<Integer> rnd_string = new HashSet<Integer>();
        private final Map<Integer, Integer> rnd_string_min = new HashMap<Integer, Integer>();
        private final Map<Integer, Integer> rnd_string_max = new HashMap<Integer, Integer>();
        private final Set<Integer> rnd_integer = new HashSet<Integer>();

        public FixedDataIterable(Table catalog_tbl, File filename) throws Exception {
            super(catalog_tbl, filename, true, true);

            // Figure out which columns are random integers and strings
            for (Column catalog_col : catalog_tbl.getColumns()) {
                String col_name = catalog_col.getName();
                int col_idx = catalog_col.getIndex();
                if (col_name.contains("_SATTR")) {
                    this.rnd_string.add(col_idx);
                    this.rnd_string_min.put(col_idx, SEATSLoader.this.rng.nextInt(catalog_col.getSize() - 1));
                    this.rnd_string_max.put(col_idx, catalog_col.getSize());
                } else if (col_name.contains("_IATTR")) {
                    this.rnd_integer.add(catalog_col.getIndex());
                }
            } // FOR
        }

        @Override
        public Iterator<Object[]> iterator() {
            // This is nasty old boy!
            return (new TableDataIterable.TableIterator() {

                @Override
                public Object[] next() {
                    Object[] tuple = super.next();

                    // Random String (*_SATTR##)
                    for (int col_idx : FixedDataIterable.this.rnd_string) {
                        int min_length = FixedDataIterable.this.rnd_string_min.get(col_idx);
                        int max_length = FixedDataIterable.this.rnd_string_max.get(col_idx);
                        tuple[col_idx] = SEATSLoader.this.rng.astring(min_length, max_length);
                    } // FOR
                      // Random Integer (*_IATTR##)
                    for (int col_idx : FixedDataIterable.this.rnd_integer) {
                        tuple[col_idx] = SEATSLoader.this.rng.nextLong();
                    } // FOR

                    return (tuple);
                }
            });
        }
    } // END CLASS

    // ----------------------------------------------------------------
    // SCALING TABLE DATA GENERATION
    // ----------------------------------------------------------------

    /**
     * Return an iterable that spits out tuples for scaling tables
     *
     * @param catalog_tbl
     *            the target table that we need an iterable for
     */
    protected Iterable<Object[]> getScalingIterable(Table catalog_tbl) {
        String name = catalog_tbl.getName().toUpperCase();
        ScalingDataIterable it = null;
        double scaleFactor = this.workConf.getScaleFactor();
        long num_customers = Math.round(SEATSConstants.CUSTOMERS_COUNT * scaleFactor);

        // Customers
        if (name.equals(SEATSConstants.TABLENAME_CUSTOMER)) {
            it = new CustomerIterable(catalog_tbl, num_customers);
        }
        // FrequentFlyer
        else if (name.equals(SEATSConstants.TABLENAME_FREQUENT_FLYER)) {
            it = new FrequentFlyerIterable(catalog_tbl, num_customers);
        }
        // Airport Distance
        else if (name.equals(SEATSConstants.TABLENAME_AIRPORT_DISTANCE)) {
            int max_distance = Integer.MAX_VALUE; // SEATSConstants.DISTANCES[SEATSConstants.DISTANCES.length
                                                  // - 1];
            it = new AirportDistanceIterable(catalog_tbl, max_distance);
        }
        // Flights
        else if (name.equals(SEATSConstants.TABLENAME_FLIGHT)) {
            it = new FlightIterable(catalog_tbl, (int) Math.round(SEATSConstants.FLIGHTS_DAYS_PAST * scaleFactor), (int) Math.round(SEATSConstants.FLIGHTS_DAYS_FUTURE * scaleFactor));
        }
        // Reservations
        else if (name.equals(SEATSConstants.TABLENAME_RESERVATION)) {
            long total = Math.round((SEATSConstants.FLIGHTS_PER_DAY_MIN + SEATSConstants.FLIGHTS_PER_DAY_MAX) / 2d * scaleFactor);
            it = new ReservationIterable(catalog_tbl, total);
        } else {
            assert (false) : "Unexpected table '" + name + "' in getScalingIterable()";
        }
        assert (it != null) : "The ScalingIterable for '" + name + "' is null!";
        return (it);
    }

    /**
     * Base Iterable implementation for scaling tables Sub-classes implement the
     * specialValue() method to generate values of a specific type instead of
     * just using the random data generators
     */
    protected abstract class ScalingDataIterable implements Iterable<Object[]> {
        private final Table catalog_tbl;
        private final boolean special[];
        private final Object[] data;
        private final int types[];
        protected long total;
        private long last_id = 0;

        /**
         * Constructor
         *
         * @param catalog_tbl
         * @param table_file
         * @throws Exception
         */
        public ScalingDataIterable(Table catalog_tbl, long total) {
            this(catalog_tbl, total, new int[0]);
        }

        /**
         * @param catalog_tbl
         * @param total
         * @param special_columns
         *            The offsets of the columns that we will invoke
         *            specialValue() to get their values
         * @throws Exception
         */
        public ScalingDataIterable(Table catalog_tbl, long total, int special_columns[]) {
            this.catalog_tbl = catalog_tbl;
            this.total = total;
            this.data = new Object[this.catalog_tbl.getColumns().size()];
            this.special = new boolean[this.catalog_tbl.getColumns().size()];

            for (int i = 0; i < this.special.length; i++) {
                this.special[i] = false;
            }
            for (int idx : special_columns) {
                this.special[idx] = true;
            }

            // Cache the types
            this.types = new int[catalog_tbl.getColumns().size()];
            for (Column catalog_col : catalog_tbl.getColumns()) {
                this.types[catalog_col.getIndex()] = catalog_col.getType();
            } // FOR
        }

        /**
         * Generate a special value for this particular column index
         *
         * @param idx
         * @return
         */
        protected abstract Object specialValue(long id, int column_idx);

        /**
         * Simple callback when the ScalingDataIterable is finished
         */
        protected void callbackFinished() {
            // Nothing...
        }

        protected boolean hasNext() {
            boolean has_next = (this.last_id < this.total);
            if (has_next == false) {
                this.callbackFinished();
            }
            return (has_next);
        }

        /**
         * Generate the iterator
         */
        @Override
        public Iterator<Object[]> iterator() {
            Iterator<Object[]> it = new Iterator<Object[]>() {
                @Override
                public boolean hasNext() {
                    return (ScalingDataIterable.this.hasNext());
                }

                @Override
                public Object[] next() {
                    for (int i = 0; i < ScalingDataIterable.this.data.length; i++) {
                        Column catalog_col = ScalingDataIterable.this.catalog_tbl.getColumn(i);
                        assert (catalog_col != null) : "The column at position " + i + " for " + ScalingDataIterable.this.catalog_tbl + " is null";

                        // Special Value Column
                        if (ScalingDataIterable.this.special[i]) {
                            ScalingDataIterable.this.data[i] = ScalingDataIterable.this.specialValue(ScalingDataIterable.this.last_id, i);

                            // Id column (always first unless overridden in
                            // special)
                        } else if (i == 0) {
                            ScalingDataIterable.this.data[i] = Long.valueOf(ScalingDataIterable.this.last_id);

                            // Strings
                        } else if (SQLUtil.isStringType(ScalingDataIterable.this.types[i])) {
                            int size = catalog_col.getSize();
                            ScalingDataIterable.this.data[i] = SEATSLoader.this.rng.astring(SEATSLoader.this.rng.nextInt(size - 1), size);

                            // Ints/Longs
                        } else {
                            assert (SQLUtil.isIntegerType(ScalingDataIterable.this.types[i])) : "Unexpected column type " + ScalingDataIterable.this.catalog_tbl.getColumn(i).fullName();
                            ScalingDataIterable.this.data[i] = SEATSLoader.this.rng.number(0, 1 << 30);
                        }
                    } // FOR
                    ScalingDataIterable.this.last_id++;
                    return (ScalingDataIterable.this.data);
                }

                @Override
                public void remove() {
                    // Not Implemented
                }
            };
            return (it);
        }
    } // END CLASS

    // ----------------------------------------------------------------
    // CUSTOMERS
    // ----------------------------------------------------------------
    protected class CustomerIterable extends ScalingDataIterable {
        private final FlatHistogram<String> rand;
        private final RandomDistribution.Flat randBalance;
        private String airport_code = null;
        private CustomerId last_id = null;

        public CustomerIterable(Table catalog_tbl, long total) {
            super(catalog_tbl, total, new int[] { 0, 1, 2, 3 });

            // Use the flights per airport histogram to select where people are
            // located
            Histogram<String> histogram = SEATSLoader.this.profile.getHistogram(SEATSConstants.HISTOGRAM_FLIGHTS_PER_AIRPORT);
            this.rand = new FlatHistogram<String>(SEATSLoader.this.rng, histogram);
            if (LOG.isDebugEnabled()) {
                this.rand.enableHistory();
            }

            this.randBalance = new RandomDistribution.Flat(SEATSLoader.this.rng, 1000, 10000);
        }

        @Override
        protected Object specialValue(long id, int columnIdx) {
            Object value = null;
            switch (columnIdx) {
                // CUSTOMER ID
                case (0): {
                    // HACK: The flights_per_airport histogram may not match up
                    // exactly with the airport
                    // data files, so we'll just spin until we get a good one
                    Long airport_id = null;
                    while (airport_id == null) {
                        this.airport_code = this.rand.nextValue();
                        airport_id = SEATSLoader.this.profile.getAirportId(this.airport_code);
                    } // WHILE
                    int next_customer_id = SEATSLoader.this.profile.incrementAirportCustomerCount(airport_id);
                    this.last_id = new CustomerId(next_customer_id, airport_id);
                    if (LOG.isTraceEnabled()) {
                        LOG.trace("NEW CUSTOMER: " + this.last_id.encode() + " / " + this.last_id);
                    }
                    value = this.last_id.encode();
                    if (LOG.isTraceEnabled()) {
                        LOG.trace(value + " => " + this.airport_code + " [" + SEATSLoader.this.profile.getCustomerIdCount(airport_id) + "]");
                    }
                    break;
                }
                // CUSTOMER ID STR
                case (1): {
                    assert (this.last_id != null);
                    value = Long.toString(this.last_id.encode());
                    this.last_id = null;
                    break;
                }
                // LOCAL AIRPORT
                case (2): {
                    assert (this.airport_code != null);
                    value = this.airport_code;
                    break;
                }
                // BALANCE
                case (3): {
                    value = (double) this.randBalance.nextInt();
                    break;
                }
                // BAD MOJO!
                default:
                    assert (false) : "Unexpected special column index " + columnIdx;
            } // SWITCH
            return (value);
        }

        @Override
        protected void callbackFinished() {
            if (LOG.isTraceEnabled()) {
                Histogram<String> h = this.rand.getHistogramHistory();
                LOG.trace(String.format("Customer Local Airports Histogram [valueCount=%d, sampleCount=%d]\n%s", h.getValueCount(), h.getSampleCount(), h.toString()));
            }
        }
    }

    // ----------------------------------------------------------------
    // FREQUENT_FLYER
    // ----------------------------------------------------------------
    protected class FrequentFlyerIterable extends ScalingDataIterable {
        private final Iterator<CustomerId> customer_id_iterator;
        private final short ff_per_customer[];
        private final FlatHistogram<String> airline_rand;

        private int customer_idx = 0;
        private CustomerId last_customer_id = null;
        private Collection<String> customer_airlines = new HashSet<String>();

        public FrequentFlyerIterable(Table catalog_tbl, long num_customers) {
            super(catalog_tbl, num_customers, new int[] { 0, 1, 2 });

            this.customer_id_iterator = new CustomerIdIterable(SEATSLoader.this.profile.airport_max_customer_id).iterator();
            this.last_customer_id = this.customer_id_iterator.next();

            // A customer is more likely to have a FREQUENTY_FLYER account with
            // an airline that has more flights.
            // IMPORTANT: Add one to all of the airlines so that we don't get
            // trapped
            // in an infinite loop
            assert (SEATSLoader.this.flights_per_airline.isEmpty() == false);
            SEATSLoader.this.flights_per_airline.putAll();
            this.airline_rand = new FlatHistogram<String>(SEATSLoader.this.rng, SEATSLoader.this.flights_per_airline);
            if (LOG.isTraceEnabled()) {
                this.airline_rand.enableHistory();
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("Flights Per Airline:\n" + SEATSLoader.this.flights_per_airline);
            }

            // Loop through for the total customers and figure out how many
            // entries we
            // should have for each one. This will be our new total;
            long max_per_customer = Math.min(Math.round(SEATSConstants.CUSTOMER_NUM_FREQUENTFLYERS_MAX * Math.max(1, SEATSLoader.this.scaleFactor)),
                    SEATSLoader.this.flights_per_airline.getValueCount());
            Zipf ff_zipf = new Zipf(SEATSLoader.this.rng, SEATSConstants.CUSTOMER_NUM_FREQUENTFLYERS_MIN, max_per_customer, SEATSConstants.CUSTOMER_NUM_FREQUENTFLYERS_SIGMA);
            long new_total = 0;
            long total = SEATSLoader.this.profile.getCustomerIdCount();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Num of Customers: " + total);
            }
            this.ff_per_customer = new short[(int) total];
            for (int i = 0; i < total; i++) {
                this.ff_per_customer[i] = (short) ff_zipf.nextInt();
                if (this.ff_per_customer[i] > max_per_customer) {
                    this.ff_per_customer[i] = (short) max_per_customer;
                }
                new_total += this.ff_per_customer[i];
            } // FOR
            this.total = new_total;
            if (LOG.isDebugEnabled()) {
                LOG.debug("Constructing " + this.total + " FrequentFlyer tuples...");
            }
        }

        @Override
        protected Object specialValue(long id, int columnIdx) {
            Object value = null;
            switch (columnIdx) {
                // CUSTOMER ID
                case (0): {
                    while (this.customer_idx < this.ff_per_customer.length && this.ff_per_customer[this.customer_idx] <= 0) {
                        this.customer_idx++;
                        this.customer_airlines.clear();
                        if (LOG.isTraceEnabled()) {
                            LOG.trace(String.format("CUSTOMER IDX: %d / %d", this.customer_idx, SEATSLoader.this.profile.getCustomerIdCount()));
                        }
                        assert (this.customer_id_iterator.hasNext());
                        this.last_customer_id = this.customer_id_iterator.next();
                    } // WHILE
                    this.ff_per_customer[this.customer_idx]--;
                    value = this.last_customer_id.encode();
                    break;
                }
                // AIRLINE ID
                case (1): {
                    assert (this.customer_airlines.size() < SEATSLoader.this.flights_per_airline.getValueCount());
                    do {
                        value = this.airline_rand.nextValue();
                    } while (this.customer_airlines.contains(value));
                    this.customer_airlines.add((String) value);
                    if (LOG.isTraceEnabled()) {
                        LOG.trace(this.last_customer_id + " => " + value);
                    }
                    break;
                }
                // CUSTOMER_ID_STR
                case (2): {
                    value = Long.toString(this.last_customer_id.encode());
                    break;
                }
                // BAD MOJO!
                default:
                    assert (false) : "Unexpected special column index " + columnIdx;
            } // SWITCH
            return (value);
        }

        @Override
        protected void callbackFinished() {
            if (LOG.isTraceEnabled()) {
                Histogram<String> h = this.airline_rand.getHistogramHistory();
                LOG.trace(String.format("Airline Flights Histogram [valueCount=%d, sampleCount=%d]\n%s", h.getValueCount(), h.getSampleCount(), h.toString()));
            }
        }
    }

    // ----------------------------------------------------------------
    // AIRPORT_DISTANCE
    // ----------------------------------------------------------------
    protected class AirportDistanceIterable extends ScalingDataIterable {
        private final int max_distance;
        private final int num_airports;
        private final Collection<String> record_airports;

        private int outer_ctr = 0;
        private String outer_airport;
        private Pair<Double, Double> outer_location;

        private Integer last_inner_ctr = null;
        private String inner_airport;
        private Pair<Double, Double> inner_location;
        private double distance;

        /**
         * Constructor
         *
         * @param catalog_tbl
         * @param max_distance
         */
        public AirportDistanceIterable(Table catalog_tbl, int max_distance) {
            super(catalog_tbl, Long.MAX_VALUE, new int[] { 0, 1, 2 });
            // total work around ????
            this.max_distance = max_distance;
            this.num_airports = SEATSLoader.this.airport_locations.size();
            this.record_airports = SEATSLoader.this.profile.getAirportCodes();
        }

        /**
         * Find the next two airports that are within our max_distance limit. We
         * keep track of where we were in the inner loop using last_inner_ctr
         */
        @Override
        protected boolean hasNext() {
            for (; this.outer_ctr < (this.num_airports - 1); this.outer_ctr++) {
                this.outer_airport = SEATSLoader.this.airport_locations.get(this.outer_ctr);
                this.outer_location = SEATSLoader.this.airport_locations.getValue(this.outer_ctr);
                if (SEATSLoader.this.profile.hasFlights(this.outer_airport) == false) {
                    continue;
                }

                int inner_ctr = (this.last_inner_ctr != null ? this.last_inner_ctr : this.outer_ctr + 1);
                this.last_inner_ctr = null;
                for (; inner_ctr < this.num_airports; inner_ctr++) {
                    assert (this.outer_ctr != inner_ctr);
                    this.inner_airport = SEATSLoader.this.airport_locations.get(inner_ctr);
                    this.inner_location = SEATSLoader.this.airport_locations.getValue(inner_ctr);
                    if (SEATSLoader.this.profile.hasFlights(this.inner_airport) == false) {
                        continue;
                    }
                    this.distance = DistanceUtil.distance(this.outer_location, this.inner_location);

                    // Store the distance between the airports locally if either
                    // one is in our
                    // flights-per-airport data set
                    if (this.record_airports.contains(this.outer_airport) && this.record_airports.contains(this.inner_airport)) {
                        SEATSLoader.this.setDistance(this.outer_airport, this.inner_airport, this.distance);
                    }

                    // Stop here if these two airports are within range
                    if (this.distance > 0 && this.distance <= this.max_distance) {
                        // System.err.println(this.outer_airport + "->" +
                        // this.inner_airport + ": " + distance);
                        this.last_inner_ctr = inner_ctr + 1;
                        return (true);
                    }
                } // FOR
            } // FOR
            return (false);
        }

        @Override
        protected Object specialValue(long id, int columnIdx) {
            Object value = null;
            switch (columnIdx) {
                // OUTER AIRPORT
                case (0):
                    value = this.outer_airport;
                    break;
                // INNER AIRPORT
                case (1):
                    value = this.inner_airport;
                    break;
                // DISTANCE
                case (2):
                    value = this.distance;
                    break;
                // BAD MOJO!
                default:
                    assert (false) : "Unexpected special column index " + columnIdx;
            } // SWITCH
            return (value);
        }
    }

    // ----------------------------------------------------------------
    // FLIGHTS
    // ----------------------------------------------------------------
    protected class FlightIterable extends ScalingDataIterable {
        private final FlatHistogram<String> airlines;
        private final FlatHistogram<String> airports;
        private final Map<String, FlatHistogram<String>> flights_per_airport = new HashMap<String, FlatHistogram<String>>();
        private final FlatHistogram<String> flight_times;
        private final Flat prices;

        private final Set<FlightId> todays_flights = new HashSet<FlightId>();
        private final ListOrderedMap<Timestamp, Integer> flights_per_day = new ListOrderedMap<Timestamp, Integer>();

        private int day_idx = 0;
        private Timestamp today;
        private Timestamp start_date;

        private FlightId flight_id;
        private String depart_airport;
        private String arrive_airport;
        private String airline_code;
        private Long airline_id;
        private Timestamp depart_time;
        private Timestamp arrive_time;
        private int status;

        public FlightIterable(Table catalog_tbl, int days_past, int days_future) {
            super(catalog_tbl, Long.MAX_VALUE, new int[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 });
            assert (days_past >= 0);
            assert (days_future >= 0);

            this.prices = new Flat(SEATSLoader.this.rng, SEATSConstants.RESERVATION_PRICE_MIN, SEATSConstants.RESERVATION_PRICE_MAX);

            // Flights per Airline
            Collection<String> all_airlines = SEATSLoader.this.profile.getAirlineCodes();
            Histogram<String> histogram = new Histogram<String>();
            histogram.putAll(all_airlines);

            // Embed a Gaussian distribution
            Gaussian gauss_rng = new Gaussian(SEATSLoader.this.rng, 0, all_airlines.size());
            this.airlines = new FlatHistogram<String>(gauss_rng, histogram);

            // Flights Per Airport
            histogram = SEATSLoader.this.profile.getHistogram(SEATSConstants.HISTOGRAM_FLIGHTS_PER_AIRPORT);
            this.airports = new FlatHistogram<String>(SEATSLoader.this.rng, histogram);
            for (String airport_code : histogram.values()) {
                histogram = SEATSLoader.this.profile.getFightsPerAirportHistogram(airport_code);
                assert (histogram != null) : "Unexpected departure airport code '" + airport_code + "'";
                this.flights_per_airport.put(airport_code, new FlatHistogram<String>(SEATSLoader.this.rng, histogram));
            } // FOR

            // Flights Per Departure Time
            histogram = SEATSLoader.this.profile.getHistogram(SEATSConstants.HISTOGRAM_FLIGHTS_PER_DEPART_TIMES);
            this.flight_times = new FlatHistogram<String>(SEATSLoader.this.rng, histogram);

            // Figure out how many flights that we want for each day
            this.today = new Timestamp(System.currentTimeMillis());

            // Sometimes there are more flights per day, and sometimes there are
            // fewer
            Gaussian gaussian = new Gaussian(SEATSLoader.this.rng, SEATSConstants.FLIGHTS_PER_DAY_MIN, SEATSConstants.FLIGHTS_PER_DAY_MAX);

            this.total = 0;
            boolean first = true;
            for (long t = this.today.getTime() - (days_past * SEATSConstants.MILLISECONDS_PER_DAY); t < this.today.getTime(); t += SEATSConstants.MILLISECONDS_PER_DAY) {
                Timestamp timestamp = new Timestamp(t);
                if (first) {
                    this.start_date = timestamp;
                    first = false;
                }
                int num_flights = gaussian.nextInt();
                this.flights_per_day.put(timestamp, num_flights);
                this.total += num_flights;
            } // FOR
            if (this.start_date == null) {
                this.start_date = this.today;
            }
            SEATSLoader.this.profile.setFlightStartDate(this.start_date);

            // This is for upcoming flights that we want to be able to schedule
            // new reservations for in the benchmark
            SEATSLoader.this.profile.setFlightUpcomingDate(this.today);
            for (long t = this.today.getTime(), last_date = this.today.getTime() + (days_future * SEATSConstants.MILLISECONDS_PER_DAY); t <= last_date; t += SEATSConstants.MILLISECONDS_PER_DAY) {
                Timestamp timestamp = new Timestamp(t);
                int num_flights = gaussian.nextInt();
                this.flights_per_day.put(timestamp, num_flights);
                this.total += num_flights;
            } // FOR

            // Update profile
            SEATSLoader.this.profile.setFlightPastDays(days_past);
            SEATSLoader.this.profile.setFlightFutureDays(days_future);
        }

        /**
         * Convert a time string "HH:MM" to a Timestamp object
         *
         * @param code
         * @return
         */
        private Timestamp convertTimeString(Timestamp base_date, String code) {
            Matcher m = SEATSConstants.TIMECODE_PATTERN.matcher(code);
            boolean result = m.find();
            assert (result) : "Invalid time code '" + code + "'";

            int hour = -1;
            try {
                hour = Integer.valueOf(m.group(1));
            } catch (Throwable ex) {
                throw new RuntimeException("Invalid HOUR in time code '" + code + "'", ex);
            }
            assert (hour != -1);

            int minute = -1;
            try {
                minute = Integer.valueOf(m.group(2));
            } catch (Throwable ex) {
                throw new RuntimeException("Invalid MINUTE in time code '" + code + "'", ex);
            }
            assert (minute != -1);

            long offset = (hour * 60 * SEATSConstants.MILLISECONDS_PER_MINUTE) + (minute * SEATSConstants.MILLISECONDS_PER_MINUTE);
            return (new Timestamp(base_date.getTime() + offset));
        }

        /**
         * Select all the data elements for the current tuple
         *
         * @param date
         */
        private void populate(Timestamp date) {
            // Depart/Arrive Airports
            this.depart_airport = this.airports.nextValue();
            this.arrive_airport = this.flights_per_airport.get(this.depart_airport).nextValue();

            // Depart/Arrive Times
            this.depart_time = this.convertTimeString(date, this.flight_times.nextValue());
            this.arrive_time = SEATSLoader.this.calculateArrivalTime(this.depart_airport, this.arrive_airport, this.depart_time);

            // Airline
            this.airline_code = this.airlines.nextValue();
            this.airline_id = SEATSLoader.this.profile.getAirlineId(this.airline_code);

            // Status
            this.status = 0; // TODO

            this.flights_per_day.put(date, this.flights_per_day.get(date) - 1);
            return;
        }

        /**
         * Returns true if this seat is occupied (which means we must generate a
         * reservation)
         */
        boolean seatIsOccupied() {
            return (SEATSLoader.this.rng.nextInt(100) < SEATSConstants.PROB_SEAT_OCCUPIED);
        }

        @Override
        protected Object specialValue(long id, int columnIdx) {
            Object value = null;
            switch (columnIdx) {
                // FLIGHT ID
                case (0): {
                    // Figure out what date we are currently on
                    Integer remaining = null;
                    Timestamp date;
                    do {
                        // Move to the next day.
                        // Make sure that we reset the set of FlightIds that
                        // we've used for today
                        if (remaining != null) {
                            this.todays_flights.clear();
                            this.day_idx++;
                        }
                        date = this.flights_per_day.get(this.day_idx);
                        remaining = this.flights_per_day.getValue(this.day_idx);
                    } while (remaining <= 0 && this.day_idx + 1 < this.flights_per_day.size());
                    assert (date != null);

                    // Keep looping until we get a FlightId that we haven't seen
                    // yet for this date
                    while (true) {
                        this.populate(date);

                        // Generate a composite FlightId
                        this.flight_id = new FlightId(this.airline_id, SEATSLoader.this.profile.getAirportId(this.depart_airport), SEATSLoader.this.profile.getAirportId(this.arrive_airport),
                                this.start_date, this.depart_time);
                        if (this.todays_flights.contains(this.flight_id) == false) {
                            break;
                        }
                    } // WHILE
                    if (LOG.isTraceEnabled()) {
                        LOG.trace(String.format("%s [remaining=%d, dayIdx=%d]", this.flight_id, remaining, this.day_idx));
                    }
                    assert (this.todays_flights.contains(this.flight_id) == false) : this.flight_id;

                    this.todays_flights.add(this.flight_id);
                    SEATSLoader.this.addFlightId(this.flight_id);
                    value = this.flight_id.encode();
                    break;
                }
                // AIRLINE ID
                case (1): {
                    value = this.airline_code;
                    SEATSLoader.this.flights_per_airline.put(this.airline_code);
                    break;
                }
                // DEPART AIRPORT
                case (2): {
                    value = this.depart_airport;
                    break;
                }
                // DEPART TIME
                case (3): {
                    value = this.depart_time;
                    break;
                }
                // ARRIVE AIRPORT
                case (4): {
                    value = this.arrive_airport;
                    break;
                }
                // ARRIVE TIME
                case (5): {
                    value = this.arrive_time;
                    break;
                }
                // FLIGHT STATUS
                case (6): {
                    value = this.status;
                    break;
                }
                // BASE PRICE
                case (7): {
                    value = (double) this.prices.nextInt();
                    break;
                }
                // SEATS TOTAL
                case (8): {
                    value = SEATSConstants.FLIGHTS_NUM_SEATS;
                    break;
                }
                // SEATS REMAINING
                case (9): {
                    // We have to figure this out ahead of time since we need to
                    // populate the tuple now
                    for (int seatnum = 0; seatnum < SEATSConstants.FLIGHTS_NUM_SEATS; seatnum++) {
                        if (!this.seatIsOccupied()) {
                            continue;
                        }
                        SEATSLoader.this.decrementFlightSeat(this.flight_id);
                    } // FOR
                    value = Long.valueOf(SEATSLoader.this.getFlightRemainingSeats(this.flight_id));
                    if (LOG.isTraceEnabled()) {
                        LOG.trace(this.flight_id + " SEATS REMAINING: " + value);
                    }
                    break;
                }
                // BAD MOJO!
                default:
                    assert (false) : "Unexpected special column index " + columnIdx;
            } // SWITCH
            return (value);
        }
    }

    // ----------------------------------------------------------------
    // RESERVATIONS
    // ----------------------------------------------------------------
    protected class ReservationIterable extends ScalingDataIterable {
        private final RandomDistribution.Flat prices = new RandomDistribution.Flat(SEATSLoader.this.rng, SEATSConstants.RESERVATION_PRICE_MIN, SEATSConstants.RESERVATION_PRICE_MAX);

        /**
         * For each airport id, store a list of ReturnFlight objects that
         * represent customers that need return flights back to their home
         * airport ArriveAirportId -> ReturnFlights
         */
        private final Map<Long, TreeSet<ReturnFlight>> airport_returns = new HashMap<Long, TreeSet<ReturnFlight>>();

        /**
         * When this flag is true, then the data generation thread is finished
         */
        private boolean done = false;

        /**
         * We use a Gaussian distribution for determining how long a customer
         * will stay at their destination before needing to return to their
         * original airport
         */
        private final Gaussian rand_returns = new Gaussian(SEATSLoader.this.rng, SEATSConstants.CUSTOMER_RETURN_FLIGHT_DAYS_MIN, SEATSConstants.CUSTOMER_RETURN_FLIGHT_DAYS_MAX);

        private final LinkedBlockingDeque<Object[]> queue = new LinkedBlockingDeque<Object[]>(100);
        private Object current[] = null;
        private Throwable error = null;

        /**
         * Constructor
         *
         * @param catalog_tbl
         * @param total
         */
        public ReservationIterable(Table catalog_tbl, long total) {
            // Special Columns: R_C_ID, R_F_ID, R_F_AL_ID, R_SEAT, R_PRICE
            super(catalog_tbl, total, new int[] { 1, 2, 3, 4 });

            for (long airport_id : SEATSLoader.this.profile.getAirportIds()) {
                // Return Flights per airport
                this.airport_returns.put(airport_id, new TreeSet<ReturnFlight>());
            } // FOR

            // Data Generation Thread
            // Ok, hang on tight. We are going to fork off a separate thread to
            // generate our tuples because it's easier than trying to pick up
            // where we left off every time. That means that when hasNext() is
            // called, it will block and poke this thread to start running.
            // Once this thread has generate a new tuple, it will block
            // itself and then poke the hasNext() thread. This is sort of
            // like a hacky version of Python's yield
            new Thread() {
                @Override
                public void run() {
                    try {
                        ReservationIterable.this.generateData();
                    } catch (Throwable ex) {
                        // System.err.println("Airport Customers:\n" +
                        // getAirportCustomerHistogram());
                        ReservationIterable.this.error = ex;
                    } finally {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Reservation generation thread is finished");
                        }
                        ReservationIterable.this.done = true;
                    }
                } // run
            }.start();
        }

        private void generateData() throws Exception {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Reservation data generation thread started");
            }

            Collection<CustomerId> flight_customer_ids = new HashSet<CustomerId>();
            Collection<ReturnFlight> returning_customers = new ListOrderedSet<ReturnFlight>();

            // Loop through the flights and generate reservations
            for (FlightId flight_id : SEATSLoader.this.getFlightIds()) {
                long depart_airport_id = flight_id.getDepartAirportId();
                String depart_airport_code = SEATSLoader.this.profile.getAirportCode(depart_airport_id);
                long arrive_airport_id = flight_id.getArriveAirportId();
                String arrive_airport_code = SEATSLoader.this.profile.getAirportCode(arrive_airport_id);
                Timestamp depart_time = flight_id.getDepartDate(SEATSLoader.this.profile.getFlightStartDate());
                Timestamp arrive_time = SEATSLoader.this.calculateArrivalTime(depart_airport_code, arrive_airport_code, depart_time);
                flight_customer_ids.clear();

                // For each flight figure out which customers are returning
                this.getReturningCustomers(returning_customers, flight_id);
                int booked_seats = SEATSConstants.FLIGHTS_NUM_SEATS - SEATSLoader.this.getFlightRemainingSeats(flight_id);

                if (LOG.isTraceEnabled()) {
                    Map<String, Object> m = new ListOrderedMap<String, Object>();
                    m.put("Flight Id", flight_id + " / " + flight_id.encode());
                    m.put("Departure", String.format("%s / %s", SEATSLoader.this.profile.getAirportCode(depart_airport_id), depart_time));
                    m.put("Arrival", String.format("%s / %s", SEATSLoader.this.profile.getAirportCode(arrive_airport_id), arrive_time));
                    m.put("Booked Seats", booked_seats);
                    m.put(String.format("Returning Customers[%d]", returning_customers.size()), StringUtil.join("\n", returning_customers));
                    LOG.trace("Flight Information\n" + StringUtil.formatMaps(m));
                }

                for (int seatnum = 0; seatnum < booked_seats; seatnum++) {
                    CustomerId customer_id = null;
                    Integer airport_customer_cnt = SEATSLoader.this.profile.getCustomerIdCount(depart_airport_id);
                    boolean local_customer = airport_customer_cnt != null && (flight_customer_ids.size() < airport_customer_cnt.intValue());
                    int tries = 2000;
                    ReturnFlight return_flight = null;
                    while (tries > 0) {
                        return_flight = null;

                        // Always book returning customers first
                        if (returning_customers.isEmpty() == false) {
                            return_flight = CollectionUtil.pop(returning_customers);
                            customer_id = return_flight.getCustomerId();
                        }
                        // New Outbound Reservation
                        // Prefer to use a customer based out of the local
                        // airport
                        else if (local_customer) {
                            customer_id = SEATSLoader.this.profile.getRandomCustomerId(depart_airport_id);
                        }
                        // New Outbound Reservation
                        // We'll take anybody!
                        else {
                            customer_id = SEATSLoader.this.profile.getRandomCustomerId();
                        }
                        if (flight_customer_ids.contains(customer_id) == false) {
                            break;
                        }
                        tries--;
                    } // WHILE
                    assert (tries > 0) : String.format("Safety check! [local=%s]", local_customer);

                    // If this is return flight, then there's nothing extra that
                    // we need to do
                    if (return_flight != null) {
                        if (LOG.isTraceEnabled()) {
                            LOG.trace("Booked return flight: " + return_flight + " [remaining=" + returning_customers.size() + "]");
                        }

                        // If it's a new outbound flight, then we will randomly
                        // decide when this customer will return (if at all)
                    } else {
                        if (SEATSLoader.this.rng.nextInt(100) < SEATSConstants.PROB_SINGLE_FLIGHT_RESERVATION) {
                            // Do nothing for now...

                            // Create a ReturnFlight object to record that this
                            // customer needs a flight
                            // back to their original depart airport
                        } else {
                            int return_days = this.rand_returns.nextInt();
                            return_flight = new ReturnFlight(customer_id, depart_airport_id, depart_time, return_days);
                            this.airport_returns.get(arrive_airport_id).add(return_flight);
                        }
                    }
                    assert (customer_id != null) : "Null customer id on " + flight_id;
                    assert (flight_customer_ids.contains(customer_id) == false) : flight_id + " already contains " + customer_id;
                    flight_customer_ids.add(customer_id);

                    if (LOG.isTraceEnabled()) {
                        LOG.trace(String.format("New reservation ready. Adding to queue! [queueSize=%d]", this.queue.size()));
                    }
                    this.queue.put(new Object[] { customer_id, flight_id, seatnum });
                } // FOR (seats)
            } // FOR (flights)
            if (LOG.isDebugEnabled()) {
                LOG.debug("Reservation data generation thread is finished");
            }
        }

        /**
         * Return a list of the customers that need to return to their original
         * location on this particular flight.
         *
         * @param flight_id
         * @return
         */
        private void getReturningCustomers(Collection<ReturnFlight> returning_customers, FlightId flight_id) {
            Timestamp flight_date = flight_id.getDepartDate(SEATSLoader.this.profile.getFlightStartDate());
            returning_customers.clear();
            Set<ReturnFlight> returns = this.airport_returns.get(flight_id.getDepartAirportId());
            if (!returns.isEmpty()) {
                for (ReturnFlight return_flight : returns) {
                    if (return_flight.getReturnDate().compareTo(flight_date) > 0) {
                        break;
                    }
                    if (return_flight.getReturnAirportId() == flight_id.getArriveAirportId()) {
                        returning_customers.add(return_flight);
                    }
                } // FOR
                if (!returning_customers.isEmpty()) {
                    returns.removeAll(returning_customers);
                }
            }
        }

        @Override
        protected boolean hasNext() {
            if (LOG.isTraceEnabled()) {
                LOG.trace("hasNext() called");
            }
            this.current = null;
            while (this.done == false || this.queue.isEmpty() == false) {
                if (this.error != null) {
                    throw new RuntimeException("Failed to generate Reservation records", this.error);
                }

                try {
                    this.current = this.queue.poll(100, TimeUnit.MILLISECONDS);
                } catch (InterruptedException ex) {
                    throw new RuntimeException("Unexpected interruption!", ex);
                }
                if (this.current != null) {
                    return (true);
                }
                if (LOG.isTraceEnabled()) {
                    LOG.trace("There were no new reservations. Let's try again!");
                }
            } // WHILE
            return (false);
        }

        @Override
        protected Object specialValue(long id, int columnIdx) {
            assert (this.current != null);
            Object value = null;
            switch (columnIdx) {
                // CUSTOMER ID
                case (1): {
                    value = ((CustomerId) this.current[0]).encode();
                    break;
                }
                // FLIGHT ID
                case (2): {
                    FlightId flight_id = (FlightId) this.current[1];
                    value = flight_id.encode();
                    if (SEATSLoader.this.profile.getReservationUpcomingOffset() == null
                            && flight_id.isUpcoming(SEATSLoader.this.profile.getFlightStartDate(), SEATSLoader.this.profile.getFlightPastDays())) {
                        SEATSLoader.this.profile.setReservationUpcomingOffset(id);
                    }
                    break;
                }
                // SEAT
                case (3): {
                    value = this.current[2];
                    break;
                }
                // PRICE
                case (4): {
                    value = (double) this.prices.nextInt();
                    break;
                }
                // BAD MOJO!
                default:
                    assert (false) : "Unexpected special column index " + columnIdx;
            } // SWITCH
            return (value);
        }
    } // END CLASS

    // -----------------------------------------------------------------
    // FLIGHT IDS
    // -----------------------------------------------------------------

    public Iterable<FlightId> getFlightIds() {
        return (new Iterable<FlightId>() {
            @Override
            public Iterator<FlightId> iterator() {
                return (new Iterator<FlightId>() {
                    private int idx = 0;
                    private final int cnt = SEATSLoader.this.seats_remaining.size();

                    @Override
                    public boolean hasNext() {
                        return (this.idx < this.cnt);
                    }

                    @Override
                    public FlightId next() {
                        return (SEATSLoader.this.seats_remaining.get(this.idx++));
                    }

                    @Override
                    public void remove() {
                        // Not implemented
                    }
                });
            }
        });
    }

    /**
     * @param flight_id
     */
    public boolean addFlightId(FlightId flight_id) {
        assert (flight_id != null);
        assert (this.profile.flight_start_date != null);
        assert (this.profile.flight_upcoming_date != null);
        this.profile.addFlightId(flight_id);
        this.seats_remaining.put(flight_id, (short) SEATSConstants.FLIGHTS_NUM_SEATS);

        // XXX
        if (this.profile.flight_upcoming_offset == null && this.profile.flight_upcoming_date.compareTo(flight_id.getDepartDate(this.profile.flight_start_date)) < 0) {
            this.profile.flight_upcoming_offset = (long) (this.seats_remaining.size() - 1);
        }
        return (true);
    }

    /**
     * Return the number of unique flight ids
     *
     * @return
     */
    public long getFlightIdCount() {
        return (this.seats_remaining.size());
    }

    /**
     * Return the index offset of when future flights
     *
     * @return
     */
    public long getFlightIdStartingOffset() {
        return (this.profile.flight_upcoming_offset);
    }

    /**
     * Return flight
     *
     * @param index
     * @return
     */
    public FlightId getFlightId(int index) {
        assert (index >= 0);
        assert (index <= this.getFlightIdCount());
        return (this.seats_remaining.get(index));
    }

    /**
     * Return the number of seats remaining for a flight
     *
     * @param flight_id
     * @return
     */
    public int getFlightRemainingSeats(FlightId flight_id) {
        return (this.seats_remaining.get(flight_id));
    }

    /**
     * Decrement the number of available seats for a flight and return the total
     * amount remaining
     */
    public int decrementFlightSeat(FlightId flight_id) {
        Short seats = this.seats_remaining.get(flight_id);
        assert (seats != null) : "Missing seat count for " + flight_id;
        assert (seats >= 0) : "Invalid seat count for " + flight_id;
        return (this.seats_remaining.put(flight_id, (short) (seats - 1)));
    }

    // ----------------------------------------------------------------
    // DISTANCE METHODS
    // ----------------------------------------------------------------

    public void setDistance(String airport0, String airport1, double distance) {
        short short_distance = (short) Math.round(distance);
        for (String a[] : new String[][] { { airport0, airport1 }, { airport1, airport0 } }) {
            if (!this.airport_distances.containsKey(a[0])) {
                this.airport_distances.put(a[0], new HashMap<String, Short>());
            }
            this.airport_distances.get(a[0]).put(a[1], short_distance);
        } // FOR
    }

    public Integer getDistance(String airport0, String airport1) {
        assert (this.airport_distances.containsKey(airport0)) : "No distance entries for '" + airport0 + "'";
        assert (this.airport_distances.get(airport0).containsKey(airport1)) : "No distance entries from '" + airport0 + "' to '" + airport1 + "'";
        return ((int) this.airport_distances.get(airport0).get(airport1));
    }

    /**
     * For the current depart+arrive airport destinations, calculate the
     * estimated flight time and then add the to the departure time in order to
     * come up with the expected arrival time.
     *
     * @param depart_airport
     * @param arrive_airport
     * @param depart_time
     * @return
     */
    public Timestamp calculateArrivalTime(String depart_airport, String arrive_airport, Timestamp depart_time) {
        Integer distance = this.getDistance(depart_airport, arrive_airport);
        assert (distance != null) : String.format("The calculated distance between '%s' and '%s' is null", depart_airport, arrive_airport);
        long flight_time = Math.round(distance / SEATSConstants.FLIGHT_TRAVEL_RATE) * 3600000000l;
        // 60 sec * 60 min * 1,000,000
        return (new Timestamp(depart_time.getTime() + flight_time));
    }
}