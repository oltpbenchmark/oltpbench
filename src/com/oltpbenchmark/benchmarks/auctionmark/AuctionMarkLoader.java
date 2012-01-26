/***************************************************************************
 *  Copyright (C) 2010 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  Andy Pavlo (pavlo@cs.brown.edu)                                        *
 *  http://www.cs.brown.edu/~pavlo/                                        *
 *                                                                         *
 *  Visawee Angkanawaraphan (visawee@cs.brown.edu)                         *
 *  http://www.cs.brown.edu/~visawee/                                      *
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
package com.oltpbenchmark.benchmarks.auctionmark;

import java.io.File;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.commons.collections15.CollectionUtils;
import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.log4j.Logger;

import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.benchmarks.auctionmark.AuctionMarkConstants.ItemStatus;
import com.oltpbenchmark.benchmarks.auctionmark.util.CategoryParser;
import com.oltpbenchmark.benchmarks.auctionmark.util.Category;
import com.oltpbenchmark.benchmarks.auctionmark.util.GlobalAttributeGroupId;
import com.oltpbenchmark.benchmarks.auctionmark.util.GlobalAttributeValueId;
import com.oltpbenchmark.benchmarks.auctionmark.util.ItemId;
import com.oltpbenchmark.benchmarks.auctionmark.util.LoaderItemInfo;
import com.oltpbenchmark.benchmarks.auctionmark.util.UserId;
import com.oltpbenchmark.benchmarks.auctionmark.util.UserIdGenerator;
import com.oltpbenchmark.catalog.Column;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.util.*;
import com.oltpbenchmark.util.RandomDistribution.Flat;
import com.oltpbenchmark.util.RandomDistribution.Zipf;

/**
 * 
 * @author pavlo
 * @author visawee
 */
public class AuctionMarkLoader extends Loader {
    private static final Logger LOG = Logger.getLogger(AuctionMarkLoader.class);
    
    // -----------------------------------------------------------------
    // INTERNAL DATA MEMBERS
    // -----------------------------------------------------------------
    
    protected final AuctionMarkProfile profile;
    
    /**
     * Data Generator Classes
     * TableName -> AbstactTableGenerator
     */
    private final Map<String, AbstractTableGenerator> generators = new ListOrderedMap<String, AbstractTableGenerator>();
    
    private final Collection<String> sub_generators = new HashSet<String>();

    /** The set of tables that we have finished loading **/
    private final transient Collection<String> finished = Collections.synchronizedCollection(new HashSet<String>());
    
    private final Histogram<String> tableSizes = new Histogram<String>();

    private boolean fail = false;
    
    // -----------------------------------------------------------------
    // INITIALIZATION
    // -----------------------------------------------------------------

    /**
     * Constructor
     * 
     * @param args
     */
    public AuctionMarkLoader(AuctionMarkBenchmark benchmark, Connection conn) {
        super(benchmark, conn);

        // BenchmarkProfile
        profile = new AuctionMarkProfile(benchmark, benchmark.getRandomGenerator());
        profile.setAndGetBenchmarkStartTime();

        File category_file = new File(profile.data_directory.getAbsolutePath() + "/table.category.gz");
        
        // ---------------------------
        // Fixed-Size Table Generators
        // ---------------------------
        
        this.registerGenerator(new RegionGenerator());
        this.registerGenerator(new CategoryGenerator(category_file));
        this.registerGenerator(new GlobalAttributeGroupGenerator());
        this.registerGenerator(new GlobalAttributeValueGenerator());

        // ---------------------------
        // Scaling-Size Table Generators
        // ---------------------------
        
        // USER TABLES
        this.registerGenerator(new UserGenerator());
        this.registerGenerator(new UserAttributesGenerator());
        this.registerGenerator(new UserItemGenerator());
        this.registerGenerator(new UserWatchGenerator());
        this.registerGenerator(new UserFeedbackGenerator());
        
        // ITEM TABLES
        this.registerGenerator(new ItemGenerator());
        this.registerGenerator(new ItemAttributeGenerator());
        this.registerGenerator(new ItemBidGenerator());
        this.registerGenerator(new ItemMaxBidGenerator());
        this.registerGenerator(new ItemCommentGenerator());
        this.registerGenerator(new ItemImageGenerator());
        this.registerGenerator(new ItemPurchaseGenerator());
    }
    
    // -----------------------------------------------------------------
    // LOADING METHODS
    // -----------------------------------------------------------------
    
    @Override
    public void load() {
        if (LOG.isDebugEnabled())
            LOG.debug(String.format("Starting loader [scaleFactor=%.2f]", profile.getScaleFactor())); 
        
        final EventObservableExceptionHandler handler = new EventObservableExceptionHandler();
        final List<Thread> threads = new ArrayList<Thread>();
        for (AbstractTableGenerator generator : this.generators.values()) {
            // if (isSubGenerator(generator)) continue;
            Thread t = new Thread(generator);
            t.setName(generator.getTableName());
            t.setUncaughtExceptionHandler(handler);
            
            // Call init() before we start!
            // This will setup non-data related dependencies
            generator.init();
            
            threads.add(t);
        } // FOR
        assert(threads.size() > 0);
        handler.addObserver(new EventObserver<Pair<Thread,Throwable>>() {
            @Override
            public void update(EventObservable<Pair<Thread, Throwable>> o, Pair<Thread, Throwable> t) {
                fail = true;
                for (Thread thread : threads)
                    thread.interrupt();
            }
        });
        
        // Construct a new thread to load each table
        // Fire off the threads and wait for them to complete
        // If debug is set to true, then we'll execute them serially
        try {
            for (Thread t : threads) {
                t.start();
            } // FOR
            for (Thread t : threads) {
                t.join();
            } // FOR
        } catch (InterruptedException e) {
            LOG.fatal("Unexpected error", e);
        } finally {
            if (handler.hasError()) {
                throw new RuntimeException("Error while generating table data.", handler.getError());
            }
        }
        
        // Save the benchmark profile out to disk so that we can send it
        // to all of the clients
        try {
            profile.saveProfile(this.conn);
        } catch (SQLException ex) {
            throw new RuntimeException("Failed to save profile information in database", ex);
        }
        LOG.info("Finished generating data for all tables");
    }
    
    private void registerGenerator(AbstractTableGenerator generator) {
        // Register this one as well as any sub-generators
        this.generators.put(generator.getTableName(), generator);
        for (AbstractTableGenerator sub_generator : generator.getSubTableGenerators()) {
            this.registerGenerator(sub_generator);
            this.sub_generators.add(sub_generator.getTableName());
        } // FOR
    }
    
    protected AbstractTableGenerator getGenerator(String table_name) {
        return (this.generators.get(table_name));
    }

    /**
     * Load the tuples for the given table name
     * @param tableName
     */
    protected void generateTableData(String tableName) throws SQLException {
        LOG.info("*** START " + tableName);
        final AbstractTableGenerator generator = this.generators.get(tableName);
        assert (generator != null);

        // Generate Data
        final Table catalog_tbl = benchmark.getCatalog().getTable(tableName);
        assert(catalog_tbl != null) : tableName;
        final List<Object[]> volt_table = generator.getVoltTable();
        final String sql = SQLUtil.getInsertSQL(catalog_tbl);
        final PreparedStatement stmt = conn.prepareStatement(sql);
        
        while (generator.hasMore()) {
            generator.generateBatch();
            
//            StringBuilder sb = new StringBuilder();
//            if (tableName.equalsIgnoreCase("USER_FEEDBACK")) { //  || tableName.equalsIgnoreCase("USER_ATTRIBUTES")) {
//                sb.append(tableName + "\n");
//                for (int i = 0; i < volt_table.size(); i++) {
//                    sb.append(String.format("[%03d] %s\n", i, StringUtil.abbrv(Arrays.toString(volt_table.get(i)), 100)));
//                }
//                LOG.debug(sb.toString() + "\n");
//            }
            
            for (Object row[] : volt_table) {
                for (int i = 0; i < row.length; i++) {
                    stmt.setObject(i+1, row[i]);
                } // FOR
                stmt.addBatch();
            } // FOR
            stmt.executeBatch();
            conn.commit();
            stmt.clearBatch();
            
            this.tableSizes.put(tableName, volt_table.size());
            
            // Release anything to the sub-generators if we have it
            // We have to do this to ensure that all of the parent tuples get
            // insert first for foreign-key relationships
            generator.releaseHoldsToSubTableGenerators();
        } // WHILE
        
        // Mark as finished
        if (this.fail == false) {
            generator.markAsFinished();
            this.finished.add(tableName);
            LOG.info(String.format("*** FINISH %s - %d tuples - [%d / %d]",
                                   tableName, this.tableSizes.get(tableName),
                                   this.finished.size(), this.generators.size()));
            if (LOG.isDebugEnabled()) {
                LOG.debug("Remaining Tables: " + CollectionUtils.subtract(this.generators.keySet(), this.finished));
            }
        }
    }

    /**********************************************************************************************
     * AbstractTableGenerator
     **********************************************************************************************/
    protected abstract class AbstractTableGenerator implements Runnable {
        private final String tableName;
        private final Table catalog_tbl;
        protected final List<Object[]> table = new ArrayList<Object[]>();
        protected Long tableSize;
        protected Long batchSize;
        protected final CountDownLatch latch = new CountDownLatch(1);
        protected final List<String> dependencyTables = new ArrayList<String>();

        /**
         * Some generators have children tables that we want to load tuples for each batch of this generator. 
         * The queues we need to update every time we generate a new LoaderItemInfo
         */
        protected final Set<SubTableGenerator<?>> sub_generators = new HashSet<SubTableGenerator<?>>();  

        protected final List<Object> subGenerator_hold = new ArrayList<Object>();
        
        protected long count = 0;
        
        /** Any column with the name XX_SATTR## will automatically be filled with a random string */
        protected final List<Column> random_str_cols = new ArrayList<Column>();
        protected final Pattern random_str_regex = Pattern.compile("[\\w]+\\_SATTR[\\d]+", Pattern.CASE_INSENSITIVE);
        
        /** Any column with the name XX_IATTR## will automatically be filled with a random integer */
        protected List<Column> random_int_cols = new ArrayList<Column>();
        protected final Pattern random_int_regex = Pattern.compile("[\\w]+\\_IATTR[\\d]+", Pattern.CASE_INSENSITIVE);

        /**
         * Constructor
         * @param catalog_tbl
         */
        public AbstractTableGenerator(String tableName, String...dependencies) {
            this.tableName = tableName;
            this.catalog_tbl = benchmark.getCatalog().getTable(tableName);
            assert(catalog_tbl != null) : "Invalid table name '" + tableName + "'";
            
            boolean fixed_size = AuctionMarkConstants.FIXED_TABLES.contains(catalog_tbl.getName());
            boolean dynamic_size = AuctionMarkConstants.DYNAMIC_TABLES.contains(catalog_tbl.getName());
            boolean data_file = AuctionMarkConstants.DATAFILE_TABLES.contains(catalog_tbl.getName());

            // Add the dependencies so that we know what we need to block on
            CollectionUtil.addAll(this.dependencyTables, dependencies);
            
            try {
                String field_name = "BATCHSIZE_" + catalog_tbl.getName();
                Field field_handle = AuctionMarkConstants.class.getField(field_name);
                assert (field_handle != null);
                this.batchSize = (Long) field_handle.get(null);
            } catch (Exception ex) {
                throw new RuntimeException("Missing field needed for '" + tableName + "'", ex);
            } 

            // Initialize dynamic parameters for tables that are not loaded from data files
            if (!data_file && !dynamic_size && tableName.equalsIgnoreCase(AuctionMarkConstants.TABLENAME_ITEM) == false) {
                try {
                    String field_name = "TABLESIZE_" + catalog_tbl.getName();
                    Field field_handle = AuctionMarkConstants.class.getField(field_name);
                    assert (field_handle != null);
                    this.tableSize = (Long) field_handle.get(null);
                    if (!fixed_size) {
                        this.tableSize = (long)Math.max(1, (int)Math.round(this.tableSize * profile.getScaleFactor()));
                    }
                } catch (NoSuchFieldException ex) {
                    if (LOG.isDebugEnabled()) LOG.warn("No table size field for '" + tableName + "'", ex);
                } catch (Exception ex) {
                    throw new RuntimeException("Missing field needed for '" + tableName + "'", ex);
                } 
            } 
            
            for (Column catalog_col : this.catalog_tbl.getColumns()) {
                if (random_str_regex.matcher(catalog_col.getName()).matches()) {
                    assert(SQLUtil.isStringType(catalog_col.getType())) : catalog_col.fullName();
                    this.random_str_cols.add(catalog_col);
                    if (LOG.isTraceEnabled()) LOG.trace("Random String Column: " + catalog_col.fullName());
                }
                else if (random_int_regex.matcher(catalog_col.getName()).matches()) {
                    assert(SQLUtil.isIntegerType(catalog_col.getType())) : catalog_col.fullName();
                    this.random_int_cols.add(catalog_col);
                    if (LOG.isTraceEnabled()) LOG.trace("Random Integer Column: " + catalog_col.fullName());
                }
            } // FOR
            if (LOG.isDebugEnabled()) {
                if (this.random_str_cols.size() > 0) LOG.debug(String.format("%s Random String Columns: %s", tableName, this.random_str_cols));
                if (this.random_int_cols.size() > 0) LOG.debug(String.format("%s Random Integer Columns: %s", tableName, this.random_int_cols));
            }
        }

        /**
         * Initiate data that need dependencies
         */
        public abstract void init();
        
        /**
         * Prepare to generate tuples
         */
        public abstract void prepare();
        
        /**
         * All sub-classes must implement this. This will enter new tuple data into the row
         * @param row TODO
         */
        protected abstract int populateRow(Object[] row);
        
        public void run() {
            // First block on the CountDownLatches of all the tables that we depend on
            if (this.dependencyTables.size() > 0 && LOG.isDebugEnabled())
                LOG.debug(String.format("%s: Table generator is blocked waiting for %d other tables: %s",
                                        this.tableName, this.dependencyTables.size(), this.dependencyTables));
            for (String dependency : this.dependencyTables) {
                AbstractTableGenerator gen = AuctionMarkLoader.this.generators.get(dependency);
                assert(gen != null) : "Missing table generator for '" + dependency + "'";
                try {
                    gen.latch.await();
                } catch (InterruptedException ex) {
                    throw new RuntimeException("Unexpected interruption for '" + this.tableName + "' waiting for '" + dependency + "'", ex);
                }
            } // FOR
            
            // Make sure we call prepare before we start generating table data
            this.prepare();
            
            // Then invoke the loader generation method
            try {
                AuctionMarkLoader.this.generateTableData(this.tableName);
            } catch (Throwable ex) {
                throw new RuntimeException("Unexpected error while generating table data for '" + this.tableName + "'", ex);
            }
        }
        
        @SuppressWarnings("unchecked")
        public <T extends AbstractTableGenerator> T addSubTableGenerator(SubTableGenerator<?> sub_item) {
            this.sub_generators.add(sub_item);
            return ((T)this);
        }
        @SuppressWarnings("unchecked")
        public void releaseHoldsToSubTableGenerators() {
            if (this.subGenerator_hold.isEmpty() == false) {
                LOG.debug(String.format("%s: Releasing %d held objects to %d sub-generators",
                                        this.tableName, this.subGenerator_hold.size(), this.sub_generators.size()));
                for (@SuppressWarnings("rawtypes") SubTableGenerator sub_generator : this.sub_generators) {
                    sub_generator.queue.addAll(this.subGenerator_hold);
                } // FOR
                this.subGenerator_hold.clear();
            }
        }
        public void updateSubTableGenerators(Object obj) {
            // Queue up this item for our multi-threaded sub-generators
            if (LOG.isTraceEnabled())
                LOG.trace(String.format("%s: Updating %d sub-generators with %s: %s",
                                        this.tableName, this.sub_generators.size(), obj, this.sub_generators));
            this.subGenerator_hold.add(obj);
        }
        public boolean hasSubTableGenerators() {
            return (!this.sub_generators.isEmpty());
        }
        public Collection<SubTableGenerator<?>> getSubTableGenerators() {
            return (this.sub_generators);
        }
        public Collection<String> getSubGeneratorTableNames() {
            List<String> names = new ArrayList<String>();
            for (AbstractTableGenerator gen : this.sub_generators) {
                names.add(gen.catalog_tbl.getName());
            }
            return (names);
        }
        
        protected int populateRandomColumns(Object row[]) {
            int cols = 0;
            
            // STRINGS
            for (Column catalog_col : this.random_str_cols) {
                int size = catalog_col.getSize();
                row[catalog_col.getIndex()] = profile.rng.astring(profile.rng.nextInt(size - 1), size);
                cols++;
            } // FOR
            
            // INTEGER
            for (Column catalog_col : this.random_int_cols) {
                row[catalog_col.getIndex()] = profile.rng.number(0, 1<<30);
                cols++;
            } // FOR
            
            return (cols);
        }

        /**
         * Returns true if this generator has more tuples that it wants to add
         * @return
         */
        public synchronized boolean hasMore() {
            return (this.count < this.tableSize);
        }
        /**
         * Return the table's catalog object for this generator
         * @return
         */
        public Table getTableCatalog() {
            return (this.catalog_tbl);
        }
        /**
         * Return the VoltTable handle
         * @return
         */
        public List<Object[]> getVoltTable() {
            return this.table;
        }
        /**
         * Returns the number of tuples that will be loaded into this table
         * @return
         */
        public Long getTableSize() {
            return this.tableSize;
        }
        /**
         * Returns the number of tuples per batch that this generator will want loaded
         * @return
         */
        public Long getBatchSize() {
            return this.batchSize;
        }
        /**
         * Returns the name of the table this this generates
         * @return
         */
        public String getTableName() {
            return this.tableName;
        }
        /**
         * Returns the total number of tuples generated thusfar
         * @return
         */
        public synchronized long getCount() {
            return this.count;
        }

        /**
         * When called, the generator will populate a new row record and append it to the underlying VoltTable
         */
        public synchronized void addRow() {
            Object row[] = new Object[this.catalog_tbl.getColumnCount()];
            
            // Main Columns
            int cols = this.populateRow(row);
            
            // RANDOM COLS
            cols += this.populateRandomColumns(row);
            
            assert(cols == this.catalog_tbl.getColumnCount()) : 
                String.format("Invalid number of columns for %s [expected=%d, actual=%d]",
                              this.tableName, this.catalog_tbl.getColumnCount(), cols);
            
            // Convert all CompositeIds into their long encodings
            for (int i = 0; i < cols; i++) {
                if (row[i] != null && row[i] instanceof CompositeId) {
                    row[i] = ((CompositeId)row[i]).encode();
                }
            } // FOR
            
            this.count++;
            this.table.add(row);
        }
        /**
         * 
         */
        public void generateBatch() {
            if (LOG.isTraceEnabled()) LOG.trace(String.format("%s: Generating new batch", this.getTableName()));
            long batch_count = 0;
            this.table.clear();
            while (this.hasMore() && this.table.size() < this.batchSize) {
                this.addRow();
                batch_count++;
            } // WHILE
            if (LOG.isDebugEnabled())
                LOG.debug(String.format("%s: Finished generating new batch of %d tuples", this.getTableName(), batch_count));
        }

        public void markAsFinished() {
            if (LOG.isDebugEnabled())
                LOG.debug(String.format("%s: Marking as finished", this.tableName));
        	this.latch.countDown();
            for (SubTableGenerator<?> sub_generator : this.sub_generators) {
                sub_generator.stopWhenEmpty();
            } // FOR
        }
        
        public boolean isFinish(){
        	return (this.latch.getCount() == 0);
        }
        
        public List<String> getDependencies() {
            return this.dependencyTables;
        }
        
        @Override
        public String toString() {
            return String.format("Generator[%s]", this.tableName);
        }
    } // END CLASS

    /**********************************************************************************************
     * SubUserTableGenerator
     * This is for tables that are based off of the USER table
     **********************************************************************************************/
    protected abstract class SubTableGenerator<T> extends AbstractTableGenerator {
        
        private final LinkedBlockingDeque<T> queue = new LinkedBlockingDeque<T>();
        private T current;
        private short currentCounter;
        private boolean stop = false;
        private final String sourceTableName;

        public SubTableGenerator(String tableName, String sourceTableName, String...dependencies) {
            super(tableName, dependencies);
            this.sourceTableName = sourceTableName;
        }
        
        protected abstract short getElementCounter(T t);
        protected abstract int populateRow(T t, Object[] row, short remaining);
        
        public void stopWhenEmpty() {
            if (LOG.isDebugEnabled())
                LOG.debug(String.format("%s: Will stop when queue is empty", this.getTableName()));
            this.stop = true;
        }
        
        @Override
        public void init() {
            // Get the AbstractTableGenerator that will feed into this generator
            AbstractTableGenerator parent_gen = AuctionMarkLoader.this.generators.get(this.sourceTableName);
            assert(parent_gen != null) : "Unexpected source TableGenerator '" + this.sourceTableName + "'";
            parent_gen.addSubTableGenerator(this);
            
            this.current = null;
            this.currentCounter = 0;
        }
        @Override
        public void prepare() {
            // Nothing to do...
        }
        @Override
        public final boolean hasMore() {
            return (this.getNext() != null);
        }
        @Override
        protected final int populateRow(Object[] row) {
            T t = this.getNext();
            assert(t != null);
            this.currentCounter--;
            return (this.populateRow(t, row, this.currentCounter));
        }
        private final T getNext() {
            T last = this.current;
            if (this.current == null || this.currentCounter == 0) {
                while (this.currentCounter == 0) {
                    try {
                        this.current = this.queue.poll(1000, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException ex) {
                        return (null);
                    }
                    // Check whether we should stop
                    if (this.current == null) {
                        if (this.stop) break;
                        continue;
                    }
                    this.currentCounter = this.getElementCounter(this.current);
                } // WHILE
            }
            if (last != this.current) {
                if (last != null) this.finishElementCallback(last);
                if (this.current != null) this.newElementCallback(this.current);
            }
            return this.current;
        }
        protected void finishElementCallback(T t) {
            // Nothing...
        }
        protected void newElementCallback(T t) {
            // Nothing... 
        }
    } // END CLASS
    
    /**********************************************************************************************
     * REGION Generator
     **********************************************************************************************/
    protected class RegionGenerator extends AbstractTableGenerator {

        public RegionGenerator() {
            super(AuctionMarkConstants.TABLENAME_REGION);
        }

        @Override
        public void init() {
            // Nothing to do
        }
        @Override
        public void prepare() {
            // Nothing to do
        }
        @Override
        protected int populateRow(Object[] row) {
            int col = 0;

            // R_ID
            row[col++] = new Integer((int) this.count);
            // R_NAME
            row[col++] = profile.rng.astring(6, 32);
            
            return (col);
        }
    } // END CLASS

    /**********************************************************************************************
     * CATEGORY Generator
     **********************************************************************************************/
    protected class CategoryGenerator extends AbstractTableGenerator {
        private final File data_file;
        private final Map<String, Category> categoryMap;
        private final LinkedList<Category> categories = new LinkedList<Category>();

        public CategoryGenerator(File data_file) {
            super(AuctionMarkConstants.TABLENAME_CATEGORY);
            this.data_file = data_file;
            assert(this.data_file.exists()) : 
                "The data file for the category generator does not exist: " + this.data_file;

            this.categoryMap = (new CategoryParser(data_file)).getCategoryMap();
            this.tableSize = (long)this.categoryMap.size();
        }

        @Override
        public void init() {
            for (Category category : this.categoryMap.values()) {
                if (category.isLeaf()) {
                    profile.item_category_histogram.put((long)category.getCategoryID(), category.getItemCount());
                }
                this.categories.add(category);
            } // FOR
        }
        @Override
        public void prepare() {
            // Nothing to do
        }
        @Override
        protected int populateRow(Object[] row) {
            int col = 0;

            Category category = this.categories.poll();
            assert(category != null);
            
            // C_ID
            row[col++] = category.getCategoryID();
            // C_NAME
            row[col++] = category.getName();
            // C_PARENT_ID
            row[col++] = category.getParentCategoryID();
            
            return (col);
        }
    } // END CLASS

    /**********************************************************************************************
     * GLOBAL_ATTRIBUTE_GROUP Generator
     **********************************************************************************************/
    protected class GlobalAttributeGroupGenerator extends AbstractTableGenerator {
        private long num_categories = 0l;
        private final Histogram<Integer> category_groups = new Histogram<Integer>();
        private final LinkedList<GlobalAttributeGroupId> group_ids = new LinkedList<GlobalAttributeGroupId>();

        public GlobalAttributeGroupGenerator() {
            super(AuctionMarkConstants.TABLENAME_GLOBAL_ATTRIBUTE_GROUP,
                  AuctionMarkConstants.TABLENAME_CATEGORY);
        }

        @Override
        public void init() {
            // Nothing to do
        }
        @Override
        public void prepare() {
            // Grab the number of CATEGORY items that we have inserted
            this.num_categories = getGenerator(AuctionMarkConstants.TABLENAME_CATEGORY).tableSize;
            
            for (int i = 0; i < this.tableSize; i++) {
                int category_id = profile.rng.number(0, (int)this.num_categories);
                this.category_groups.put(category_id);
                int id = this.category_groups.get(category_id).intValue();
                int count = (int)profile.rng.number(1, AuctionMarkConstants.TABLESIZE_GLOBAL_ATTRIBUTE_VALUE_PER_GROUP);
                GlobalAttributeGroupId gag_id = new GlobalAttributeGroupId(category_id, id, count);
                assert(profile.gag_ids.contains(gag_id) == false);
                profile.gag_ids.add(gag_id);
                this.group_ids.add(gag_id);
            } // FOR
        }
        @Override
        protected int populateRow(Object[] row) {
            int col = 0;

            GlobalAttributeGroupId gag_id = this.group_ids.poll();
            assert(gag_id != null);
            
            // GAG_ID
            row[col++] = gag_id.encode();
            // GAG_C_ID
            row[col++] = gag_id.getCategoryId();
            // GAG_NAME
            row[col++] = profile.rng.astring(6, 32);
            
            return (col);
        }
    } // END CLASS

    /**********************************************************************************************
     * GLOBAL_ATTRIBUTE_VALUE Generator
     **********************************************************************************************/
    protected class GlobalAttributeValueGenerator extends AbstractTableGenerator {

        private Histogram<GlobalAttributeGroupId> gag_counters = new Histogram<GlobalAttributeGroupId>(true);
        private Iterator<GlobalAttributeGroupId> gag_iterator;
        private GlobalAttributeGroupId gag_current;
        private int gav_counter = -1;

        public GlobalAttributeValueGenerator() {
            super(AuctionMarkConstants.TABLENAME_GLOBAL_ATTRIBUTE_VALUE,
                  AuctionMarkConstants.TABLENAME_GLOBAL_ATTRIBUTE_GROUP);
        }

        @Override
        public void init() {
            // Nothing to do
        }
        @Override
        public void prepare() {
            this.tableSize = 0l;
            for (GlobalAttributeGroupId gag_id : profile.gag_ids) {
                this.gag_counters.set(gag_id, 0);
                this.tableSize += gag_id.getCount();
            } // FOR
            this.gag_iterator = profile.gag_ids.iterator();
        }
        @Override
        protected int populateRow(Object[] row) {
            int col = 0;
            
            if (this.gav_counter == -1 || ++this.gav_counter == this.gag_current.getCount()) {
                this.gag_current = this.gag_iterator.next();
                assert(this.gag_current != null);
                this.gav_counter = 0;
            }

            GlobalAttributeValueId gav_id = new GlobalAttributeValueId(this.gag_current.encode(),
                                                                     this.gav_counter);
            
            // GAV_ID
            row[col++] = gav_id.encode();
            // GAV_GAG_ID
            row[col++] = this.gag_current.encode();
            // GAV_NAME
            row[col++] = profile.rng.astring(6, 32);
            
            return (col);
        }
    } // END CLASS

    /**********************************************************************************************
     * USER Generator
     **********************************************************************************************/
    protected class UserGenerator extends AbstractTableGenerator {
        private final Zipf randomBalance;
        private final Flat randomRegion;
        private final Zipf randomRating;
        private UserIdGenerator idGenerator;
        
        public UserGenerator() {
            super(AuctionMarkConstants.TABLENAME_USER,
                  AuctionMarkConstants.TABLENAME_REGION);
            this.randomRegion = new Flat(profile.rng, 0, (int)AuctionMarkConstants.TABLESIZE_REGION);
            this.randomRating = new Zipf(profile.rng, AuctionMarkConstants.USER_MIN_RATING,
                                                      AuctionMarkConstants.USER_MAX_RATING, 1.0001);
            this.randomBalance = new Zipf(profile.rng, AuctionMarkConstants.USER_MIN_BALANCE,
                                                       AuctionMarkConstants.USER_MAX_BALANCE, 1.001);
        }

        @Override
        public void init() {
            // Populate the profile's users per item count histogram so that we know how many
            // items that each user should have. This will then be used to calculate the
            // the user ids by placing them into numeric ranges
            int max_items = Math.max(1, (int)Math.ceil(AuctionMarkConstants.ITEM_MAX_ITEMS_PER_SELLER * profile.getScaleFactor()));
            assert(max_items > 0);
            Zipf randomNumItems = new Zipf(profile.rng,
                                           AuctionMarkConstants.ITEM_MIN_ITEMS_PER_SELLER,
                                           max_items,
                                           1.001);
            for (long i = 0; i < this.tableSize; i++) {
                long num_items = randomNumItems.nextInt();
                profile.users_per_item_count.put(num_items);
            } // FOR
            if (LOG.isTraceEnabled())
                LOG.trace("Users Per Item Count:\n" + profile.users_per_item_count);
            this.idGenerator = new UserIdGenerator(profile.users_per_item_count, benchmark.getWorkloadConfiguration().getTerminals());
            assert(this.idGenerator.hasNext());
        }
        @Override
        public void prepare() {
            // Nothing to do
        }
        @Override
        public synchronized boolean hasMore() {
            return this.idGenerator.hasNext();
        }
        @Override
        protected int populateRow(Object[] row) {
            int col = 0;

            UserId u_id = this.idGenerator.next();
            
            // U_ID
            row[col++] = u_id;
            // U_RATING
            row[col++] = this.randomRating.nextInt();
            // U_BALANCE
            row[col++] = (this.randomBalance.nextInt()) / 10.0;
            // U_COMMENTS
            row[col++] = 0;
            // U_R_ID
            row[col++] = this.randomRegion.nextInt();
            // U_CREATED
            row[col++] = new Date(System.currentTimeMillis());
            // U_UPDATED
            row[col++] = new Date(System.currentTimeMillis());
            
            this.updateSubTableGenerators(u_id);
            return (col);
        }
    }

    /**********************************************************************************************
     * USER_ATTRIBUTES Generator
     **********************************************************************************************/
    protected class UserAttributesGenerator extends SubTableGenerator<UserId> {
        private final Zipf randomNumUserAttributes;
        
        public UserAttributesGenerator() {
            super(AuctionMarkConstants.TABLENAME_USER_ATTRIBUTES,
                  AuctionMarkConstants.TABLENAME_USER);
            
            this.randomNumUserAttributes = new Zipf(profile.rng,
                                                    AuctionMarkConstants.USER_MIN_ATTRIBUTES,
                                                    AuctionMarkConstants.USER_MAX_ATTRIBUTES, 1.001);
        }
        @Override
        protected short getElementCounter(UserId user_id) {
            return (short)(randomNumUserAttributes.nextInt());
        }
        @Override
        protected int populateRow(UserId user_id, Object[] row, short remaining) {
            int col = 0;
            
            // UA_ID
            row[col++] = this.count;
            // UA_U_ID
            row[col++] = user_id;
            // UA_NAME
            row[col++] = profile.rng.astring(AuctionMarkConstants.USER_ATTRIBUTE_NAME_LENGTH_MIN,
                                             AuctionMarkConstants.USER_ATTRIBUTE_NAME_LENGTH_MAX);
            // UA_VALUE
            row[col++] = profile.rng.astring(AuctionMarkConstants.USER_ATTRIBUTE_VALUE_LENGTH_MIN,
                                             AuctionMarkConstants.USER_ATTRIBUTE_VALUE_LENGTH_MAX);
            // U_CREATED
            row[col++] = new Date(System.currentTimeMillis());
            
            return (col);
        }
    } // END CLASS

    /**********************************************************************************************
     * ITEM Generator
     **********************************************************************************************/
    protected class ItemGenerator extends SubTableGenerator<UserId> {
        
        /**
         * BidDurationDay -> Pair<NumberOfBids, NumberOfWatches>
         */
        private final Map<Long, Pair<Zipf, Zipf>> item_bid_watch_zipfs = new HashMap<Long, Pair<Zipf,Zipf>>();
        
        public ItemGenerator() {
            super(AuctionMarkConstants.TABLENAME_ITEM,
                  AuctionMarkConstants.TABLENAME_USER,
                  AuctionMarkConstants.TABLENAME_CATEGORY);
        }
        
        @Override
        protected short getElementCounter(UserId user_id) {
            return (short)(user_id.getItemCount());
        }

        @Override
        public void init() {
            super.init();
            this.tableSize = 0l;
            for (Long size : profile.users_per_item_count.values()) {
                this.tableSize += size.intValue() * profile.users_per_item_count.get(size);
            } // FOR
        }
        @Override
        protected int populateRow(UserId seller_id, Object[] row, short remaining) {
            int col = 0;
            
            ItemId itemId = new ItemId(seller_id, remaining);
            Date endDate = this.getRandomEndTimestamp();
            Date startDate = this.getRandomStartTimestamp(endDate); 
            if (LOG.isTraceEnabled())
                LOG.trace("endDate = " + endDate + " : startDate = " + startDate);
            
            long bidDurationDay = ((endDate.getTime() - startDate.getTime()) / AuctionMarkConstants.MILLISECONDS_IN_A_DAY);
            if (this.item_bid_watch_zipfs.containsKey(bidDurationDay) == false) {
                Zipf randomNumBids = new Zipf(profile.rng,
                        AuctionMarkConstants.ITEM_MIN_BIDS_PER_DAY * (int)bidDurationDay,
                        AuctionMarkConstants.ITEM_MAX_BIDS_PER_DAY * (int)bidDurationDay,
                        1.001);
                Zipf randomNumWatches = new Zipf(profile.rng,
                        AuctionMarkConstants.ITEM_MIN_WATCHES_PER_DAY * (int)bidDurationDay,
                        (int)Math.ceil(AuctionMarkConstants.ITEM_MAX_WATCHES_PER_DAY * (int)bidDurationDay * profile.getScaleFactor()), 1.001);
                this.item_bid_watch_zipfs.put(bidDurationDay, Pair.of(randomNumBids, randomNumWatches));
            }
            Pair<Zipf, Zipf> p = this.item_bid_watch_zipfs.get(bidDurationDay);
            assert(p != null);

            // Calculate the number of bids and watches for this item
            short numBids = (short)p.getFirst().nextInt();
            short numWatches = (short)p.getSecond().nextInt();
            
            // Create the ItemInfo object that we will use to cache the local data 
            // for this item. This will get garbage collected once all the derivative
            // tables are done with it.
            LoaderItemInfo itemInfo = new LoaderItemInfo(itemId, endDate, numBids);
            itemInfo.sellerId = seller_id;
            itemInfo.startDate = startDate;
            itemInfo.initialPrice = profile.randomInitialPrice.nextInt();
            assert(itemInfo.initialPrice > 0) : "Invalid initial price for " + itemId;
            itemInfo.numImages = (short) profile.randomNumImages.nextInt();
            itemInfo.numAttributes = (short) profile.randomNumAttributes.nextInt();
            itemInfo.numBids = numBids;
            itemInfo.numWatches = numWatches;
            
            // The auction for this item has already closed
            if (itemInfo.endDate.getTime() <= profile.getBenchmarkStartTime().getTime()) {
                // Somebody won a bid and bought the item
                if (itemInfo.numBids > 0) {
                    itemInfo.lastBidderId = profile.getRandomBuyerId(itemInfo.sellerId);
                    itemInfo.purchaseDate = this.getRandomPurchaseTimestamp(itemInfo.endDate);
                    itemInfo.numComments = (short) profile.randomNumComments.nextInt();
                }
                itemInfo.status = ItemStatus.CLOSED;
            }
            // Item is still available
            else if (itemInfo.numBids > 0) {
        		itemInfo.lastBidderId = profile.getRandomBuyerId(itemInfo.sellerId);
            }
            profile.addItemToProperQueue(itemInfo, true);

            // I_ID
            row[col++] = itemInfo.itemId;
            // I_U_ID
            row[col++] = itemInfo.sellerId;
            // I_C_ID
            row[col++] = profile.getRandomCategoryId();
            // I_NAME
            row[col++] = profile.rng.astring(AuctionMarkConstants.ITEM_NAME_LENGTH_MIN,
                                             AuctionMarkConstants.ITEM_NAME_LENGTH_MAX);
            // I_DESCRIPTION
            row[col++] = profile.rng.astring(AuctionMarkConstants.ITEM_DESCRIPTION_LENGTH_MIN,
                                             AuctionMarkConstants.ITEM_DESCRIPTION_LENGTH_MAX);
            // I_USER_ATTRIBUTES
            row[col++] = profile.rng.astring(AuctionMarkConstants.ITEM_USER_ATTRIBUTES_LENGTH_MIN,
                                             AuctionMarkConstants.ITEM_USER_ATTRIBUTES_LENGTH_MAX);
            // I_INITIAL_PRICE
            row[col++] = itemInfo.initialPrice;

            // I_CURRENT_PRICE
            if (itemInfo.numBids > 0) {
                itemInfo.currentPrice = itemInfo.initialPrice + (itemInfo.numBids * itemInfo.initialPrice * AuctionMarkConstants.ITEM_BID_PERCENT_STEP);
                row[col++] = itemInfo.currentPrice;
            } else {
                row[col++] = itemInfo.initialPrice;
            }

            // I_NUM_BIDS
            row[col++] = itemInfo.numBids;
            // I_NUM_IMAGES
            row[col++] = itemInfo.numImages;
            // I_NUM_GLOBAL_ATTRS
            row[col++] = itemInfo.numAttributes;
            // I_NUM_COMMENTS
            row[col++] = itemInfo.numComments;
            // I_START_DATE
            row[col++] = itemInfo.startDate;
            // I_END_DATE
            row[col++] = itemInfo.endDate;
            // I_STATUS
            row[col++] = itemInfo.status.ordinal();
            // I_UPDATED
            row[col++] = itemInfo.startDate;

            this.updateSubTableGenerators(itemInfo);
            return (col);
        }

        private Date getRandomStartTimestamp(Date endDate) {
            long duration = ((long)profile.randomDuration.nextInt()) * AuctionMarkConstants.MILLISECONDS_IN_A_DAY;
            long lStartTimestamp = endDate.getTime() - duration;
            Date startTimestamp = new Date(lStartTimestamp);
            return startTimestamp;
        }
        private Date getRandomEndTimestamp() {
            int timeDiff = profile.randomTimeDiff.nextInt();
            Date time = new Date(profile.getBenchmarkStartTime().getTime() + (timeDiff * AuctionMarkConstants.MILLISECONDS_IN_A_SECOND));
//            LOG.info(timeDiff + " => " + sdf.format(time.asApproximateJavaDate()));
            return time;
        }
        private Date getRandomPurchaseTimestamp(Date endDate) {
            long duration = profile.randomPurchaseDuration.nextInt();
            return new Date(endDate.getTime() + duration * AuctionMarkConstants.MILLISECONDS_IN_A_DAY);
        }
    }
    
    /**********************************************************************************************
     * ITEM_IMAGE Generator
     **********************************************************************************************/
    protected class ItemImageGenerator extends SubTableGenerator<LoaderItemInfo> {

        public ItemImageGenerator() {
            super(AuctionMarkConstants.TABLENAME_ITEM_IMAGE,
                  AuctionMarkConstants.TABLENAME_ITEM);
        }
        @Override
        public short getElementCounter(LoaderItemInfo itemInfo) {
            return itemInfo.numImages;
        }
        @Override
        protected int populateRow(LoaderItemInfo itemInfo, Object[] row, short remaining) {
            int col = 0;

            // II_ID
            row[col++] = this.count;
            // II_I_ID
            row[col++] = itemInfo.itemId;
            // II_U_ID
            row[col++] = itemInfo.sellerId;

            return (col);
        }
    } // END CLASS
    
    /**********************************************************************************************
     * ITEM_ATTRIBUTE Generator
     **********************************************************************************************/
    protected class ItemAttributeGenerator extends SubTableGenerator<LoaderItemInfo> {

        public ItemAttributeGenerator() {
            super(AuctionMarkConstants.TABLENAME_ITEM_ATTRIBUTE,
                  AuctionMarkConstants.TABLENAME_ITEM,
                  AuctionMarkConstants.TABLENAME_GLOBAL_ATTRIBUTE_GROUP, AuctionMarkConstants.TABLENAME_GLOBAL_ATTRIBUTE_VALUE);
        }
        @Override
        public short getElementCounter(LoaderItemInfo itemInfo) {
            return itemInfo.numAttributes;
        }
        @Override
        protected int populateRow(LoaderItemInfo itemInfo, Object[] row, short remaining) {
            int col = 0;
            GlobalAttributeValueId gav_id = profile.getRandomGlobalAttributeValue();
            assert(gav_id != null);
            
            // IA_ID
            row[col++] = this.count;
            // IA_I_ID
            row[col++] = itemInfo.itemId;
            // IA_U_ID
            row[col++] = itemInfo.sellerId;
            // IA_GAV_ID
            row[col++] = gav_id.encode();
            // IA_GAG_ID
            row[col++] = gav_id.getGlobalAttributeGroup().encode();

            return (col);
        }
    } // END CLASS

    /**********************************************************************************************
     * ITEM_COMMENT Generator
     **********************************************************************************************/
    protected class ItemCommentGenerator extends SubTableGenerator<LoaderItemInfo> {

        public ItemCommentGenerator() {
            super(AuctionMarkConstants.TABLENAME_ITEM_COMMENT,
                  AuctionMarkConstants.TABLENAME_ITEM);
        }
        @Override
        public short getElementCounter(LoaderItemInfo itemInfo) {
            return (itemInfo.purchaseDate != null ? itemInfo.numComments : 0);
        }
        @Override
        protected int populateRow(LoaderItemInfo itemInfo, Object[] row, short remaining) {
            int col = 0;

            // IC_ID
            row[col++] = new Integer((int) this.count);
            // IC_I_ID
            row[col++] = itemInfo.itemId;
            // IC_U_ID
            row[col++] = itemInfo.sellerId;
            // IC_BUYER_ID
            row[col++] = itemInfo.lastBidderId;
            // IC_QUESTION
            row[col++] = profile.rng.astring(AuctionMarkConstants.ITEM_COMMENT_LENGTH_MIN,
                                             AuctionMarkConstants.ITEM_COMMENT_LENGTH_MAX);
            // IC_RESPONSE
            row[col++] = profile.rng.astring(AuctionMarkConstants.ITEM_COMMENT_LENGTH_MIN,
                                             AuctionMarkConstants.ITEM_COMMENT_LENGTH_MAX);
            // IC_CREATED
            row[col++] = this.getRandomCommentDate(itemInfo.startDate, itemInfo.endDate);
            // IC_UPDATED
            row[col++] = this.getRandomCommentDate(itemInfo.startDate, itemInfo.endDate);

            return (col);
        }
        private Date getRandomCommentDate(Date startDate, Date endDate) {
            int start = Math.round(startDate.getTime() / AuctionMarkConstants.MILLISECONDS_IN_A_SECOND);
            int end = Math.round(endDate.getTime() / AuctionMarkConstants.MILLISECONDS_IN_A_SECOND);
            return new Date((profile.rng.number(start, end)) * AuctionMarkConstants.MILLISECONDS_IN_A_SECOND);
        }
    }

    /**********************************************************************************************
     * ITEM_BID Generator
     **********************************************************************************************/
    protected class ItemBidGenerator extends SubTableGenerator<LoaderItemInfo> {

        private LoaderItemInfo.Bid bid = null;
        private float currentBidPriceAdvanceStep;
        private long currentCreateDateAdvanceStep;
        private boolean new_item;
        
        public ItemBidGenerator() {
            super(AuctionMarkConstants.TABLENAME_ITEM_BID,
                  AuctionMarkConstants.TABLENAME_ITEM);
        }
        @Override
        public short getElementCounter(LoaderItemInfo itemInfo) {
            return ((short)itemInfo.numBids);
        }
        @Override
        protected int populateRow(LoaderItemInfo itemInfo, Object[] row, short remaining) {
            int col = 0;
            assert(itemInfo.numBids > 0);
            
            UserId bidderId = null;
            
            // Figure out the UserId for the person bidding on this item now
            if (this.new_item) {
                // If this is a new item and there is more than one bid, then
                // we'll choose the bidder's UserId at random.
                // If there is only one bid, then it will have to be the last bidder
                bidderId = (itemInfo.numBids == 1 ? itemInfo.lastBidderId :
                                                    profile.getRandomBuyerId(itemInfo.sellerId));
                Date endDate;
                if (itemInfo.status == ItemStatus.OPEN) {
                    endDate = profile.getBenchmarkStartTime();
                } else {
                    endDate = itemInfo.endDate;
                }
                this.currentCreateDateAdvanceStep = (endDate.getTime() - itemInfo.startDate.getTime()) / (remaining + 1);
                this.currentBidPriceAdvanceStep = itemInfo.initialPrice * AuctionMarkConstants.ITEM_BID_PERCENT_STEP;
            }
            // The last bid must always be the item's lastBidderId
            else if (remaining == 0) {
                bidderId = itemInfo.lastBidderId; 
            }
            // The first bid for a two-bid item must always be different than the lastBidderId
            else if (itemInfo.numBids == 2) {
                assert(remaining == 1);
                bidderId = profile.getRandomBuyerId(itemInfo.lastBidderId, itemInfo.sellerId);
            } 
            // Since there are multiple bids, we want randomly select one based on the previous bidders
            // We will get the histogram of bidders so that we are more likely to select
            // an existing bidder rather than a completely random one
            else {
                assert(this.bid != null);
                Histogram<UserId> bidderHistogram = itemInfo.getBidderHistogram();
                bidderId = profile.getRandomBuyerId(bidderHistogram, this.bid.bidderId, itemInfo.sellerId);
            }
            assert(bidderId != null);

            float last_bid = (this.new_item ? itemInfo.initialPrice : this.bid.maxBid);
            this.bid = itemInfo.getNextBid(this.count, bidderId);
            this.bid.createDate = new Date(itemInfo.startDate.getTime() + this.currentCreateDateAdvanceStep);
            this.bid.updateDate = this.bid.createDate; 
            
            if (remaining == 0) {
                this.bid.maxBid = itemInfo.currentPrice;
                if (itemInfo.purchaseDate != null) {
                    assert(itemInfo.getBidCount() == itemInfo.numBids) : String.format("%d != %d\n%s", itemInfo.getBidCount(), itemInfo.numBids, itemInfo);
                }
            } else {
                this.bid.maxBid = last_bid + this.currentBidPriceAdvanceStep;
            }
            
            // IB_ID
            row[col++] = new Long(this.bid.id);
            // IB_I_ID
            row[col++] = itemInfo.itemId;
            // IB_U_ID
            row[col++] = itemInfo.sellerId;
            // IB_BUYER_ID
            row[col++] = this.bid.bidderId;
            // IB_BID
            row[col++] = this.bid.maxBid - (remaining > 0 ? (this.currentBidPriceAdvanceStep/2.0f) : 0);
            // IB_MAX_BID
            row[col++] = this.bid.maxBid;
            // IB_CREATED
            row[col++] = this.bid.createDate;
            // IB_UPDATED
            row[col++] = this.bid.updateDate;

            if (remaining == 0) this.updateSubTableGenerators(itemInfo);
            return (col);
        }
        @Override
        protected void newElementCallback(LoaderItemInfo itemInfo) {
            this.new_item = true;
            this.bid = null;
        }
    }

    /**********************************************************************************************
     * ITEM_MAX_BID Generator
     **********************************************************************************************/
    protected class ItemMaxBidGenerator extends SubTableGenerator<LoaderItemInfo> {

        public ItemMaxBidGenerator() {
            super(AuctionMarkConstants.TABLENAME_ITEM_MAX_BID,
                AuctionMarkConstants.TABLENAME_ITEM_BID);
        }
        @Override
        public short getElementCounter(LoaderItemInfo itemInfo) {
            return (short)(itemInfo.getBidCount() > 0 ? 1 : 0);
        }
        @Override
        protected int populateRow(LoaderItemInfo itemInfo, Object[] row, short remaining) {
            int col = 0;
            LoaderItemInfo.Bid bid = itemInfo.getLastBid();
            assert(bid != null) : "No bids?\n" + itemInfo;

            // IMB_I_ID
            row[col++] = itemInfo.itemId;
            // IMB_U_ID
            row[col++] = itemInfo.sellerId;
            // IMB_IB_ID
            row[col++] = bid.id;
            // IMB_IB_I_ID
            row[col++] = itemInfo.itemId;
            // IMB_IB_U_ID
            row[col++] = itemInfo.sellerId;
            // IMB_CREATED
            row[col++] = bid.createDate;
            // IMB_UPDATED
            row[col++] = bid.updateDate;

            return (col);
        }
    }

    /**********************************************************************************************
     * ITEM_PURCHASE Generator
     **********************************************************************************************/
    protected class ItemPurchaseGenerator extends SubTableGenerator<LoaderItemInfo> {

        public ItemPurchaseGenerator() {
            super(AuctionMarkConstants.TABLENAME_ITEM_PURCHASE,
                  AuctionMarkConstants.TABLENAME_ITEM_BID);
        }
        @Override
        public short getElementCounter(LoaderItemInfo itemInfo) {
            return (short)(itemInfo.getBidCount() > 0 && itemInfo.purchaseDate != null ? 1 : 0);
        }
        @Override
        protected int populateRow(LoaderItemInfo itemInfo, Object[] row, short remaining) {
            int col = 0;
            LoaderItemInfo.Bid bid = itemInfo.getLastBid();
            assert(bid != null) : itemInfo;
            
            // IP_ID
            row[col++] = this.count;
            // IP_IB_ID
            row[col++] = bid.id;
            // IP_IB_I_ID
            row[col++] = itemInfo.itemId;
            // IP_IB_U_ID
            row[col++] = itemInfo.sellerId;
            // IP_DATE
            row[col++] = itemInfo.purchaseDate;

            if (profile.rng.number(1, 100) <= AuctionMarkConstants.PROB_PURCHASE_BUYER_LEAVES_FEEDBACK) {
                bid.buyer_feedback = true;
            }
            if (profile.rng.number(1, 100) <= AuctionMarkConstants.PROB_PURCHASE_SELLER_LEAVES_FEEDBACK) {
                bid.seller_feedback = true;
            }
            
            if (remaining == 0) this.updateSubTableGenerators(bid);
            return (col);
        }
    } // END CLASS
    
    /**********************************************************************************************
     * USER_FEEDBACK Generator
     **********************************************************************************************/
    protected class UserFeedbackGenerator extends SubTableGenerator<LoaderItemInfo.Bid> {

        Set<ItemId> seenIds = new HashSet<ItemId>();
        
        public UserFeedbackGenerator() {
            super(AuctionMarkConstants.TABLENAME_USER_FEEDBACK,
                  AuctionMarkConstants.TABLENAME_ITEM_PURCHASE);
        }

        @Override
        protected short getElementCounter(LoaderItemInfo.Bid bid) {
            assert(seenIds.contains(bid.getLoaderItemInfo().itemId) == false);
            seenIds.add(bid.getLoaderItemInfo().itemId);
            return (short)((bid.buyer_feedback ? 1 : 0) + (bid.seller_feedback ? 1 : 0));
        }
        @Override
        protected int populateRow(LoaderItemInfo.Bid bid, Object[] row, short remaining) {
            int col = 0;

            boolean is_buyer = false;
            if (remaining > 1 || (bid.buyer_feedback && bid.seller_feedback == false)) {
                is_buyer = true;
            } else {
                assert(bid.seller_feedback);
                is_buyer = false;
            }
            LoaderItemInfo itemInfo = bid.getLoaderItemInfo();
            
            // UF_U_ID
            row[col++] = (is_buyer ? bid.bidderId : itemInfo.sellerId);
            // UF_I_ID
            row[col++] = itemInfo.itemId;
            // UF_I_U_ID
            row[col++] = itemInfo.sellerId;
            // UF_FROM_ID
            row[col++] = (is_buyer ? itemInfo.sellerId : bid.bidderId);
            // UF_RATING
            row[col++] = 1; // TODO
            // UF_DATE
            row[col++] = profile.getBenchmarkStartTime(); // Does this matter?

            return (col);
        }
    }

    /**********************************************************************************************
     * USER_ITEM Generator
     **********************************************************************************************/
    protected class UserItemGenerator extends SubTableGenerator<LoaderItemInfo> {

        public UserItemGenerator() {
            super(AuctionMarkConstants.TABLENAME_USER_ITEM,
                  AuctionMarkConstants.TABLENAME_ITEM_BID);
        }
        @Override
        public short getElementCounter(LoaderItemInfo itemInfo) {
            return (short)(itemInfo.getBidCount() > 0 && itemInfo.purchaseDate != null ? 1 : 0);
        }
        @Override
        protected int populateRow(LoaderItemInfo itemInfo, Object[] row, short remaining) {
            int col = 0;
            LoaderItemInfo.Bid bid = itemInfo.getLastBid();
            assert(bid != null) : itemInfo;
            
            // UI_U_ID
            row[col++] = bid.bidderId;
            // UI_I_ID
            row[col++] = itemInfo.itemId;
            // UI_I_U_ID
            row[col++] = itemInfo.sellerId;
            // UI_IP_ID
            row[col++] = null;
            // UI_IP_IB_ID
            row[col++] = null;
            // UI_IP_IB_I_ID
            row[col++] = null;
            // UI_IP_IB_U_ID
            row[col++] = null;
            // UI_CREATED
            row[col++] = itemInfo.endDate;
            
            return (col);
        }
    } // END CLASS

    /**********************************************************************************************
     * USER_WATCH Generator
     **********************************************************************************************/
    protected class UserWatchGenerator extends SubTableGenerator<LoaderItemInfo> {

        final Set<UserId> watchers = new HashSet<UserId>();
        
        public UserWatchGenerator() {
            super(AuctionMarkConstants.TABLENAME_USER_WATCH,
                  AuctionMarkConstants.TABLENAME_ITEM_BID);
        }
        @Override
        public short getElementCounter(LoaderItemInfo itemInfo) {
            return (itemInfo.numWatches);
        }
        @Override
        protected int populateRow(LoaderItemInfo itemInfo, Object[] row, short remaining) {
            int col = 0;
            
            // Make it more likely that a user that has bid on an item is watching it
            Histogram<UserId> bidderHistogram = itemInfo.getBidderHistogram();
            UserId buyerId = null;
            boolean use_random = (this.watchers.size() == bidderHistogram.getValueCount());
            
            if (LOG.isTraceEnabled())
                LOG.trace(String.format("Selecting USER_WATCH buyerId [useRandom=%s, size=%d]", use_random, this.watchers.size()));
            while (buyerId == null) {
                try {
                    if (use_random) {
                        buyerId = profile.getRandomBuyerId();        
                    } else {
                        buyerId = profile.getRandomBuyerId(bidderHistogram, itemInfo.sellerId);
                    }
                } catch (NullPointerException ex) {
                    LOG.error("Busted Bidder Histogram:\n" + bidderHistogram);
                    throw ex;
                }
                if (this.watchers.contains(buyerId) == false) break;
                buyerId = null;
            } // WHILE
            assert(buyerId != null);
            this.watchers.add(buyerId);
            
            // UW_U_ID
            row[col++] = buyerId;
            // UW_I_ID
            row[col++] = itemInfo.itemId;
            // UW_I_U_ID
            row[col++] = itemInfo.sellerId;
            // UW_CREATED
            row[col++] = this.getRandomDate(itemInfo.startDate, itemInfo.endDate);

            return (col);
        }
        @Override
        protected void finishElementCallback(LoaderItemInfo t) {
            if (LOG.isTraceEnabled())
                LOG.trace("Clearing watcher cache [size=" + this.watchers.size() + "]");
            this.watchers.clear();
        }
        private Date getRandomDate(Date startDate, Date endDate) {
            int start = Math.round(startDate.getTime() / AuctionMarkConstants.MILLISECONDS_IN_A_SECOND);
            int end = Math.round(endDate.getTime() / AuctionMarkConstants.MILLISECONDS_IN_A_SECOND);
            long offset = profile.rng.number(start, end);
            return new Date(offset * AuctionMarkConstants.MILLISECONDS_IN_A_SECOND);
        }
    } // END CLASS
} // END CLASS