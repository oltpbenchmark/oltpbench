package com.oltpbenchmark.catalog;

import java.util.List;

import junit.framework.TestCase;

import com.oltpbenchmark.api.MockBenchmark;
import com.oltpbenchmark.util.FileUtil;
import com.oltpbenchmark.util.SQLUtil;

public class TestCatalog extends TestCase {

    static {
        org.apache.log4j.PropertyConfigurator.configure("/home/pavlo/Documents/OLTPBenchmark/OLTPBenchmark/log4j.properties");
    }
    
    private MockBenchmark benchmark;
    private Catalog catalog;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp();
        
        this.benchmark = new MockBenchmark();
        this.catalog = new Catalog(benchmark);
        assertNotNull(this.catalog);
        
        System.err.println("CATALOG:\n" + catalog);
    }
    
    /**
     * testGetTables
     */
    public void testGetTables() throws Exception {
        // Count the number of CREATE TABLEs in our test file
        String contents = FileUtil.readFile(this.benchmark.getDatabaseDDL());
        assertFalse(contents.isEmpty());
        int offset = 0;
        int num_tables = 0;
        while (offset < contents.length()) {
            offset = contents.indexOf("CREATE TABLE", offset);
            if (offset == -1) break;
            num_tables++;
            offset++;
        } // FOR
        assert(num_tables > 0);
        
        // Make sure that CatalogUtil returns the same number of tables
        assertEquals(num_tables, this.catalog.getTableCount());
        
        // Make sure that Map names match the Table names
        for (String table_name : this.catalog.getTableNames()) {
            Table catalog_tbl = this.catalog.getTable(table_name);
            assertNotNull(catalog_tbl);
            assertEquals(table_name, catalog_tbl.getName());
        } // FOR
    }
    
    /**
     * testPrimaryKeys
     */
    public void testPrimaryKeys() throws Exception {
        // All but one of our tables should have a single column primary key
        // The remaining table should have a multi-attribute primary key
        // that references all of our tables
        int num_tables = this.catalog.getTableCount();
        Table multicol_table = null;
        for (Table catalog_tbl : this.catalog.getTables()) {
            List<String> pkeys = catalog_tbl.getPrimaryKeyColumns();
            assertNotNull(pkeys);
            assertFalse(catalog_tbl.getName(), pkeys.isEmpty());
            
            if (pkeys.size() > 1) {
                assertNull(multicol_table);
                multicol_table = catalog_tbl;
                assertEquals(num_tables, pkeys.size());
            }
        } // FOR
        assertNotNull(multicol_table);
    }
    
    /**
     * testForeignKeys
     */
    public void testForeignKeys() throws Exception {
        // The C table should have two foreign keys
        Table catalog_tbl = this.catalog.getTable("C");
        int found = 0;
        assert(catalog_tbl != null) : this.catalog.getTableNames();
        for (Column catalog_col : catalog_tbl.getColumns()) {
            assertNotNull(catalog_col);
            Column fkey_col = catalog_col.getForeignKey();
            if (fkey_col != null) {
                assertFalse(fkey_col.getTable().equals(catalog_tbl));
                found++;
            }
        } // FOR
        assertEquals(2, found);
    }
    
    /**
     * testIndexes
     */
    public void testIndexes() throws Exception {
        // We should always have a PRIMARY KEY index
        for (Table catalog_tbl : this.catalog.getTables()) {
            assertNotNull(catalog_tbl);
            
            for (Index catalog_idx : catalog_tbl.getIndexes()) {
                assertNotNull(catalog_idx);
                assertEquals(catalog_tbl, catalog_idx.getTable());
                assert(catalog_idx.getColumnCount() > 0);
                
                for (int i = 0; i < catalog_idx.getColumnCount(); i++) {
                    assertNotNull(catalog_idx.getColumnName(i));
                    assertNotNull(catalog_idx.getColumnDirection(i));
                } // FOR
                System.err.println(catalog_idx.debug());
            } // FOR
        }
    }
    
    /**
     * testIntegerColumns
     */
    public void testIntegerColumns() throws Exception {
        // Any column that has a name with 'IATTR' in it is an integer
        // So we need to check to make sure that our little checker works
        for (Table catalog_tbl : this.catalog.getTables()) {
            assertNotNull(catalog_tbl);
            for (Column catalog_col : catalog_tbl.getColumns()) {
                assertNotNull(catalog_col);
                if (catalog_col.getName().contains("_IATTR")) {
                    boolean actual = SQLUtil.isIntegerType(catalog_col.getType());
                    assertTrue(catalog_col.fullName() + " -> " + catalog_col.getType(), actual);
                }
            } // FOR (col)
        } // FOR (table)
    }
}
