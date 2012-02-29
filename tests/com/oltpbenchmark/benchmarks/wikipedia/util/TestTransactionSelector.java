package com.oltpbenchmark.benchmarks.wikipedia.util;

import java.io.File;
import java.io.PrintStream;
import java.util.List;
import java.util.Random;

import com.oltpbenchmark.util.FileUtil;
import com.oltpbenchmark.util.TextGenerator;

import junit.framework.TestCase;

public class TestTransactionSelector extends TestCase {

    private static final int NUM_TRACES = 1000;
    private static final int NUM_USERS = 10000;
    private static final int NUM_NAMESPACES = 100;
    
    private Random rng = new Random();
    private File traceFile;
    
    @Override
    protected void setUp() throws Exception {
        this.traceFile = FileUtil.getTempFile("trace", true);
    }
    
    private void populateTraceFile(File f, boolean appendHyphen) throws Exception {
        // Generate a bunch of fake traces
        PrintStream ps = new PrintStream(f);
        for (int i = 0; i < NUM_TRACES; i++) {
            int userId = rng.nextInt(NUM_USERS-1)+1;
            int pageNamespace = rng.nextInt(NUM_NAMESPACES);
            String title = TextGenerator.randomStr(rng, rng.nextInt(100));
            if (appendHyphen) title += " - ";
            TransactionSelector.writeEntry(ps, userId, pageNamespace, title);
        } // FOR
        ps.close();
    }
    
    private void validate(List<WikipediaOperation> ops) {
        assertNotNull(ops);
        assertEquals(NUM_TRACES, ops.size());
        
        for (WikipediaOperation o : ops) {
            assertNotNull(o);
            assertTrue(o.toString(), o.userId > 0);
            assertNotNull(o.toString(), o.pageTitle);
            assertFalse(o.toString(), o.pageTitle.endsWith(" - "));
        } // FOR
    }
    
    @Override
    protected void tearDown() throws Exception {
        if (this.traceFile.exists()) this.traceFile.delete();
    }
    
    /**
     * testSynthetic
     */
    public void testSynthetic() throws Exception {
        this.populateTraceFile(this.traceFile, false);
        TransactionSelector ts = new TransactionSelector(this.traceFile, null);
        this.validate(ts.readAll());
    }
    
    /**
     * testReal
     */
    public void testReal() throws Exception {
        this.populateTraceFile(this.traceFile, true);
        TransactionSelector ts = new TransactionSelector(this.traceFile, null);
        this.validate(ts.readAll());
    }
    
}
