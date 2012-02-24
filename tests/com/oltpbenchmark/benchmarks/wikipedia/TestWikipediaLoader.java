package com.oltpbenchmark.benchmarks.wikipedia;

import com.oltpbenchmark.api.AbstractTestLoader;
import com.oltpbenchmark.catalog.Catalog;

public class TestWikipediaLoader extends AbstractTestLoader<WikipediaBenchmark> {

    @Override
    protected void setUp() throws Exception {
        super.setUp(WikipediaBenchmark.class, null, TestWikipediaBenchmark.PROC_CLASSES);
        this.workConf.setScaleFactor(0.1);
        
        // For some reason we have to do this for HSQLDB
        Catalog.setSeparator("");
        
    }

//    public void testHistograms() throws Exception {
//        Collection<Integer> values = TextHistograms.TEXT_LENGTH.values();
//        Histogram<Integer> new_h = new Histogram<Integer>();
//        for (Integer v : values) {
//            Integer cnt = TextHistograms.TEXT_LENGTH.get(v);
//            if (v >= 100000) {
//                int new_v = (int)Math.round(v / 1000.0d) * 1000;
//                new_h.put(new_v, cnt);
//            } else {
//                new_h.put(v, cnt);
//            }
//        }
//        for (Integer v : new_h.values()) {
//            Integer cnt = new_h.get(v);
//            System.err.printf("this.put(%d, %d);\n", v, cnt);
//        }
//    }
    
}
