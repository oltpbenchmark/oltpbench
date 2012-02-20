package com.oltpbenchmark.benchmarks.auctionmark;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import com.oltpbenchmark.api.AbstractTestLoader;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.auctionmark.util.ItemInfo;
import com.oltpbenchmark.util.Histogram;
import com.oltpbenchmark.util.RandomGenerator;

public class TestAuctionMarkLoader extends AbstractTestLoader<AuctionMarkBenchmark> {

    static {
        org.apache.log4j.PropertyConfigurator.configure("/home/pavlo/Documents/OLTPBenchmark/OLTPBenchmark/log4j.properties");
    }
    
    private static String IGNORE[] = {
//        AuctionMarkConstants.TABLENAME_CONFIG_PROFILE,
    };
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(AuctionMarkBenchmark.class, IGNORE, TestAuctionMarkBenchmark.PROC_CLASSES);
        this.workConf.setScaleFactor(0.1);
    }
    
    /**
     * testSaveLoadProfile
     */
    public void testSaveLoadProfile() throws Exception {
        AuctionMarkProfile.clearCachedProfile();
        AuctionMarkLoader loader = (AuctionMarkLoader)this.benchmark.makeLoaderImpl(conn);
        assertNotNull(loader);
        loader.load();
        
        AuctionMarkProfile orig = loader.profile;
        assertNotNull(orig);
        assertFalse(orig.users_per_item_count.isEmpty());
        
        AuctionMarkProfile copy = new AuctionMarkProfile(this.benchmark, new RandomGenerator(0));
        assertTrue(copy.users_per_item_count.isEmpty());
        
        List<Worker> workers = this.benchmark.makeWorkers(false);
        AuctionMarkWorker worker = (AuctionMarkWorker)workers.get(0); 
        copy.loadProfile(worker);
        
        assertEquals(orig.scale_factor, copy.scale_factor);
        assertEquals(orig.benchmarkStartTime.toString(), copy.benchmarkStartTime.toString());
        assertEquals(orig.users_per_item_count, copy.users_per_item_count);
    }
    
    /**
     * testLoadProfilePerClient
     */
    public void testLoadProfilePerClient() throws Exception {
        // We don't have to reload our cached profile here
        // We just want to make sure that each client's profile contains a unique
        // set of ItemInfo records that are not found in any other profile's lists
        int num_clients = 9;
        this.workConf.setTerminals(num_clients);
        AuctionMarkLoader loader = (AuctionMarkLoader)this.benchmark.makeLoaderImpl(conn);
        assertNotNull(loader);
        
        Set<ItemInfo> allItemInfos = new HashSet<ItemInfo>();
        Set<ItemInfo> clientItemInfos = new HashSet<ItemInfo>();
        Histogram<Integer> clientItemCtr = new Histogram<Integer>();
        List<Worker> workers = this.benchmark.makeWorkers(false);
        assertEquals(num_clients, workers.size());
        for (int i = 0; i < num_clients; i++) {
            AuctionMarkWorker worker = (AuctionMarkWorker)workers.get(i);
            assertNotNull(worker);
            worker.initialize(); // Initializes the profile we need
            
            clientItemInfos.clear();
            for (LinkedList<ItemInfo> items : worker.profile.allItemSets) {
                assertNotNull(items);
                for (ItemInfo itemInfo : items) {
                    // Make sure we haven't seen it another list for this client
                    assertFalse(itemInfo.toString(), clientItemInfos.contains(itemInfo));
                    // Nor that we have seen it in any other client 
                    assertFalse(itemInfo.toString(), allItemInfos.contains(itemInfo));
                } // FOR
                clientItemInfos.addAll(items);
            } // FOR
            clientItemCtr.put(i, clientItemInfos.size());
            allItemInfos.addAll(clientItemInfos);
            assert(clientItemInfos.size() > 0);
        } // FOR
        System.err.println(clientItemCtr);
    }
    
}
