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

package com.oltpbenchmark.benchmarks.linkbench;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.log4j.Logger;

import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.benchmarks.linkbench.generators.DataGenerator;
import com.oltpbenchmark.benchmarks.linkbench.distributions.ID2Chooser;
import com.oltpbenchmark.benchmarks.linkbench.distributions.LogNormalDistribution;
import com.oltpbenchmark.benchmarks.linkbench.pojo.Link;
import com.oltpbenchmark.benchmarks.linkbench.pojo.Node;
import com.oltpbenchmark.benchmarks.linkbench.procedures.AddLink;
import com.oltpbenchmark.benchmarks.linkbench.procedures.AddNode;
import com.oltpbenchmark.benchmarks.linkbench.procedures.CountLink;
import com.oltpbenchmark.benchmarks.linkbench.utils.ConfigUtil;
import com.oltpbenchmark.util.ClassUtil;

public class LinkBenchLoader extends Loader<LinkBenchBenchmark> {
    private static final Logger LOG = Logger.getLogger(LinkBenchLoader.class);
    
    private LinkBenchProfile profile;

    private final boolean genNodes;
    
    // Counters for load statistics
    private long sameShuffle;
    private long diffShuffle;
    private long linksloaded;

    private final int nTotalLoaders;
    private final int nLinkLoaders;
    private final long chunkSize;

    private AddLink addLink;
    private AddNode addNode;

    public LinkBenchLoader(LinkBenchBenchmark benchmark, Properties props, Connection conn, Random rng) {
        super(benchmark, conn);
        
        this.profile = new LinkBenchProfile(benchmark, rng, props);
        
        this.genNodes = ConfigUtil.getBool(props, LinkBenchConstants.GENERATE_NODES);

        if (genNodes) {
        	profile.initNodeDataGeneration();
        }
        profile.initLinkDataGeneration();

        // Counters for load statistics
        this.sameShuffle = 0;
        this.diffShuffle = 0;
        this.linksloaded = 0;
        this.nTotalLoaders = ConfigUtil.getInt(props, LinkBenchConstants.NUM_LOADERS);
        this.nLinkLoaders = genNodes ? nTotalLoaders - 1 : nTotalLoaders;
        this.chunkSize = ConfigUtil.getLong(props, LinkBenchConstants.LOADER_CHUNK_SIZE);
        
        if (genNodes) {
            this.addNode = new AddNode();
        }
        this.addLink = new AddLink();
    }

    @Override
    public List<LoaderThread> createLoaderThreads() throws SQLException {
        List<LoaderThread> threads = new ArrayList<LoaderThread>();

        if (genNodes) {
            threads.add(new LoaderThread() {
                @Override
                public void load(Connection conn) throws SQLException {
                    LOG.info("Loader thread #" + String.valueOf(nTotalLoaders - 1) + " processing nodes");
                    loadNodes(conn);
                    LOG.info("Loader thread #" + String.valueOf(nTotalLoaders - 1) + " finished");
                }
            });
        }
        
        // each linked list is a thread's stack of LoadChunks
        ArrayList<LinkedList<LoadChunk>> threadChunks = new ArrayList<LinkedList<LoadChunk>>(nLinkLoaders);
        for (int i = 0; i < nLinkLoaders; i++) {
            threadChunks.add(new LinkedList<LoadChunk>());
        }
        int chunkNum = 0;
        
        // iterate over the id space and add LoadChunks to thread stacks
        for (long id1 = profile.getStartid1(); id1 < profile.getMaxid1(); id1 += chunkSize) {
            int threadInd = chunkNum % nLinkLoaders;
            LoadChunk chunk = new LoadChunk(chunkNum, id1,
                    Math.min(id1 + chunkSize, profile.getMaxid1()), profile.getRng());
            threadChunks.get(threadInd).add(chunk);
            chunkNum++;
        }
        
        // add a thread to process each stack of chunks
        for (int i = 0; i < nLinkLoaders; i++) {
            final int loaderID = i;
            final LinkedList<LoadChunk> chunks = threadChunks.get(i);
            threads.add(new LoaderThread() {
                @Override
                public void load(Connection conn) throws SQLException {
                    // pop in reverse order as later chunks tend to be larger
                    for (int i = chunks.size() - 1; i >= 0; i--) {
                        LoadChunk chunk = chunks.get(i);
                           LOG.info("Loader thread #" + loaderID + " processing "
                                   + chunk.toString());
                           processChunk(loaderID, chunk, conn);
                    }
                    LOG.info("Loader thread #" + loaderID + " finished");
                }
            });
        }

        return threads;
    }
    
    private void loadNodes(Connection conn) {
        // reuse Node object
        Node node = new Node(profile.getStartid1(), LinkBenchConstants.DEFAULT_NODE_TYPE, 1,
                (int)(System.currentTimeMillis()/1000L), null);
        
        for (long id = profile.getStartid1(); id < profile.getMaxid1(); id++) {
            node.id = id;
            node.time = (int)(System.currentTimeMillis()/1000L);
            node.data = profile.getNodeAddData();
            
            loadNode(conn, node);
        }
    }
    
    private void processChunk(int loaderID, LoadChunk chunk, Connection conn) {
        // Counter for total number of links loaded in chunk;
        long links_in_chunk = 0;
        
        Link link = new Link(chunk.start, LinkBenchConstants.DEFAULT_LINK_TYPE, chunk.start,
                LinkBenchConstants.VISIBILITY_DEFAULT, null, 0, (int)(System.currentTimeMillis()/1000L));

        long prevPercentPrinted = 0;
        for (long id1 = chunk.start; id1 < chunk.end; id1 += chunk.step) {
            long added_links= createOutLinks(conn, chunk.rng, link, id1);
            links_in_chunk += added_links;
        
            if (!profile.getSingleAssoc()) {
                long nloaded = (id1 - chunk.start) / chunk.step;
                long percent = 100 * nloaded/(chunk.size);
                if ((percent % 10 == 0) && (percent > prevPercentPrinted)) {
                    LOG.debug(chunk.toString() +  ": Percent done = " + percent);
                    prevPercentPrinted = percent;
                }
            }
        }
    }
    
    private long createOutLinks(Connection conn, Random rng, Link link, long id1) {
        long nlinks_total = 0;
        for (long link_type: profile.getID2Chooser().getLinkTypes()) {
            long nlinks = profile.getID2Chooser().calcLinkCount(id1, link_type);
            nlinks_total += nlinks;

            if (profile.getID2Chooser().sameShuffle) {
                sameShuffle++;
            } else {
                diffShuffle++;
            }

            for (long outlink_count = 0; outlink_count < nlinks; outlink_count++) {
                if (profile.getSingleAssoc()) {
                  // some constant value
                  link.id2 = 45;
                }
                else {
                    link.id2 = profile.getID2Chooser().chooseForLoad(rng, id1, link_type, outlink_count);
                      link.data = profile.getLinkAddData();
                }
                link.link_type = link_type;

                loadLink(conn, link);
            }

        }

        return nlinks_total;
    }

    private void loadNode(Connection conn, Node node) {
        try {
           	addNode.run(conn, node);
        }  catch (SQLException ex) {
            SQLException next = ex.getNextException();
            LOG.error("Failed to load data for LinkBench", ex);
            if (next != null) LOG.error(ex.getClass().getSimpleName() + " Cause => " + next.getMessage());

            LOG.debug("Rolling back changes from last batch");
            transRollback(conn);
        }
    }
    
    private void loadLink(Connection conn, Link link) {
        try {
        	addLink.run(conn, link, false);
        } catch (SQLException ex) {
            SQLException next = ex.getNextException();
            LOG.error("Failed to load data for LinkBench", ex);
            if (next != null) LOG.error(ex.getClass().getSimpleName() + " Cause => " + next.getMessage());

            LOG.debug("Rolling back changes from last batch");
            transRollback(conn);
        }
    }
    
    private void transRollback(Connection conn) {
        try {
            conn.rollback();
        } catch (SQLException se) {
            LOG.debug(se.getMessage());
        }
    }
    
    @Override
    public void load() throws SQLException {
        // NEEDS PORTING
        // TODO https://github.com/mdcallag/linkbench/blob/master/src/main/java/com/facebook/LinkBench/LinkBenchLoad.java#L99
    }
    
    /**
     * Represents a portion of the id space, starting with start,
     * going up until end (non-inclusive) with step size step
     */
    public static class LoadChunk {
        public static LoadChunk SHUTDOWN = new LoadChunk(true, 0, 0, 0, 1, null);

        public LoadChunk(long id, long start, long end, Random rng) {
            this(false, id, start, end, 1, rng);
        }

        public LoadChunk(boolean shutdown, long id, long start, long end, long step, Random rng) {
            super();
            this.shutdown = shutdown;
            this.id = id;
            this.start = start;
            this.end = end;
            this.step = step;
            this.size = (end - start) / step;
            this.rng = rng;
        }

        public final boolean shutdown;
        public final long id;
        public final long start;
        public final long end;
        public final long step;
        public final long size;
        public Random rng;

        public String toString() {
            if (shutdown) {
                return "chunk SHUTDOWN";
            }
            String range;
            if (step == 1) {
                range = "[" + start + ":" + end + "]";
            } else {
                range = "[" + start + ":" + step + ":" + end + "]";
            }
            return "chunk " + id + range;
        }
    }
}
