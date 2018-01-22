package com.oltpbenchmark.benchmarks.linkbench;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Random;

import com.oltpbenchmark.benchmarks.linkbench.distributions.ID2Chooser;
import com.oltpbenchmark.benchmarks.linkbench.distributions.LogNormalDistribution;
import com.oltpbenchmark.benchmarks.linkbench.generators.DataGenerator;
import com.oltpbenchmark.benchmarks.linkbench.pojo.Link;
import com.oltpbenchmark.benchmarks.linkbench.pojo.Node;
import com.oltpbenchmark.benchmarks.linkbench.procedures.AddLink;
import com.oltpbenchmark.benchmarks.linkbench.procedures.AddNode;
import com.oltpbenchmark.benchmarks.linkbench.utils.ConfigUtil;
import com.oltpbenchmark.util.ClassUtil;
import com.oltpbenchmark.util.RandomGenerator;

public class LinkBenchProfile {
	
	private final LinkBenchBenchmark benchmark;

    private final long maxid1;
    private final long startid1;
    private boolean singleAssoc = false;
    
    private transient final Random rng;
    
    private Properties props;

    // Control data generation settings
    private transient LogNormalDistribution linkDataSize;
    private transient DataGenerator linkAddDataGen;
    private transient DataGenerator linkUpDataGen;
    private transient LogNormalDistribution nodeDataSize;
    private transient DataGenerator nodeAddDataGen;
    private transient DataGenerator nodeUpDataGen;
    
    private final ID2Chooser id2chooser;

	public LinkBenchProfile(LinkBenchBenchmark benchmark, Random rng, Properties props) {
		this.benchmark = benchmark;
		this.rng = rng;
		this.props = props;
		
        this.startid1 = ConfigUtil.getLong(props, LinkBenchConstants.MIN_ID);
        this.maxid1 = ConfigUtil.getLong(props, LinkBenchConstants.MAX_ID);

        // math functions may cause problems for id1 < 1
        if (startid1 <= 0) {
            throw new LinkBenchConfigError("startid1 must be >= 1");
        }
        if (maxid1 <= startid1) {
            throw new LinkBenchConfigError("maxid1 must be > startid1");
        }
        // is this a single assoc test?
        if (startid1 + 1 == maxid1) {
            singleAssoc = true;
        }

        this.id2chooser = new ID2Chooser(props, startid1, maxid1, 1, 1);
	}
	
    public void initLinkDataGeneration() throws ClassNotFoundException {
        double medLinkDataSize = ConfigUtil.getDouble(props,
                LinkBenchConstants.LINK_DATASIZE);
        linkDataSize = new LogNormalDistribution();
        linkDataSize.init(0, LinkBenchConstants.MAX_LINK_DATA, medLinkDataSize,
                LinkBenchConstants.LINK_DATASIZE_SIGMA);
        linkAddDataGen = ClassUtil.newInstance(
                ConfigUtil.getPropertyRequired(props, LinkBenchConstants.LINK_ADD_DATAGEN),
                DataGenerator.class);
        linkAddDataGen.init(props, LinkBenchConstants.LINK_ADD_DATAGEN_PREFIX);

        linkUpDataGen = ClassUtil.newInstance(
                ConfigUtil.getPropertyRequired(props, LinkBenchConstants.LINK_UP_DATAGEN),
                DataGenerator.class);
        linkUpDataGen.init(props, LinkBenchConstants.LINK_UP_DATAGEN_PREFIX);
    }

    public void initNodeDataGeneration() throws ClassNotFoundException {
        double medNodeDataSize = ConfigUtil.getDouble(props,
                LinkBenchConstants.NODE_DATASIZE);
        nodeDataSize = new LogNormalDistribution();
        nodeDataSize.init(0, LinkBenchConstants.MAX_NODE_DATA, medNodeDataSize,
                LinkBenchConstants.NODE_DATASIZE_SIGMA);

        String dataGenClass = ConfigUtil.getPropertyRequired(props,
                LinkBenchConstants.NODE_ADD_DATAGEN);
        nodeAddDataGen = ClassUtil.newInstance(dataGenClass,
                DataGenerator.class);
        nodeAddDataGen.init(props, LinkBenchConstants.NODE_ADD_DATAGEN_PREFIX);

        dataGenClass = ConfigUtil.getPropertyRequired(props,
                LinkBenchConstants.NODE_UP_DATAGEN);
        nodeUpDataGen = ClassUtil.newInstance(dataGenClass,
                DataGenerator.class);
        nodeUpDataGen.init(props, LinkBenchConstants.NODE_UP_DATAGEN_PREFIX);
    }
	
    public void loadNode(Connection conn, Node node) throws SQLException {
    	AddNode addNode = new AddNode();
        addNode.run(conn, node);
    }
    
    public void loadLink(Connection conn, Link link) throws SQLException {
    	AddLink addLink = new AddLink();
        addLink.run(conn, link, false);
    }
    
    public long getStartid1() {
    	return startid1;
    }
    
    public long getMaxid1() {
    	return startid1;
    }
    
    public boolean getSingleAssoc() {
    	return singleAssoc;
    }
    
    public Random getRng() {
    	return rng;
    }
    
    public ID2Chooser getID2Chooser() {
    	return id2chooser;
    }
    
    public byte[] getLinkAddData() {
    	return linkAddDataGen.fill(rng, new byte[(int)linkDataSize.choose(rng)]);
    }
    
    public byte[] getLinkUpData() {
    	return linkUpDataGen.fill(rng, new byte[(int)linkDataSize.choose(rng)]);
    }
    
    public byte[] getNodeAddData() {
    	return nodeAddDataGen.fill(rng, new byte[(int)nodeDataSize.choose(rng)]);
    }
    
    public byte[] getNodeUpData() {
    	return nodeUpDataGen.fill(rng, new byte[(int)nodeDataSize.choose(rng)]);
    }
}
