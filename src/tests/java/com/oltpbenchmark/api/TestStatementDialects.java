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

package com.oltpbenchmark.api;

import java.io.File;
import java.util.Collection;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.benchmarks.epinions.EpinionsBenchmark;
import com.oltpbenchmark.benchmarks.epinions.TestEpinionsBenchmark;
import com.oltpbenchmark.benchmarks.epinions.procedures.GetItemAverageRating;
import com.oltpbenchmark.types.DatabaseType;
import com.oltpbenchmark.util.ClassUtil;
import com.oltpbenchmark.util.FileUtil;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.convert.LegacyListDelimiterHandler;
import org.apache.commons.configuration2.tree.xpath.XPathExpressionEngine;

public class TestStatementDialects extends AbstractTestCase<EpinionsBenchmark> {
    
    private File xmlFile;
    private XMLConfiguration xmlConfig;
    
    private static final DatabaseType TARGET_DATABASE = DatabaseType.SQLITE;
    private static final Class<? extends Procedure> TARGET_PROCEDURE = GetItemAverageRating.class;
    private static final String TARGET_STMT = "getAverageRating";
    private static final String TARGET_STMT_SQL = "SELECT * FROM review";
    
    private static final String dialectXML = 
            "<dialects>\n" +
            "<dialect type=\"" + TARGET_DATABASE.name() + "\">\n" +
            "<procedure name=\"" + TARGET_PROCEDURE.getSimpleName() + "\">\n" +
            "<statement name=\""+ TARGET_STMT + "\">" + TARGET_STMT_SQL + "</statement>\n" +
            "</procedure>\n" +
            "</dialect>\n" +
            "</dialects>";
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(EpinionsBenchmark.class, TestEpinionsBenchmark.PROC_CLASSES);
        this.xmlFile = FileUtil.writeStringToTempFile(dialectXML, "xml");
        Parameters params = new Parameters();
        FileBasedConfigurationBuilder<XMLConfiguration> builder = new FileBasedConfigurationBuilder<>(XMLConfiguration.class)
                .configure(params.xml()
                        .setFile(xmlFile)
                        .setListDelimiterHandler(new LegacyListDelimiterHandler(','))
                        .setExpressionEngine(new XPathExpressionEngine()));
        this.xmlConfig = builder.getConfiguration();
    }
    
    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        
        if (this.xmlFile.exists()) {
            this.xmlFile.delete();
        }
    }
    
    /**
     * testDumpXMLFile
     */
    public void testDumpXMLFile() throws Exception {
        DatabaseType dbType = DatabaseType.POSTGRES;
        WorkloadConfiguration tmpWorkConfig = new WorkloadConfiguration();
        tmpWorkConfig.setDatabaseType(dbType);
        tmpWorkConfig.setXmlConfig(xmlConfig);
        StatementDialects dialects = new StatementDialects(tmpWorkConfig);
        
        String dump = dialects.export(dbType, this.benchmark.getProcedures().values());
        assertNotNull(dump);
        assertFalse(dump.isEmpty());
        
        System.err.println(dump);
        
        // Make sure that the dump has all our procedures and statements that
        // we expect to be there
        for (Procedure proc : this.benchmark.getProcedures().values()) {
            assertTrue(proc.getProcedureName(), dump.contains(proc.getProcedureName()));
            for (String stmtName : proc.getStatements().keySet()) {
                assertTrue(proc.getProcedureName() + "." + stmtName, dump.contains(stmtName));
            }
        }
    }
    
    /**
     * testLoadXMLFile
     */
    public void testLoadXMLFile() throws Exception {
        for (DatabaseType dbType : DatabaseType.values()) {
            this.workConf.setDatabaseType(dbType);
            File xmlFile = new File(this.benchmark.getStatementDialects().getSQLDialectPath(dbType));
            if (xmlFile == null) continue;

            this.workConf.setXmlConfig(xmlConfig);
            StatementDialects dialects = new StatementDialects(workConf);

            boolean ret = dialects.load();
            if (ret == false) continue;
            
            Collection<String> procNames = dialects.getProcedureNames();
            assertNotNull(dbType.toString(), procNames);
            assertFalse(dbType.toString(), procNames.isEmpty());
            
            for (String procName : procNames) {
                assertFalse(procName.isEmpty());
                Collection<String> stmtNames = dialects.getStatementNames(procName);
                assertNotNull(procName, stmtNames);
                assertFalse(procName, stmtNames.isEmpty());
                
                for (String stmtName : stmtNames) {
                    assertFalse(stmtName.isEmpty());
                    String stmtSQL = dialects.getSQL(procName, stmtName);
                    assertNotNull(stmtSQL);
                    assertFalse(stmtSQL.isEmpty());
                } // FOR (stmt)
            } // FOR (proc)
        } // FOR (dbtype)
    }
    
    /**
     * testSetDialect
     */
    public void testSetDialect() throws Exception {
        // Load in our fabricated dialects
        WorkloadConfiguration tmpWorkConfig = new WorkloadConfiguration();
        tmpWorkConfig.setDatabaseType(TARGET_DATABASE);
        tmpWorkConfig.setXmlConfig(xmlConfig);
        StatementDialects dialects = new StatementDialects(tmpWorkConfig);
        boolean ret = dialects.load();
        assertTrue(ret);
        Procedure proc = ClassUtil.newInstance(TARGET_PROCEDURE, new Object[0], new Class<?>[0]);
        assertNotNull(proc);
        proc.initialize(DatabaseType.POSTGRES);
        proc.loadSQLDialect(dialects);
        
        // And then check to see that our target SQLStmt got its SQL changed
        SQLStmt stmt = proc.getStatment(TARGET_STMT);
        assertNotNull(stmt);
        assertEquals(TARGET_STMT_SQL, stmt.getSQL());
    }
}
