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

package com.oltpbenchmark.util;

import com.oltpbenchmark.DistributionStatistics;
import com.oltpbenchmark.Results;
import com.oltpbenchmark.util.dbms_collectors.DBParameterCollector;
import com.oltpbenchmark.util.dbms_collectors.DBParameterCollectorGen;
import com.oltpbenchmark.util.json.JSONException;
import com.oltpbenchmark.util.json.JSONStringer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.FileBody;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.Date;
import java.util.TimeZone;
import java.util.zip.GZIPOutputStream;

public class ResultUploader {
    private static final Logger LOG = Logger.getLogger(ResultUploader.class);

    private static String[] IGNORE_CONF = {
            "dbtype",
            "driver",
            "DBUrl",
            "username",
            "password",
            "uploadCode",
            "uploadUrl"
    };

    private static String[] BENCHMARK_KEY_FIELD = {
            "isolation",
            "scalefactor",
            "terminals"
    };

    XMLConfiguration expConf;
    Results results;
    CommandLine argsLine;
    DBParameterCollector collector;

    String dbUrl, dbType;
    String username, password;
    String benchType;
    int windowSize;
    String uploadCode, uploadUrl;

    public ResultUploader(Results r, XMLConfiguration conf, CommandLine argsLine) {
        this.expConf = conf;
        this.results = r;
        this.argsLine = argsLine;

        dbUrl = expConf.getString("DBUrl");
        dbType = expConf.getString("dbtype");
        username = expConf.getString("username");
        password = expConf.getString("password");
        benchType = argsLine.getOptionValue("b");
        uploadCode = expConf.getString("uploadCode");
        uploadUrl = expConf.getString("uploadUrl");
        
        try {
            windowSize = Integer.parseInt(argsLine.getOptionValue("s"));
        } catch (NumberFormatException e) {
            windowSize = 5;
        }

        this.collector = DBParameterCollectorGen.getCollector(dbType, dbUrl, username, password);
    }

    public void writeDBParameters(PrintStream os) {
        String dbConf = collector.collectConfigParameters();
        os.print(dbConf);
    }
    
    public void writeDBStatus(PrintStream os) {
        String dbConf = collector.collectStats();
        os.print(dbConf);
    }
    
    public void writeDBTables(PrintStream os) {
        //String dbConf = collector.collectTableParameters();
        os.print("");
    }

    public void writeBenchmarkConf(PrintStream os) throws ConfigurationException {
        XMLConfiguration outputConf = (XMLConfiguration) expConf.clone();
        for (String key: IGNORE_CONF) {
            outputConf.clearProperty(key);
        }
        outputConf.save(os);
    }

    public void writeSummary(PrintStream os) {
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        Date now = new Date();
        JSONStringer stringer = new JSONStringer();
        DistributionStatistics ld = results.latencyDistribution;
        final double SECONDS_FACTOR = 1e6;
        try {
            stringer.object()
                    .key("timestamp_utc_sec")
                    .value(now.getTime() / 1000L)
                    .key("dbms")
                    .value(dbType)
                    .key("dbms_version")
                    .value(collector.collectVersion())
                    .key("dbms_info")
                    .value("TODO")
                    .key("benchmark")
                    .value(benchType);
            for (String field: BENCHMARK_KEY_FIELD) {
                stringer.key(field)
                        .value(expConf.getString(field));
            }
            stringer.key("latency_sec")
                        .object()
                        .key("25th_percentile")
                        .value(ld.get25thPercentile() / SECONDS_FACTOR)
                        .key("75th_percentile")
                        .value(ld.get75thPercentile() / SECONDS_FACTOR)
                        .key("90th_percentile")
                        .value(ld.get90thPercentile() / SECONDS_FACTOR)
                        .key("95th_percentile")
                        .value(ld.get95thPercentile() / SECONDS_FACTOR)
                        .key("99th_percentile")
                        .value(ld.get99thPercentile() / SECONDS_FACTOR)
                        .key("average")
                        .value(ld.getAverage() / SECONDS_FACTOR)
                        .key("max")
                        .value(ld.getMaximum() / SECONDS_FACTOR)
                        .key("median")
                        .value(ld.getMedian() / SECONDS_FACTOR)
                        .key("min")
                        .value(ld.getMinimum() / SECONDS_FACTOR)
                        .key("standard_deviation")
                        .value(ld.getStandardDeviation() / SECONDS_FACTOR)
                        .endObject()
                    .endObject();
        } catch(JSONException e) {
            e.printStackTrace();
        }
        os.println(JSONUtil.format(stringer.toString()));
    }

    public void uploadResult(boolean includeRawData) throws ParseException {
        try {
            File expConfFile = File.createTempFile("expConf", ".tmp");
            File sampleFile = File.createTempFile("sample", ".tmp");
            File summaryFile = File.createTempFile("summary", ".tmp");
            File dbConfFile = File.createTempFile("dbConf", ".tmp");
            File dbStatusFile = File.createTempFile("dbStatus", ".tmp");
            File dbTableFile = File.createTempFile("dbTbls", ".tmp");
            File rawDataFile = null;
            if (includeRawData) {
            	rawDataFile = File.createTempFile("raw", ".gz");
            }

            PrintStream confOut = new PrintStream(new FileOutputStream(expConfFile));
            writeBenchmarkConf(confOut);
            confOut.close();

            confOut = new PrintStream(new FileOutputStream(dbConfFile));
            writeDBParameters(confOut);
            confOut.close();

            confOut = new PrintStream(new FileOutputStream(sampleFile));
            results.writeCSV(windowSize, confOut);
            confOut.close();

            confOut = new PrintStream(new FileOutputStream(summaryFile));
            writeSummary(confOut);
            confOut.close();
            
            confOut = new PrintStream(new FileOutputStream(dbStatusFile));
            writeDBStatus(confOut);
            confOut.close();
            
            confOut = new PrintStream(new FileOutputStream(dbTableFile));
            writeDBTables(confOut);
            confOut.close();

            if (includeRawData) {
            	confOut = new PrintStream(new GZIPOutputStream(new FileOutputStream(rawDataFile)));
            	results.writeAllCSVAbsoluteTiming(confOut);
            	confOut.close();
            }

            CloseableHttpClient httpclient = HttpClients.createDefault();
            HttpPost httppost = new HttpPost(uploadUrl);
            
            MultipartEntityBuilder builder = MultipartEntityBuilder.create()
                    .addTextBody("upload_code", uploadCode)
                    .addPart("sample_data", new FileBody(sampleFile))
                    .addPart("db_conf_data", new FileBody(dbConfFile))
                    .addPart("db_status_data", new FileBody(dbStatusFile))
                    .addPart("db_table_data", new FileBody(dbTableFile))
                    .addPart("benchmark_conf_data", new FileBody(expConfFile))
                    .addPart("summary_data", new FileBody(summaryFile));
            
            if (includeRawData) {
            	builder.addPart("raw_data", new FileBody(rawDataFile));
            }

            HttpEntity reqEntity = builder.build();
            httppost.setEntity(reqEntity);

            LOG.info("executing request " + httppost.getRequestLine());
            CloseableHttpResponse response = httpclient.execute(httppost);
            try {
                HttpEntity resEntity = response.getEntity();
                LOG.info(IOUtils.toString(resEntity.getContent()));
                EntityUtils.consume(resEntity);
            } finally {
                response.close();
            }
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (ConfigurationException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }
}
