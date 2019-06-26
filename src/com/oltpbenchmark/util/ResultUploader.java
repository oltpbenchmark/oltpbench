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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.cli.ParseException;
import org.apache.commons.collections15.map.ListOrderedMap;
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

import com.oltpbenchmark.Results;
import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.collectors.DBCollector;

public class ResultUploader {
    private static final Logger LOG = Logger.getLogger(ResultUploader.class);

    private static final String[] IGNORE_CONF = {
            "dbtype",
            "driver",
            "DBUrl",
            "username",
            "password",
            "uploadCode",
            "uploadUrl"
    };

    private static final String[] FILE_EXTENSIONS = {
            "csv.gz",
            "expconfig",
            "metrics",
            "params",
            "samples",
            "summary"
    };

    private final String uploadUrl;
    private final String uploadCode;
    private final String uploadHash;

    public ResultUploader(String uploadUrl, String uploadCode, String uploadHash) {
        this.uploadUrl = uploadUrl;
        this.uploadCode = uploadCode;
        this.uploadHash = uploadHash;
    }

    public static void writeBenchmarkConf(XMLConfiguration xmlConf, PrintStream out) throws ConfigurationException {
        xmlConf = new XMLConfiguration(xmlConf);
        for (String key: IGNORE_CONF) {
            xmlConf.clearProperty(key);
        }
        xmlConf.save(out);
    }

    public static void writeSummary(WorkloadConfiguration workConf, Results results, PrintStream out) {
        Map<String, Object> summary = new ListOrderedMap<String, Object>();
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        Date now = new Date();
        summary.put("Current Timestamp (milliseconds)", now.getTime());
        summary.put("DBMS Type", workConf.getDBType().name().toLowerCase());
        summary.put("DBMS Version", workConf.getDB().getVersion());
        summary.put("Benchmark Type", workConf.getBenchmarkName());
        summary.put("Latency Distribution", results.latencyDistribution.toMap());
        summary.put("Requests", results.getRequests());
        summary.put("Runtime (seconds)", results.getRuntimeSeconds());
        summary.put("Throughput (requests/second)", results.getRequestsPerSecond());
        summary.put("isolation", workConf.getIsolationString());
        summary.put("scalefactor", workConf.getScaleFactor());
        summary.put("terminals", workConf.getTerminals());
        out.println(JSONUtil.format(JSONUtil.toJSONString(summary)));
    }

    public void uploadResult(List<TransactionType> activeTXTypes, Results results, WorkloadConfiguration workConf,
            Map<String, String> resultFilenames) throws ParseException {
        try {
            Map<String, File> uploaderFiles = new HashMap<String, File>();
            File file;
            PrintStream out;
            DBCollector collector = null;

            for (String ext : FILE_EXTENSIONS) {
                if (resultFilenames.containsKey(ext)) {
                    // Result file already exists
                    file = new File(resultFilenames.get(ext));
                    LOG.debug(String.format("%s: found existing result file", ext.toUpperCase()));
                } else {
                    // No result file exists - write result out to temporary file
                    LOG.debug(String.format("%s: writing result to temporary file", ext.toUpperCase()));
                    if (ext.equals("csv.gz")) {
                        file = File.createTempFile("csv", ".gz");
                        out = new PrintStream(new GZIPOutputStream(new FileOutputStream(file)));
                    } else {
                        file = File.createTempFile(ext, ".tmp");
                        out = new PrintStream(new FileOutputStream(file));
                    }

                    switch (ext) {
                    case "csv.gz": {
                        results.writeAllCSVAbsoluteTiming(activeTXTypes, out);
                        break;
                    }
                    case "expconfig": {
                        writeBenchmarkConf(workConf.getXmlConfig(), out);
                        break;
                    }
                    case "metrics": {
                        if (collector == null)
                            collector = DBCollector.createCollector(workConf.getDB());
                        collector.writeMetrics(out);
                        break;
                    }
                    case "params": {
                        if (collector == null)
                            collector = DBCollector.createCollector(workConf.getDB());
                        collector.writeParameters(out);
                        break;
                    }
                    case "samples": {
                        results.writeCSV2(out);
                        break;
                    }
                    case "summary": {
                        writeSummary(workConf, results, out);
                        break;
                    }
                    }
                    out.close();
                }
                uploaderFiles.put(ext, file);
            }

            CloseableHttpClient httpclient = HttpClients.createDefault();
            HttpPost httppost = new HttpPost(uploadUrl);

            HttpEntity reqEntity = MultipartEntityBuilder.create()
                    .addTextBody("upload_code", uploadCode)
                    .addTextBody("upload_hash", uploadHash)
                    .addPart("sample_data", new FileBody(uploaderFiles.get("samples")))
                    .addPart("raw_data", new FileBody(uploaderFiles.get("csv.gz")))
                    .addPart("db_parameters_data", new FileBody(uploaderFiles.get("params")))
                    .addPart("db_metrics_data", new FileBody(uploaderFiles.get("metrics")))
                    .addPart("benchmark_conf_data", new FileBody(uploaderFiles.get("expconfig")))
                    .addPart("summary_data", new FileBody(uploaderFiles.get("summary")))
                    .build();

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
