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

package com.oltpbenchmark.api.collectors;

import org.apache.log4j.Logger;

import com.oltpbenchmark.util.JSONUtil;

import java.util.Map;
import java.util.TreeMap;

public class DBCollector implements DBParameterCollector {

    private static final Logger LOG = Logger.getLogger(DBCollector.class);

    protected final Map<String, String> dbParameters = new TreeMap<String, String>();

    protected final Map<String, String> dbMetrics = new TreeMap<String, String>();

    protected final StringBuilder version = new StringBuilder();

    @Override
    public boolean hasParameters() {
        return (dbParameters.isEmpty() == false);
    }

    @Override
    public boolean hasMetrics() {
    	return (dbMetrics.isEmpty() == false);
    }

    @Override
    public String collectParameters() {
    	return JSONUtil.format(JSONUtil.toJSONString(dbParameters));
    }

    @Override
    public String collectMetrics() {
    	return JSONUtil.format(JSONUtil.toJSONString(dbMetrics));
    }

    @Override
    public String collectVersion() {
        return version.toString();
    }
}
