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

package com.oltpbenchmark.util.dbms_collectors;

import com.oltpbenchmark.catalog.Catalog;
import org.apache.log4j.Logger;

import java.util.Map;
import java.util.TreeMap;

class DBCollector implements DBParameterCollector {
    private static final Logger LOG = Logger.getLogger(DBCollector.class);
    protected final Map<String, String> dbConf = new TreeMap<String, String>();
    protected final Map<String, String> dbStatus = new TreeMap<String, String>();

    @Override
    public String collectConfigParameters() {
        return collectMap(dbConf);
    }
    
    @Override
    public String collectStatusParameters() {
        return collectMap(dbStatus);
    }
    
    @Override
    public String collectVersion() {
        return "";
    }
    
    private static String collectMap(Map<String,String> dbMap) {
    	StringBuilder builder = new StringBuilder();
        for (Map.Entry<String, String> kv : dbMap.entrySet()) {
            builder.append(kv.getKey().toLowerCase())
                       .append("=")
                       .append(kv.getValue().toLowerCase())
                       .append("\n");
        }
        return builder.toString();
    }
}
