package com.oltpbenchmark.api.collectors;

import com.oltpbenchmark.Database;

public class NoopCollector extends DBCollector {

    public NoopCollector(Database database) {
        super(database);
    }

    @Override
    public String collectParameters() {
        return EMPTY_JSON;
    }

    @Override
    public String collectMetrics() {
        return EMPTY_JSON;
    }

}
