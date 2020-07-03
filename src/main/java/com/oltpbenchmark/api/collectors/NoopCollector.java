package com.oltpbenchmark.api.collectors;

public class NoopCollector extends DBCollector {

    public NoopCollector(String dbUrl, String dbUsername, String dbPassword) {
    }

    @Override
    public String collectParameters() {
        return "{}";
    }

    @Override
    public String collectMetrics() {
        return "{}";
    }

}
