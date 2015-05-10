package com.oltpbenchmark.benchpress;

public interface ServerCallback {
    void ready();
    void updateActualThroughput(int actualThroughput);
    void updateTargetThroughput(int targetThroughput);
    int getTargetThroughput();
}
