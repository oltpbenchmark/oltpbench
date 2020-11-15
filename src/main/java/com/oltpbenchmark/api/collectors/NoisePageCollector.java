package com.oltpbenchmark.api.collectors;

import java.sql.*;

public class NoisePageCollector extends DBCollector{
    private static final String VERSION_SQL = "SELECT version();";

    private static final String PARAMETERS_SQL = "SHOW ALL;";

    public NoisePageCollector(String oriDBUrl, String username, String password) {
        try (Connection conn = DriverManager.getConnection(oriDBUrl, username, password)) {
            try (Statement s = conn.createStatement()) {

                // Collect DBMS version
                try (ResultSet out = s.executeQuery(VERSION_SQL)) {
                    if (out.next()) {
                        this.version = out.getString(1);
                    }
                }

                // Collect DBMS parameters
                try (ResultSet out = s.executeQuery(PARAMETERS_SQL)) {
                    while (out.next()) {
                        dbParameters.put(out.getString("variable"), out.getString("value"));
                    }
                }

            }
        } catch (SQLException e) {
            LOG.error("Error while collecting DB parameters: {}", e.getMessage());
        }
    }
}
