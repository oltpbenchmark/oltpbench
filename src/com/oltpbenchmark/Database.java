package com.oltpbenchmark;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.oltpbenchmark.types.DatabaseType;

public final class Database {
    private static final Logger LOG = Logger.getLogger(Database.class);

    private final DatabaseType type;

    private final String name;

    private final String driver;

    private final String url;

    private final String username;

    private final String password;

    private final String version;

    public Database(DatabaseType type, String name, String driver, String url, String username, String password) {
        this(type, name, driver, url, username, password, null);
    }

    public Database(DatabaseType type, String name, String driver, String url, String username, String password,
            String version) {
        this.type = type;
        this.name = name;
        this.driver = driver;
        this.username = username;
        this.password = password;

        // Test connection
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(url, username, password);;
        } catch (SQLException ex) {
            if (ex.getErrorCode() == 0 && ex.getSQLState().equals("01S00")) {
                String delim = (url.contains("?")) ? ";" : "?";
                url += delim + "serverTimezone=UTC";
            } else {
                throw new RuntimeException(ex);
            }
        }
        this.url = url;

        // Retry
        if (conn == null) {
            try {
                conn = this.getConnection();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        }
        assert (conn != null);

        if (version == null) {
            // Get database version
            try {
                DatabaseMetaData meta = conn.getMetaData();
                int majorVersion = meta.getDatabaseMajorVersion();
                int minorVersion = meta.getDatabaseMinorVersion();
                version = String.format("%s.%s", majorVersion, minorVersion);
            } catch (SQLException ex) {
                LOG.warn("Error extracting database version: " + ex.getMessage());
                version = "";
            }
        }
        this.version = version;

        try {
            conn.close();
        } catch (SQLException ex) {
        }
    }


    public DatabaseType getType() {
        return type;
    }

    public String getName() {
        return name;
    }

    public String getDriver() {
        return driver;
    }

    public String getUrl() {
        return url;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getVersion() {
        return version;
    }

    public Connection getConnection() throws SQLException {
        return DriverManager.getConnection(this.url, this.username, this.password);
    }
}
