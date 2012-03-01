package com.oltpbenchmark.util;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashSet;
import java.util.Set;

import oracle.sql.TIMESTAMP;

import org.apache.log4j.Logger;

import com.oltpbenchmark.catalog.Column;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.types.DatabaseType;

public abstract class SQLUtil {
    private static final Logger LOG = Logger.getLogger(SQLUtil.class);
    
    private static final DateFormat timestamp_formats[] = new DateFormat[] {
        new SimpleDateFormat("yyyy-MM-dd"),
        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"),
        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS"),
    };

    /**
     * Return a Integer from the given object
     * Handles the different cases from the various DBMSs
     * @param obj
     * @return
     */
    public static Integer getInteger(Object obj) {
        if (obj == null) return (null);
        
        if (obj instanceof Integer || obj.getClass().equals(int.class)) {
            return (Integer)obj;
        }
        else if (obj instanceof BigDecimal) {
             return ((BigDecimal)obj).intValue();
        }
        return (null);
    }
    
    /**
     * Return a long from the given object
     * Handles the different cases from the various DBMSs
     * @param obj
     * @return
     */
    public static Long getLong(Object obj) {
        if (obj == null) return (null);
        
        if (obj instanceof Long) {
            return (Long)obj;
        }
        else if (obj.getClass().equals(long.class)) {
            return (Long)obj;
        }
        else if (obj instanceof BigDecimal) {
             return ((BigDecimal)obj).longValue();
        }
        return (null);
    }
    
    /**
     * Return a double from the given object
     * Handles the different cases from the various DBMSs
     * @param obj
     * @return
     */
    public static Double getDouble(Object obj) {
        if (obj == null) return (null);
        
        if (obj instanceof Double || obj.getClass().equals(double.class)) {
            return (Double)obj;
        }
        else if (obj instanceof Float || obj.getClass().equals(float.class)) {
            return ((Float)obj).doubleValue();
        }
        else if (obj instanceof BigDecimal) {
             return ((BigDecimal)obj).doubleValue();
        }
        return (null);
    }
    
    /**
     * Return a double from the given object
     * Handles the different cases from the various DBMSs
     * @param obj
     * @return
     */
    public static Timestamp getTimestamp(Object obj) {
        if (obj == null) return (null);
        
        if (obj instanceof Timestamp) {
            return (Timestamp)obj;
        }
        else if (obj instanceof Date) {
            return new Timestamp(((Date)obj).getTime());
        }
        else if (obj instanceof oracle.sql.TIMESTAMP) {
            try {
                return ((oracle.sql.TIMESTAMP)obj).timestampValue();
            } catch (SQLException ex) {
                throw new RuntimeException("Failed to get timestamp from '" + obj + "'", ex);
            }
        }
        
        Long timestamp = SQLUtil.getLong(obj);
        return (timestamp != null ? new Timestamp(timestamp) : null);
    }
    
    /**
     * Return the internal sequence name for the given Column
     * @param dbType
     * @param catalog_col
     * @return
     */
    public static String getSequenceName(DatabaseType dbType, Column catalog_col) {
        Table catalog_tbl = catalog_col.getTable();
        assert(catalog_tbl != null);
        
        switch (dbType) {
            case POSTGRES:
                return String.format("pg_get_serial_sequence('%s', '%s')",
                                     catalog_tbl.getName(), catalog_col.getName());
        } // SWITCH
        return (null);
    }
    
    /**
     * Returns true if the given exception is because of a duplicate key error
     * @param ex
     * @return
     */
    public static boolean isDuplicateKeyException(Exception ex) {
        // MYSQL
        if (ex instanceof SQLIntegrityConstraintViolationException) {
            return (true);
        } else if (ex instanceof SQLException) {
            SQLException sqlEx = (SQLException)ex;
            
            // POSTGRES
            if (sqlEx.getSQLState().contains("23505")) {
                return (true);
            }
        }
        return (false);
    }
    
    /**
     * Simple pretty-print debug method for the current row
     * in the given ResultSet 
     * @param rs
     * @return
     * @throws SQLException
     */
    public static String debug(ResultSet rs) throws SQLException {
        ResultSetMetaData rs_md = rs.getMetaData();
        int num_cols = rs_md.getColumnCount();
        Object data[] = new Object[num_cols];
        for (int i = 0; i < num_cols; i++) {
            data[i] = rs.getObject(i+1);
        } // FOR
        
        return (String.format("ROW[%02d] -> [%s]", rs.getRow(), StringUtil.join(",", data)));
    }
    
    /**
     * For the given string representation of a value, convert it to the proper
     * object based on its sqlType 
     * @param sqlType
     * @param value
     * @return
     * @see java.sql.Types
     */
    public static Object castValue(int sqlType, String value) {
        Object ret = null;
        switch (sqlType) {
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.NCHAR:
            case Types.NVARCHAR: {
                ret = value;
                break;
            }
            case Types.TINYINT:
            case Types.SMALLINT: {
                ret = Short.parseShort(value);
                break;
            }
            case Types.INTEGER: {
                ret = Integer.parseInt(value);
                break;
            }
            case Types.BIGINT: {
                ret = Long.parseLong(value);
                break;
            }
            case Types.BOOLEAN: {
                ret = Boolean.parseBoolean(value);
                break;
            }
            case Types.DECIMAL:
            case Types.DOUBLE: {
                ret = Double.parseDouble(value);
                break;
            }
            case Types.FLOAT: {
                ret = Float.parseFloat(value);
                break;
            }
            case Types.DATE:
            case Types.TIME:
            case Types.TIMESTAMP: {
                for (DateFormat f : timestamp_formats) {
                    try {
                        ret = f.parse(value);
                    } catch (ParseException ex) {
                        // Ignore...
                    }
                    if (ret != null) break;
                } // FOR
                if (ret == null) throw new RuntimeException("Failed to parse timestamp '" + value + "'");
                break;
            }
            default:
                LOG.warn("Unexpected SQL Type '" + sqlType + "' for value '" + value + "'");
        } // SWITCH
        return (ret);
    }

    /**
     * Returns true if the given sqlType identifier is a String data type
     * @param sqlType
     * @return
     * @see java.sql.Types
     */
    public static boolean isStringType(int sqlType) {
        switch (sqlType) {
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.NCHAR:
            case Types.NVARCHAR: {
                return (true);
            }
            default:
                return (false);
        }
    }

    /**
     * Returns true if the given sqlType identifier is an Integer data type
     * @param sqlType
     * @return
     * @see java.sql.Types
     */
    public static boolean isIntegerType(int sqlType) {
        switch (sqlType) {
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
            case Types.BIGINT: {
                return (true);
            }
            default:
                return (false);
        }
    }
    
    /**
     * Returns true if the given sqlType identifier should always
     * be included in the DML with its corresponding column size
     * @param sqlType
     * @return
     * @see java.sql.Types
     */
    public static boolean needsColumnSize(int sqlType) {
        return isStringType(sqlType);
    }
    
    /**
     * Return the COUNT(*) SQL to calculate the number of records
     * @param table
     * @return SQL for select count execution
     */
    public static String getCountSQL(Table catalog_tbl) {
        return SQLUtil.getCountSQL(catalog_tbl, "*");
    }

    /**
     * Return the COUNT() SQL to calculate the number of records.
     * Will use the col parameter as the column that is counted
     * @param table
     * @param col
     * @return SQL for select count execution
     */
    public static String getCountSQL(Table catalog_tbl, String col) {
        return String.format("SELECT COUNT(%s) FROM %s",
                             col, catalog_tbl.getEscapedName());
    }


    /**
     * Automatically generate the 'INSERT' SQL string to insert
     * one record into this table
     * @return
     */
    public static String getInsertSQL(Table catalog_tbl) {
        return getInsertSQL(catalog_tbl, 1);
    }
    
    /**
     * Automatically generate the 'INSERT' SQL string for this table
     * The batchSize parameter specifies the number of sets of parameters
     * that should be included in the insert 
     * @param batchSize
     * @return
     */
    public static String getInsertSQL(Table catalog_tbl, int batchSize, int...exclude_columns) {
        return getInsertSQL(catalog_tbl, false, true, batchSize, exclude_columns);
    }
    
    public static String getInsertSQL(Table catalog_tbl, boolean show_cols, int batchSize, int...exclude_columns) {
        return getInsertSQL(catalog_tbl, false, true, batchSize, exclude_columns);
    }
    
    public static String getInsertSQL(Table catalog_tbl, boolean show_cols, boolean escape_names, int batchSize, int...exclude_columns) {
    	StringBuilder sb = new StringBuilder();
    	sb.append("INSERT INTO ")
    	  .append(escape_names ? catalog_tbl.getEscapedName() : catalog_tbl.getName());
    	
    	StringBuilder values = new StringBuilder();
    	boolean first;
    	
    	// Column Names
    	// XXX: Disabled because of case issues with HSQLDB
    	if (show_cols) sb.append(" (");
    	first = true;
    	
    	// These are the column offset that we want to exclude
    	Set<Integer> excluded = new HashSet<Integer>();
    	for (int ex : exclude_columns)
    	    excluded.add(ex);
    	
    	for (Column catalog_col : catalog_tbl.getColumns()) {
    	    if (excluded.contains(catalog_col.getIndex())) continue;
    		if (first == false) {
    			if (show_cols) sb.append(", ");
    			values.append(", ");
    		}
    		if (show_cols) sb.append(escape_names ? catalog_col.getEscapedName() : catalog_col.getName());
    		values.append("?");
    		first = false;
    	} // FOR
    	if (show_cols) sb.append(")");
    	
    	// Values
    	sb.append(" VALUES ");
    	first = true;
    	for (int i = 0; i < batchSize; i++) {
    		if (first == false) sb.append(", ");
    		sb.append("(").append(values.toString()).append(")");
    	} // FOR
//    	sb.append(";");
    	
    	return (sb.toString());
    }

    public static String getMaxColSQL(Table catalog_tbl, String col) {
        return String.format("SELECT MAX(%s) FROM %s",
                col, catalog_tbl.getEscapedName());
    }

    public static String selectColValues(Table catalog_tbl, String col) {
        return String.format("SELECT %s FROM %s",
                col, catalog_tbl.getEscapedName());
    }
}