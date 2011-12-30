/*******************************************************************************
 * oltpbenchmark.com
 *  
 *  Project Info:  http://oltpbenchmark.com
 *  Project Members:    Carlo Curino <carlo.curino@gmail.com>
 *              Evan Jones <ej@evanjones.ca>
 *              DIFALLAH Djellel Eddine <djelleleddine.difallah@unifr.ch>
 *              Andy Pavlo <pavlo@cs.brown.edu>
 *              CUDRE-MAUROUX Philippe <philippe.cudre-mauroux@unifr.ch>  
 *                  Yang Zhang <yaaang@gmail.com> 
 * 
 *  This library is free software; you can redistribute it and/or modify it under the terms
 *  of the GNU General Public License as published by the Free Software Foundation;
 *  either version 3.0 of the License, or (at your option) any later version.
 * 
 *  This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *  See the GNU Lesser General Public License for more details.
 ******************************************************************************/
package com.oltpbenchmark.catalog;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;


/**
 * Table Catalog Object
 * @author Carlo A. Curino (carlo@curino.us)
 * @author pavlo
 * @author Djellel
 */
public class Table extends AbstractCatalogObject {
	private static final long serialVersionUID = 1L;
	
	private final List<Column> columns = new ArrayList<Column>();
    private final List<IntegrityConstraint> constraints = new ArrayList<IntegrityConstraint>();
    private final List<String> primaryKeys = new ArrayList<String>();
    private final List<Index> indexes = new ArrayList<Index>();
    
    
    public Table(String tableName) {
    	super(tableName);
    }
    
    public Table(Table srcTable) {
        this(srcTable.getName());

        for (int i = 0, cnt = srcTable.columns.size(); i < cnt; i++) {
            Column col = (Column)srcTable.columns.get(i).clone();
            this.columns.add(col);
        } // FOR
        for (IntegrityConstraint ic : srcTable.constraints) {
            this.constraints.add(ic.clone());
        } // FOR
    }

    @Override
    public Table clone() {
        return new Table(this);
    }
    
    // ----------------------------------------------------------
    // COLUMNS
    // ----------------------------------------------------------
    
    public void addColumn(Column col) {
        assert(this.columns.contains(col) == false) : "Duplicate column '" + col + "'";
        this.columns.add(col);
    }
    
    public int getColumnCount() {
        return this.columns.size();
    }
    public List<Column> getColumns() {
        return Collections.unmodifiableList(this.columns);
    }
    public Column getColumn(int index) {
        return this.columns.get(index);
    }
    public String getColumnName(int index) {
        return this.columns.get(index).getName();
    }
    public int getColumnIndex(String columnName) {
        for (int i = 0, cnt = getColumnCount(); i < cnt; i++) {
            if (this.columns.get(i).getName().equalsIgnoreCase(columnName)) {
                return (i);
            }
        } // FOR
        return -1;
    }

    public Column getColumnByName(String colname) {
        int idx = getColumnIndex(colname);
        return (idx >= 0 ? this.columns.get(idx) : null);
    }

    // ----------------------------------------------------------
    // INDEXES
    // ----------------------------------------------------------
    
    /**
     * Add a new Index for this table
     * @param index
     */
    public void addIndex(Index index) {
        assert(this.indexes.contains(index) == false) : "Duplicate index '" + index + "'";
        this.indexes.add(index);
    }
    
    public Collection<Index> getIndexes() {
        return (this.indexes);
    }
    
    /**
     * Return the number of indexes for this table
     * @return
     */
    public int getIndexCount() {
        return this.indexes.size();
    }
    
    /**
     * Return a particular index based on its name
     * @param indexName
     * @return
     */
    public Index getIndex(String indexName) {
        for (Index catalog_idx : this.indexes) {
            if (catalog_idx.getName().equalsIgnoreCase(indexName)) {
                return (catalog_idx); 
            }
        } // FOR
        return (null);
    }
    
    // ----------------------------------------------------------
    // PRIMARY KEY INDEX
    // ----------------------------------------------------------
    
    
    public void setPrimaryKeyColumns(Collection<String> colNames) {
        this.primaryKeys.clear();
        this.primaryKeys.addAll(colNames);
    }
    
    /**
     * Get the list of column names that are the primary keys for this table
     * @return
     */
    public List<String> getPrimaryKeyColumns() {
        return Collections.unmodifiableList(this.primaryKeys);
    }

    public void addConstraint(List<IntegrityConstraint> iclist) throws IntegrityConstraintsExistsException {
        for (IntegrityConstraint ic : iclist) {
            addConstraint(ic);
        }
    }

    public void addConstraint(IntegrityConstraint ic) throws IntegrityConstraintsExistsException {
        for (IntegrityConstraint c : constraints) {
            if (c != null && c.getId().equals(ic.getId()))
                throw new IntegrityConstraintsExistsException("A constraint " + ic.getId() + " already exists in this table");
        }
        constraints.add(ic);
    }

    @Override
    public boolean equals(Object object) {
        if ((object instanceof Table) == false) return (false);

        Table table2 = (Table)object;
        return (this.name.equals(table2.name) &&
                this.columns.equals(table2.columns) &&
                this.constraints.equals(table2.constraints));
    }
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        
        sb.append(getName()).append(" (\n");
        for (int i = 0, cnt = this.columns.size(); i < cnt; i++) {
            sb.append("  ").append(this.columns.get(i)).append("\n");
        } // FOR
        sb.append(")");

        return (sb.toString());
    }
}
