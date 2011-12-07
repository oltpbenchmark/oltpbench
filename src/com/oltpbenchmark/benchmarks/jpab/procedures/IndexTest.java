/*
 * JPA Performance Benchmark - http://www.jpab.org
 * Copyright ObjectDB Software Ltd. All Rights Reserved. 
 *
 * This is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published
 * by the Free Software Foundation; either version 2 of the License,
 * or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA.
 */

package com.oltpbenchmark.benchmarks.jpab.procedures;

import javax.persistence.EntityManager;

import com.oltpbenchmark.benchmarks.jpab.Test;
import com.oltpbenchmark.benchmarks.jpab.beans.IndexedPerson;
import com.oltpbenchmark.benchmarks.jpab.beans.TestEntity;


/**
 * Tests using simple Person entity objects with an index.
 */
public class IndexTest extends Test {
    
    /**
     * Gets the type of the benchmark main entity class.
     * 
     * @return the type of the benchmark main entity class.
     */
    @Override
    protected Class getEntityClass() {
        return IndexedPerson.class;
    }

	/**
	 * Creates a new entity object for storing in the database.
	 * 
	 * @return the new constructed entity object.
	 */
    @Override
    protected TestEntity newEntity() {
        return new IndexedPerson(this);
    }

	@Override
	public String getEntityName() {
		// TODO Auto-generated method stub
		return "IndexedPerson";
	}
	
	public void run(EntityManager em) {	
		this.persist(em);
		this.doAction(em, Test.ActionType.RETRIEVE);
		if (this.hasQueries()) {
			this.query(em);
		}
		this.doAction(em, Test.ActionType.UPDATE);
		this.doAction(em, Test.ActionType.DELETE);
	}
}
