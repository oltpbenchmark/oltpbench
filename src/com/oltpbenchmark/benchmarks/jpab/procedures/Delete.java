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

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.benchmarks.jpab.objects.Person;
import com.oltpbenchmark.benchmarks.jpab.objects.TestEntity;
import com.oltpbenchmark.benchmarks.jpab.tests.Test;

/**
 * Tests using simple Person entity objects.  
 */
public class Delete extends Procedure {
    
	public void run(EntityManager em, Test test) {	
		test.doAction(em, Test.ActionType.DELETE);
	}
	
}
