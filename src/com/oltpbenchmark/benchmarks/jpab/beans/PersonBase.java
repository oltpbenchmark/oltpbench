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

package com.oltpbenchmark.benchmarks.jpab.beans;

import javax.persistence.*;

import com.oltpbenchmark.api.LoaderUtil;
import com.oltpbenchmark.benchmarks.jpab.Test;

/**
 * A simple based entity class.
 */
@Entity
@DiscriminatorColumn
@TableGenerator(name="extSeq", allocationSize=1000)
public class PersonBase {
	
	// Fields:

	@Id @GeneratedValue(strategy=GenerationType.TABLE, generator="extSeq")
    private Integer id;

	private String firstName;
	private String middleName;
	private String lastName;

	// Constructors:

    public PersonBase() {
    	// used by JPA to load an entity object from the database
    }

    public PersonBase(Test test) {
    	firstName = LoaderUtil.randomStr(10);
    	middleName = LoaderUtil.randomStr(10);
    	lastName = LoaderUtil.randomStr(10);
    }

	// Methods:

    public void load() {
		assert firstName != null && middleName != null && lastName != null;
    }

    @Override
    public String toString() {
    	StringBuilder sb = new StringBuilder(64);
    	sb.append(firstName);
    	if (middleName != null) {
        	sb.append(' ').append(middleName);
    	}
    	sb.append(' ').append(lastName);
        return sb.toString();
    }
}
