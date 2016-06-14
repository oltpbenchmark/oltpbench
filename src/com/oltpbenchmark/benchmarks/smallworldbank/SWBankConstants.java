/******************************************************************************
 *  Copyright 2016 by OLTPBenchmark Project  
 *  
 *  Author: Thamir Qadah                                 *
 *                                                                            *
 *  Licensed under the Apache License, Version 2.0 (the "License");           *
 *  you may not use this file except in compliance with the License.          *
 *  You may obtain a copy of the License at                                   *
 *                                                                            *
 *    http://www.apache.org/licenses/LICENSE-2.0                              *
 *                                                                            *
 *  Unless required by applicable law or agreed to in writing, software       *
 *  distributed under the License is distributed on an "AS IS" BASIS,         *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  *
 *  See the License for the specific language governing permissions and       *
 *  limitations under the License.                                            *
 ******************************************************************************/

package com.oltpbenchmark.benchmarks.smallworldbank;

public class SWBankConstants {
    /**
     * Table Names
     */
    public static final String TABLENAME_COUNTRY = "COUNTRY";
    public static final String TABLENAME_BRANCH = "BRANCH";
    public static final String TABLENAME_CUSTOMER = "CUSTOMER";
    public static final String TABLENAME_ACCOUNT = "ACCOUNT";
    public static final String TABLENAME_CHECKING = "CHECKING";
    public static final String TABLENAME_SAVING = "SAVING";
    public static final String COUNTRIES_DATAFILE = 
            "/com/oltpbenchmark/benchmarks/smallworldbank/data/countries_data.csv";
    
    
    public static final int BRANCH_PER_COUNTRY = 1;
    public static final int CUSTOMER_PER_BRANCH = 1000;
    public static final int ACCOUNT_PER_CUSTOMER = 1;
    
    
    public static final float INITAL_BALANCE = 10000f;
}
