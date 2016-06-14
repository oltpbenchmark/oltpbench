package com.oltpbenchmark.benchmarks.smallworldbank.procedures;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.smallworldbank.SWBankConstants;

public class WriteCheck extends Procedure {
    public final SQLStmt decChkBalanceSQL = new SQLStmt("UPDATE " 
            + SWBankConstants.TABLENAME_CHECKING 
            + " SET CHK_BALANCE = CHK_BALANCE - ?"
            + " WHERE CHK_A_ID = ?");
    
    
}
