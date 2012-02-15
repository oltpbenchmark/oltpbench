package com.oltpbenchmark.benchmarks.jpab;

import java.sql.Connection;
import java.sql.SQLException;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.apache.commons.lang.NotImplementedException;

import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.benchmarks.jpab.tests.BasicTest;
import com.oltpbenchmark.benchmarks.jpab.tests.Test;

public class JPABLoader extends Loader {

    String persistanceUnit;
    public JPABLoader(BenchmarkModule benchmark, Connection conn, String persistanceUnit) throws SQLException {
        super(benchmark, conn);
        this.persistanceUnit=persistanceUnit;
        this.load();
    }

    @Override
    public void load() throws SQLException {
        int objectCount= (int)this.workConf.getScaleFactor();
        EntityManagerFactory emf = Persistence.createEntityManagerFactory(persistanceUnit);
        EntityManager em = emf.createEntityManager();
        Test test=new BasicTest();
        test.setBatchSize(10);
        test.setEntityCount(objectCount);
        test.buildInventory(objectCount); 
        while (test.getActionCount() < objectCount) {
            test.persist(em);
        }
        test.clearInventory();
    }

}
