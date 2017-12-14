DROP TABLE IF EXISTS nation;
DROP TABLE IF EXISTS region;
DROP TABLE IF EXISTS part;
DROP TABLE IF EXISTS supplier;
DROP TABLE IF EXISTS partsupp;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS customer;
DROP TABLE IF EXISTS lineitem;

-- Sccsid:     @(#)dss.ddl  2.1.8.1
CREATE TABLE nation  ( n_nationkey  INTEGER NOT NULL,
                       n_name       CHAR(25) NOT NULL,
                       n_regionkey  INTEGER NOT NULL,
                       n_comment    VARCHAR(152));

CREATE TABLE region  ( r_regionkey  INTEGER NOT NULL,
                       r_name       CHAR(25) NOT NULL,
                       r_comment    VARCHAR(152));

CREATE TABLE part  ( p_partkey     INTEGER NOT NULL,
                     p_name        VARCHAR(55) NOT NULL,
                     p_mfgr        CHAR(25) NOT NULL,
                     p_brand       CHAR(10) NOT NULL,
                     p_type        VARCHAR(25) NOT NULL,
                     p_size        INTEGER NOT NULL,
                     p_container   CHAR(10) NOT NULL,
                     p_retailprice double NOT NULL,
                     p_comment     VARCHAR(23) NOT NULL );

CREATE TABLE supplier ( s_suppkey     INTEGER NOT NULL,
                        s_name        CHAR(25) NOT NULL,
                        s_address     VARCHAR(40) NOT NULL,
                        s_nationkey   INTEGER NOT NULL,
                        s_phone       CHAR(15) NOT NULL,
                        s_acctbal     double NOT NULL,
                        s_comment     VARCHAR(101) NOT NULL);

CREATE TABLE partsupp ( ps_partkey     INTEGER NOT NULL,
                        ps_suppkey     INTEGER NOT NULL,
                        ps_availqty    INTEGER NOT NULL,
                        ps_supplycost  double  NOT NULL,
                        ps_comment     VARCHAR(199) NOT NULL );

CREATE TABLE customer ( c_custkey     INTEGER NOT NULL,
                        c_name        VARCHAR(25) NOT NULL,
                        c_address     VARCHAR(40) NOT NULL,
                        c_nationkey   INTEGER NOT NULL,
                        c_phone       CHAR(15) NOT NULL,
                        c_acctbal     double   NOT NULL,
                        c_mktsegment  CHAR(10) NOT NULL,
                        c_comment     VARCHAR(117) NOT NULL);

CREATE TABLE orders  ( o_orderkey       INTEGER NOT NULL,
                       o_custkey        INTEGER NOT NULL,
                       o_orderstatus    CHAR(3) NOT NULL,
                       o_totalprice     double NOT NULL,
                       o_orderdate      datetime DEFAULT CURRENT_TIMESTAMP,
                       o_orderpriority  CHAR(15) NOT NULL,
                       o_clerk          CHAR(15) NOT NULL,
                       o_shippriority   INTEGER NOT NULL,
                       o_comment        VARCHAR(79) NOT NULL);

CREATE TABLE lineitem ( l_orderkey    INTEGER NOT NULL,
                        l_partkey     INTEGER NOT NULL,
                        l_suppkey     INTEGER NOT NULL,
                        l_linenumber  INTEGER NOT NULL,
                        l_quantity    double NOT NULL,
                        l_extendedprice  double NOT NULL,
                        l_discount    double NOT NULL,
                        l_tax         double NOT NULL,
                        l_returnflag  CHAR(3) NOT NULL,
                        l_linestatus  CHAR(3) NOT NULL,
                        l_shipdate    datetime DEFAULT CURRENT_TIMESTAMP,
                        l_commitdate  datetime DEFAULT CURRENT_TIMESTAMP,
                        l_receiptdate datetime DEFAULT CURRENT_TIMESTAMP,
                        l_shipinstruct CHAR(25) NOT NULL,
                        l_shipmode     CHAR(10) NOT NULL,
                        l_comment      VARCHAR(44) NOT NULL);

