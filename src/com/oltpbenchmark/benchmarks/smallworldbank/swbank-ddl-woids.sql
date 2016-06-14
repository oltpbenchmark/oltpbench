-- Drop tables
DROP TABLE IF EXISTS SAVING;
DROP TABLE IF EXISTS CHECKING;
DROP TABLE IF EXISTS ACCOUNT;
DROP TABLE IF EXISTS CUSTOMER;
DROP TABLE IF EXISTS BRANCH;
DROP TABLE IF EXISTS COUNTRY;

-- countries list
CREATE TABLE COUNTRY
(
  c_id integer     NOT NULL
, c_name   varchar(50) NOT NULL
, c_dfreq smallint not null
, PRIMARY KEY
  (
    c_id
  )
) with oids; 

-- branch table
CREATE TABLE BRANCH
(
  b_id integer     NOT NULL
, b_c_id   integer NOT NULL REFERENCES COUNTRY (c_id)
, b_saddress varchar(200) NOT NULL
, b_zipcode varchar(10) not null
, b_rtotal_checking float
, b_rts_checking timestamp
, b_rtotal_saving float
, b_rts_saving timestamp
, PRIMARY KEY
  (
    b_id
  )
) with oids;

-- customer table

CREATE TABLE CUSTOMER
(
  cust_id 		bigint     NOT NULL
, cust_fname   	varchar(50) NOT NULL
, cust_lname   	varchar(50) NOT NULL
, cust_dob date not null
, cust_saddress   varchar(200) NOT NULL
, cust_zipcode varchar(10) not null
, cust_curr_tx_count integer not null
, cust_total_tx_count bigint not null
, cust_rts_total_tx_count timestamp
, PRIMARY KEY
  (
    cust_id
  )
) with oids;

-- Account table
CREATE TABLE ACCOUNT
(
  a_id bigint     NOT NULL
, a_cust_id bigint  NOT NULL REFERENCES CUSTOMER (cust_id)
, a_b_id integer  NOT NULL REFERENCES BRANCH (b_id)
, PRIMARY KEY
  (
    a_id
  )
) with oids;

-- Checking table
CREATE TABLE CHECKING
(
  chk_a_id 	    bigint     NOT NULL REFERENCES ACCOUNT (a_id)
, chk_balance 	FLOAT not null
) with oids;

-- Saving table
CREATE TABLE SAVING
(
  sav_a_id 	    bigint     NOT NULL REFERENCES ACCOUNT (a_id)
, sav_balance 	FLOAT not null
) with oids;