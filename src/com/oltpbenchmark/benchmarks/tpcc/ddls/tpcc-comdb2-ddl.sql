-- TODO: c_since ON UPDATE CURRENT_TIMESTAMP,

DROP TABLE IF EXISTS order_line;
CREATE TABLE order_line (
  ol_w_id int NOT NULL,
  ol_d_id int NOT NULL,
  ol_o_id int NOT NULL,
  ol_number int NOT NULL,
  ol_i_id int NOT NULL,
  ol_delivery_d datetime NULL DEFAULT NULL,
  ol_amount double NOT NULL,
  ol_supply_w_id int NOT NULL,
  ol_quantity double NOT NULL,
  ol_dist_info char(24) NOT NULL,
  PRIMARY KEY (ol_w_id,ol_d_id,ol_o_id,ol_number)
);

DROP TABLE IF EXISTS new_order;
CREATE TABLE new_order (
  no_w_id int NOT NULL,
  no_d_id int NOT NULL,
  no_o_id int NOT NULL,
  PRIMARY KEY (no_w_id,no_d_id,no_o_id)
);

DROP TABLE IF EXISTS stock;
CREATE TABLE stock (
  s_w_id int NOT NULL,
  s_i_id int NOT NULL,
  s_quantity double NOT NULL,
  s_ytd double NOT NULL,
  s_order_cnt int NOT NULL,
  s_remote_cnt int NOT NULL,
  s_data varchar(50) NOT NULL,
  s_dist_01 char(24) NOT NULL,
  s_dist_02 char(24) NOT NULL,
  s_dist_03 char(24) NOT NULL,
  s_dist_04 char(24) NOT NULL,
  s_dist_05 char(24) NOT NULL,
  s_dist_06 char(24) NOT NULL,
  s_dist_07 char(24) NOT NULL,
  s_dist_08 char(24) NOT NULL,
  s_dist_09 char(24) NOT NULL,
  s_dist_10 char(24) NOT NULL,
  PRIMARY KEY (s_w_id,s_i_id)
);

-- TODO: o_entry_d  ON UPDATE CURRENT_TIMESTAMP
DROP TABLE IF EXISTS oorder;
CREATE TABLE oorder (
  o_w_id int NOT NULL,
  o_d_id int NOT NULL,
  o_id int NOT NULL,
  o_c_id int NOT NULL,
  o_carrier_id int,
  o_ol_cnt double NOT NULL,
  o_all_local double NOT NULL,
  o_entry_d datetime DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (o_w_id,o_d_id,o_id)
);

-- TODO: h_date ON UPDATE CURRENT_TIMESTAMP
DROP TABLE IF EXISTS history;
CREATE TABLE history (
  h_c_id int NOT NULL,
  h_c_d_id int NOT NULL,
  h_c_w_id int NOT NULL,
  h_d_id int NOT NULL,
  h_w_id int NOT NULL,
  h_date datetime DEFAULT CURRENT_TIMESTAMP,
  h_amount double NOT NULL,
  h_data varchar(24) NOT NULL
);

DROP TABLE IF EXISTS customer;
CREATE TABLE customer (
  c_w_id int NOT NULL,
  c_d_id int NOT NULL,
  c_id int NOT NULL,
  c_discount double NOT NULL,
  c_credit char(3) NOT NULL,
  c_last varchar(16) NOT NULL,
  c_first varchar(16) NOT NULL,
  c_credit_lim double NOT NULL,
  c_balance double NOT NULL,
  c_ytd_payment float NOT NULL,
  c_payment_cnt int NOT NULL,
  c_delivery_cnt int NOT NULL,
  c_street_1 varchar(20) NOT NULL,
  c_street_2 varchar(20) NOT NULL,
  c_city varchar(20) NOT NULL,
  c_state char(3) NOT NULL,
  c_zip char(10) NOT NULL,
  c_phone char(17) NOT NULL,
  c_since datetime DEFAULT CURRENT_TIMESTAMP,
  c_middle char(3) NOT NULL,
  c_data varchar(500) NOT NULL,
  PRIMARY KEY (c_w_id,c_d_id,c_id)
);

DROP TABLE IF EXISTS district;
CREATE TABLE district (
  d_w_id int NOT NULL,
  d_id int NOT NULL,
  d_ytd double NOT NULL,
  d_tax double NOT NULL,
  d_next_o_id int NOT NULL,
  d_name varchar(10) NOT NULL,
  d_street_1 varchar(20) NOT NULL,
  d_street_2 varchar(20) NOT NULL,
  d_city varchar(20) NOT NULL,
  d_state char(3) NOT NULL,
  d_zip char(10) NOT NULL,
  PRIMARY KEY (d_w_id,d_id)
);


DROP TABLE IF EXISTS item;
CREATE TABLE item (
  i_id int NOT NULL,
  i_name varchar(24) NOT NULL,
  i_price double NOT NULL,
  i_data varchar(50) NOT NULL,
  i_im_id int NOT NULL,
  PRIMARY KEY (i_id)
);

DROP TABLE IF EXISTS warehouse;
CREATE TABLE warehouse (
  w_id int NOT NULL,
  w_ytd double NOT NULL,
  w_tax double NOT NULL,
  w_name varchar(10) NOT NULL,
  w_street_1 varchar(20) NOT NULL,
  w_street_2 varchar(20) NOT NULL,
  w_city varchar(20) NOT NULL,
  w_state char(3) NOT NULL,
  w_zip char(10) NOT NULL,
  PRIMARY KEY (w_id)
);


--add constraints and indexes
CREATE INDEX idx_customer_name ON customer (c_w_id,c_d_id,c_last,c_first);
CREATE INDEX idx_order ON oorder (o_w_id,o_d_id,o_c_id,o_id);
-- tpcc-mysql create two indexes for the foreign key constraints, Is it really necessary?
-- CREATE INDEX FKEY_STOCK_2 ON STOCK (S_I_ID);
-- CREATE INDEX FKEY_ORDER_LINE_2 ON ORDER_LINE (OL_SUPPLY_W_ID,OL_I_ID);

--add 'ON DELETE CASCADE'  to clear table work correctly

-- ALTER TABLE district  ADD CONSTRAINT fkey_district_1 FOREIGN KEY(d_w_id) REFERENCES warehouse(w_id) ON DELETE CASCADE;
-- ALTER TABLE customer ADD CONSTRAINT fkey_customer_1 FOREIGN KEY(c_w_id,c_d_id) REFERENCES district(d_w_id,d_id)  ON DELETE CASCADE ;
-- ALTER TABLE history  ADD CONSTRAINT fkey_history_1 FOREIGN KEY(h_c_w_id,h_c_d_id,h_c_id) REFERENCES customer(c_w_id,c_d_id,c_id) ON DELETE CASCADE;
-- ALTER TABLE history  ADD CONSTRAINT fkey_history_2 FOREIGN KEY(h_w_id,h_d_id) REFERENCES district(d_w_id,d_id) ON DELETE CASCADE;
-- ALTER TABLE new_order ADD CONSTRAINT fkey_new_order_1 FOREIGN KEY(no_w_id,no_d_id,no_o_id) REFERENCES oorder(o_w_id,o_d_id,o_id) ON DELETE CASCADE;
-- ALTER TABLE oorder ADD CONSTRAINT fkey_order_1 FOREIGN KEY(o_w_id,o_d_id,o_c_id) REFERENCES customer(c_w_id,c_d_id,c_id) ON DELETE CASCADE;
-- ALTER TABLE order_line ADD CONSTRAINT fkey_order_line_1 FOREIGN KEY(ol_w_id,ol_d_id,ol_o_id) REFERENCES oorder(o_w_id,o_d_id,o_id) ON DELETE CASCADE;
-- ALTER TABLE order_line ADD CONSTRAINT fkey_order_line_2 FOREIGN KEY(ol_supply_w_id,ol_i_id) REFERENCES stock(s_w_id,s_i_id) ON DELETE CASCADE;
-- ALTER TABLE stock ADD CONSTRAINT fkey_stock_1 FOREIGN KEY(s_w_id) REFERENCES warehouse(w_id) ON DELETE CASCADE;
-- ALTER TABLE stock ADD CONSTRAINT fkey_stock_2 FOREIGN KEY(s_i_id) REFERENCES item(i_id) ON DELETE CASCADE;

