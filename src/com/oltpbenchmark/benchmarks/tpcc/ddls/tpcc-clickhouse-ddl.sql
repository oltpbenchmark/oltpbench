-- TODO: c_since ON UPDATE CURRENT_TIMESTAMP,

DROP TABLE IF EXISTS TPCC.ORDER_LINE;
CREATE TABLE TPCC.ORDER_LINE (
  OL_W_ID int,
  OL_D_ID int,
  OL_O_ID int,
  OL_NUMBER int,
  OL_I_ID int,
  OL_DELIVERY_D DateTime,
  OL_AMOUNT Float32,
  OL_SUPPLY_W_ID int,
  OL_QUANTITY Float32,
  OL_DIST_INFO FixedString(24),
  EventDate Date DEFAULT toDate(now()),
  version DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(EventDate, (OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER), 8192, version);

DROP TABLE IF EXISTS TPCC.NEW_ORDER;
CREATE TABLE TPCC.NEW_ORDER (
  NO_W_ID int,
  NO_D_ID int,
  NO_O_ID int,
  EventDate Date DEFAULT toDate(now()),
  version DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(EventDate, (NO_W_ID, NO_D_ID, NO_O_ID), 8192, version);

DROP TABLE IF EXISTS TPCC.STOCK;
CREATE TABLE TPCC.STOCK (
  S_W_ID int,
  S_I_ID int,
  S_QUANTITY Float32,
  S_YTD Float32,
  S_ORDER_CNT int,
  S_REMOTE_CNT int,
  S_DATA String,
  S_DIST_01 FixedString(24),
  S_DIST_02 FixedString(24),
  S_DIST_03 FixedString(24),
  S_DIST_04 FixedString(24),
  S_DIST_05 FixedString(24),
  S_DIST_06 FixedString(24),
  S_DIST_07 FixedString(24),
  S_DIST_08 FixedString(24),
  S_DIST_09 FixedString(24),
  S_DIST_10 FixedString(24),
  EventDate Date DEFAULT toDate(now()),
  version DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(EventDate, (S_W_ID, S_I_ID), 8192, version);

-- TODO: o_entry_d  ON UPDATE CURRENT_TIMESTAMP
DROP TABLE IF EXISTS TPCC.OORDER;
CREATE TABLE TPCC.OORDER (
  O_W_ID int,
  O_D_ID int,
  O_ID int,
  O_C_ID int,
  O_CARRIER_ID int,
  O_OL_CNT Float32,
  O_ALL_LOCAL Float32,
  O_ENTRY_D DateTime DEFAULT now(),
  EventDate Date DEFAULT toDate(now()),
  version DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(EventDate, (O_W_ID,O_D_ID,O_ID), 8192, version);

-- TODO: h_date ON UPDATE CURRENT_TIMESTAMP
DROP TABLE IF EXISTS TPCC.HISTORY;
CREATE TABLE TPCC.HISTORY (
  H_C_ID int,
  H_C_D_ID int,
  H_C_W_ID int,
  H_D_ID int,
  H_W_ID int,
  H_DATE DateTime DEFAULT now(),
  H_AMOUNT Float32,
  H_DATA String,
  EventDate Date DEFAULT toDate(now()),
  version DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(EventDate, (H_C_ID, H_C_D_ID, H_C_W_ID, H_D_ID, H_W_ID), 8192, version);

DROP TABLE IF EXISTS TPCC.CUSTOMER;
CREATE TABLE TPCC.CUSTOMER (
  C_W_ID int,
  C_D_ID int,
  C_ID int,
  C_DISCOUNT Float32,
  C_CREDIT FixedString(2),
  C_LAST String,
  C_FIRST String,
  C_CREDIT_LIM Float64,
  C_BALANCE Float64,
  C_YTD_PAYMENT Float64,
  C_PAYMENT_CNT int,
  C_DELIVERY_CNT int,
  C_STREET_1 String,
  C_STREET_2 String,
  C_CITY String,
  C_STATE FixedString(2),
  C_ZIP FixedString(9),
  C_PHONE FixedString(16),
  C_SINCE DateTime DEFAULT now(),
  C_MIDDLE FixedString(2),
  C_DATA String,
  EventDate Date DEFAULT toDate(now()),
  version DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(EventDate, (C_W_ID, C_D_ID, C_ID), 8192, version);

DROP TABLE IF EXISTS TPCC.DISTRICT;
CREATE TABLE TPCC.DISTRICT (
  D_W_ID int,
  D_ID int,
  D_YTD Float64,
  D_TAX Float32,
  D_NEXT_O_ID int,
  D_NAME String,
  D_STREET_1 String,
  D_STREET_2 String,
  D_CITY String,
  D_STATE String,
  D_ZIP String,
  EventDate Date DEFAULT toDate(now()),
  version DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(EventDate, (D_W_ID, D_ID), 8192, version);


DROP TABLE IF EXISTS TPCC.ITEM;
CREATE TABLE TPCC.ITEM (
  I_ID int,
  I_NAME String,
  I_PRICE Float32,
  I_DATA String,
  I_IM_ID int,
  EventDate Date DEFAULT toDate(now()),
  version DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(EventDate, (I_ID), 8192, version);

DROP TABLE IF EXISTS TPCC.WAREHOUSE;
CREATE TABLE TPCC.WAREHOUSE (
  W_ID int,
  W_YTD Float64,
  W_TAX Float32,
  W_NAME String,
  W_STREET_1 String,
  W_STREET_2 String,
  W_CITY String,
  W_STATE FixedString(2),
  W_ZIP FixedString(9),
  EventDate Date DEFAULT toDate(now()),
  version DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(EventDate, (W_ID), 8192, version);

--add 'ON DELETE CASCADE'  to clear table work correctly

--ALTER TABLE district  ADD CONSTRAINT fkey_district_1 FOREIGN KEY(d_w_id) REFERENCES warehouse(w_id) ON DELETE CASCADE;
--ALTER TABLE customer ADD CONSTRAINT fkey_customer_1 FOREIGN KEY(c_w_id,c_d_id) REFERENCES district(d_w_id,d_id)  ON DELETE CASCADE ;
--ALTER TABLE history  ADD CONSTRAINT fkey_history_1 FOREIGN KEY(h_c_w_id,h_c_d_id,h_c_id) REFERENCES customer(c_w_id,c_d_id,c_id) ON DELETE CASCADE;
--ALTER TABLE history  ADD CONSTRAINT fkey_history_2 FOREIGN KEY(h_w_id,h_d_id) REFERENCES district(d_w_id,d_id) ON DELETE CASCADE;
--ALTER TABLE new_order ADD CONSTRAINT fkey_new_order_1 FOREIGN KEY(no_w_id,no_d_id,no_o_id) REFERENCES oorder(o_w_id,o_d_id,o_id) ON DELETE CASCADE;
--ALTER TABLE oorder ADD CONSTRAINT fkey_order_1 FOREIGN KEY(o_w_id,o_d_id,o_c_id) REFERENCES customer(c_w_id,c_d_id,c_id) ON DELETE CASCADE;
--ALTER TABLE order_line ADD CONSTRAINT fkey_order_line_1 FOREIGN KEY(ol_w_id,ol_d_id,ol_o_id) REFERENCES oorder(o_w_id,o_d_id,o_id) ON DELETE CASCADE;
--ALTER TABLE order_line ADD CONSTRAINT fkey_order_line_2 FOREIGN KEY(ol_supply_w_id,ol_i_id) REFERENCES stock(s_w_id,s_i_id) ON DELETE CASCADE;
--ALTER TABLE stock ADD CONSTRAINT fkey_stock_1 FOREIGN KEY(s_w_id) REFERENCES warehouse(w_id) ON DELETE CASCADE;
--ALTER TABLE stock ADD CONSTRAINT fkey_stock_2 FOREIGN KEY(s_i_id) REFERENCES item(i_id) ON DELETE CASCADE;
