/***************************************************************************
 *  Copyright (C) 2010 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  Andy Pavlo (pavlo@cs.brown.edu)                                        *
 *  http://www.cs.brown.edu/~pavlo/                                        *
 *                                                                         *
 *  Visawee Angkanawaraphan (visawee@cs.brown.edu)                         *
 *  http://www.cs.brown.edu/~visawee/                                      *
 *                                                                         *
 *  Permission is hereby granted, free of charge, to any person obtaining  *
 *  a copy of this software and associated documentation files (the        *
 *  "Software"), to deal in the Software without restriction, including    *
 *  without limitation the rights to use, copy, modify, merge, publish,    *
 *  distribute, sublicense, and/or sell copies of the Software, and to     *
 *  permit persons to whom the Software is furnished to do so, subject to  *
 *  the following conditions:                                              *
 *                                                                         *
 *  The above copyright notice and this permission notice shall be         *
 *  included in all copies or substantial portions of the Software.        *
 *                                                                         *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,        *
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF     *
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. *
 *  IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR      *
 *  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,  *
 *  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR  *
 *  OTHER DEALINGS IN THE SOFTWARE.                                        *
 ***************************************************************************/

-- Drop all tables
IF OBJECT_ID('CONFIG_PROFILE') IS NOT NULL DROP table CONFIG_PROFILE;
IF OBJECT_ID('USER_ATTRIBUTES') IS NOT NULL DROP table USER_ATTRIBUTES;
IF OBJECT_ID('ITEM_ATTRIBUTE') IS NOT NULL DROP table ITEM_ATTRIBUTE;
IF OBJECT_ID('ITEM_IMAGE') IS NOT NULL DROP table ITEM_IMAGE;
IF OBJECT_ID('ITEM_COMMENT') IS NOT NULL DROP table ITEM_COMMENT;
IF OBJECT_ID('USER_FEEDBACK') IS NOT NULL DROP table USER_FEEDBACK;
IF OBJECT_ID('USER_ITEM') IS NOT NULL DROP table USER_ITEM;
IF OBJECT_ID('USER_WATCH') IS NOT NULL DROP table USER_WATCH;
IF OBJECT_ID('ITEM_PURCHASE') IS NOT NULL DROP table ITEM_PURCHASE;
IF OBJECT_ID('ITEM_MAX_BID') IS NOT NULL DROP table ITEM_MAX_BID;
IF OBJECT_ID('ITEM_BID') IS NOT NULL DROP table ITEM_BID;
IF OBJECT_ID('ITEM') IS NOT NULL DROP table ITEM;
IF OBJECT_ID('GLOBAL_ATTRIBUTE_VALUE') IS NOT NULL DROP table GLOBAL_ATTRIBUTE_VALUE;
IF OBJECT_ID('GLOBAL_ATTRIBUTE_GROUP') IS NOT NULL DROP table GLOBAL_ATTRIBUTE_GROUP;
IF OBJECT_ID('CATEGORY') IS NOT NULL DROP table CATEGORY;
IF OBJECT_ID('[USER]') IS NOT NULL DROP table [USER];
IF OBJECT_ID('REGION') IS NOT NULL DROP table REGION;

-- Create tables
CREATE TABLE CONFIG_PROFILE (
    CFP_SCALE_FACTOR            FLOAT NOT NULL,
    CFP_BENCHMARK_START			DATETIME NOT NULL,
    CFP_USER_ITEM_HISTOGRAM     TEXT NOT NULL
);

CREATE TABLE REGION (
    r_id                BIGINT NOT NULL,
    r_name              VARCHAR(32),
    PRIMARY KEY (r_id)
);

CREATE TABLE USERACCT (
    u_id                BIGINT NOT NULL,
    u_rating            BIGINT NOT NULL,
    u_balance           FLOAT NOT NULL,
    u_comments			INTEGER DEFAULT 0,
    u_r_id              BIGINT NOT NULL REFERENCES REGION (r_id),
    u_created           DATETIME,
    u_updated           DATETIME,
    u_sattr0            VARCHAR(64),
    u_sattr1            VARCHAR(64),
    u_sattr2            VARCHAR(64),
    u_sattr3            VARCHAR(64),
    u_sattr4            VARCHAR(64),
    u_sattr5            VARCHAR(64),
    u_sattr6            VARCHAR(64),
    u_sattr7            VARCHAR(64),
    u_iattr0            BIGINT DEFAULT NULL,
    u_iattr1            BIGINT DEFAULT NULL,
    u_iattr2			BIGINT DEFAULT NULL,
    u_iattr3            BIGINT DEFAULT NULL,
    u_iattr4            BIGINT DEFAULT NULL,
    u_iattr5            BIGINT DEFAULT NULL,
    u_iattr6            BIGINT DEFAULT NULL,
    u_iattr7            BIGINT DEFAULT NULL, 
    PRIMARY KEY (u_id)
);
CREATE INDEX IDX_USERACCT_REGION ON USERACCT (u_id, u_r_id);

CREATE TABLE USERACCT_ATTRIBUTES (
    ua_id               BIGINT NOT NULL,
    ua_u_id             BIGINT NOT NULL REFERENCES USERACCT (u_id),
    ua_name             VARCHAR(32) NOT NULL,
    ua_value            VARCHAR(32) NOT NULL,
    u_created           DATETIME,
    PRIMARY KEY (ua_id, ua_u_id)
);

CREATE TABLE CATEGORY (
    c_id                BIGINT NOT NULL,
    c_name              VARCHAR(50),
    c_parent_id         BIGINT REFERENCES CATEGORY (c_id),
    PRIMARY KEY (c_id)
);

CREATE TABLE GLOBAL_ATTRIBUTE_GROUP (
    gag_id              BIGINT NOT NULL,
    gag_c_id            BIGINT NOT NULL REFERENCES CATEGORY (c_id),
    gag_name            VARCHAR(100) NOT NULL,
    PRIMARY KEY (gag_id)
);

CREATE TABLE GLOBAL_ATTRIBUTE_VALUE (
    gav_id              BIGINT NOT NULL,
    gav_gag_id          BIGINT NOT NULL REFERENCES GLOBAL_ATTRIBUTE_GROUP (gag_id),
    gav_name            VARCHAR(100) NOT NULL,
    PRIMARY KEY (gav_id, gav_gag_id)
);

CREATE TABLE ITEM (
    i_id                BIGINT NOT NULL,
    i_u_id              BIGINT NOT NULL REFERENCES USERACCT (u_id),
    i_c_id              BIGINT NOT NULL REFERENCES CATEGORY (c_id),
    i_name              VARCHAR(100),
    i_description       VARCHAR(1024),
    i_user_attributes   VARCHAR(255) DEFAULT NULL,
    i_initial_price     FLOAT NOT NULL,
    i_current_price     FLOAT NOT NULL,
    i_num_bids          BIGINT,
    i_num_images        BIGINT,
    i_num_global_attrs  BIGINT,
    i_num_comments      BIGINT,
    i_start_date        DATETIME,
    i_end_date          DATETIME,
    i_status		    INT DEFAULT 0,
    i_updated           DATETIME,
    i_iattr0            BIGINT DEFAULT NULL,
    i_iattr1            BIGINT DEFAULT NULL,
    i_iattr2			BIGINT DEFAULT NULL,
    i_iattr3            BIGINT DEFAULT NULL,
    i_iattr4            BIGINT DEFAULT NULL,
    i_iattr5            BIGINT DEFAULT NULL,
    i_iattr6            BIGINT DEFAULT NULL,
    i_iattr7            BIGINT DEFAULT NULL, 
    PRIMARY KEY (i_id, i_u_id)
);
CREATE INDEX IDX_ITEM_SELLER ON ITEM (i_u_id);


CREATE TABLE ITEM_ATTRIBUTE (
    ia_id               BIGINT NOT NULL,
    ia_i_id             BIGINT NOT NULL,
    ia_u_id             BIGINT NOT NULL,
    ia_gav_id           BIGINT NOT NULL,
    ia_gag_id           BIGINT NOT NULL,
    ia_sattr0			VARCHAR(64) DEFAULT NULL,
    FOREIGN KEY (ia_i_id, ia_u_id) REFERENCES ITEM (i_id, i_u_id),
    FOREIGN KEY (ia_gav_id, ia_gag_id) REFERENCES GLOBAL_ATTRIBUTE_VALUE (gav_id, gav_gag_id),
    PRIMARY KEY (ia_id, ia_i_id, ia_u_id)
);

CREATE TABLE ITEM_IMAGE (
    ii_id               BIGINT NOT NULL,
    ii_i_id             BIGINT NOT NULL,
    ii_u_id             BIGINT NOT NULL,
    ii_sattr0			VARCHAR(128) NOT NULL,
    FOREIGN KEY (ii_i_id, ii_u_id) REFERENCES ITEM (i_id, i_u_id),
    PRIMARY KEY (ii_id, ii_i_id, ii_u_id)
);

CREATE TABLE ITEM_COMMENT (
    ic_id               BIGINT NOT NULL,
    ic_i_id             BIGINT NOT NULL,
    ic_u_id             BIGINT NOT NULL,
    ic_buyer_id         BIGINT NOT NULL REFERENCES USERACCT (u_id),
    ic_question         VARCHAR(128) NOT NULL,
    ic_response         VARCHAR(128) DEFAULT NULL,
    ic_created          DATETIME,
    ic_updated          DATETIME,
    FOREIGN KEY (ic_i_id, ic_u_id) REFERENCES ITEM (i_id, i_u_id),
    PRIMARY KEY (ic_id, ic_i_id, ic_u_id)
); 
-- CREATE INDEX IDX_ITEM_COMMENT ON ITEM_COMMENT (ic_i_id, ic_u_id);

CREATE TABLE ITEM_BID (
    ib_id               BIGINT NOT NULL,
    ib_i_id             BIGINT NOT NULL,
    ib_u_id             BIGINT NOT NULL,
    ib_buyer_id         BIGINT NOT NULL REFERENCES USERACCT (u_id),
    ib_bid		        FLOAT NOT NULL,
    ib_max_bid          FLOAT NOT NULL,
    ib_created          DATETIME,
    ib_updated          DATETIME,
    FOREIGN KEY (ib_i_id, ib_u_id) REFERENCES ITEM (i_id, i_u_id),
    PRIMARY KEY (ib_id, ib_i_id, ib_u_id)
);

CREATE TABLE ITEM_MAX_BID (
    imb_i_id            BIGINT NOT NULL,
    imb_u_id            BIGINT NOT NULL,
    imb_ib_id           BIGINT NOT NULL,
    imb_ib_i_id         BIGINT NOT NULL,
    imb_ib_u_id         BIGINT NOT NULL,
    imb_created         DATETIME,
    imb_updated         DATETIME,
    FOREIGN KEY (imb_i_id, imb_u_id) REFERENCES ITEM (i_id, i_u_id),
    FOREIGN KEY (imb_ib_id, imb_ib_i_id, imb_ib_u_id) REFERENCES ITEM_BID (ib_id, ib_i_id, ib_u_id),
    PRIMARY KEY (imb_i_id, imb_u_id)
);

CREATE TABLE ITEM_PURCHASE (
    ip_id               BIGINT NOT NULL,
    ip_ib_id            BIGINT NOT NULL,
    ip_ib_i_id          BIGINT NOT NULL,
    ip_ib_u_id          BIGINT NOT NULL,
    ip_date             DATETIME,
    FOREIGN KEY (ip_ib_id, ip_ib_i_id, ip_ib_u_id) REFERENCES ITEM_BID (ib_id, ib_i_id, ib_u_id),
    PRIMARY KEY (ip_id, ip_ib_id, ip_ib_i_id, ip_ib_u_id),
    UNIQUE (ip_ib_id, ip_ib_i_id, ip_ib_u_id)
);

CREATE TABLE USERACCT_FEEDBACK (
    uf_u_id             BIGINT NOT NULL REFERENCES USERACCT (u_id),
    uf_i_id             BIGINT NOT NULL,
    uf_i_u_id           BIGINT NOT NULL,
    uf_from_id          BIGINT NOT NULL REFERENCES USERACCT (u_id),
    uf_rating           TINYINT NOT NULL,
    uf_date             DATETIME,
    uf_sattr0           VARCHAR(80) NOT NULL,
    FOREIGN KEY (uf_i_id, uf_i_u_id) REFERENCES ITEM (i_id, i_u_id),
    PRIMARY KEY (uf_u_id, uf_i_id, uf_i_u_id, uf_from_id),
    CHECK (uf_u_id <> uf_from_id)
);

CREATE TABLE USERACCT_ITEM (
    ui_u_id             BIGINT NOT NULL REFERENCES USERACCT (u_id),
    ui_i_id             BIGINT NOT NULL,
    ui_i_u_id           BIGINT NOT NULL,
    ui_ip_id            BIGINT,
    ui_ip_ib_id         BIGINT,
    ui_ip_ib_i_id       BIGINT,
    ui_ip_ib_u_id       BIGINT,
    ui_created          DATETIME,
    FOREIGN KEY (ui_i_id, ui_i_u_id) REFERENCES ITEM (i_id, i_u_id),
    FOREIGN KEY (ui_ip_id, ui_ip_ib_id, ui_ip_ib_i_id, ui_ip_ib_u_id) REFERENCES ITEM_PURCHASE (ip_id, ip_ib_id, ip_ib_i_id, ip_ib_u_id),
    PRIMARY KEY (ui_u_id, ui_i_id, ui_i_u_id)
);
-- CREATE INDEX IDX_USER_ITEM_ID ON USER_ITEM (ui_i_id);

CREATE TABLE USERACCT_WATCH (
    uw_u_id             BIGINT NOT NULL REFERENCES USERACCT (u_id),
    uw_i_id             BIGINT NOT NULL,
    uw_i_u_id           BIGINT NOT NULL,
    uw_created          DATETIME,
    FOREIGN KEY (uw_i_id, uw_i_u_id) REFERENCES ITEM (i_id, i_u_id),
    PRIMARY KEY (uw_u_id, uw_i_id, uw_i_u_id)
);
