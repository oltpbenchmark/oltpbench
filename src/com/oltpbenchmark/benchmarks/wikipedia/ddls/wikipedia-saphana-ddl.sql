DROP PROCEDURE existstable;
CREATE PROCEDURE existstable( IN tablename VARCHAR(20), IN schemaname VARCHAR(20))
    LANGUAGE SQLSCRIPT AS
    BEGIN
        DECLARE myrowid integer;
        myrowid := 0;
        select count(*) into myrowid from "PUBLIC"."M_TABLES" where schema_name =:schemaname and table_name=:tablename;
        IF (:myrowid > 0 ) then
        exec 'DROP TABLE '||:schemaname||'.'||:tablename||' CASCADE';
        END IF;
    End;

-- the schema determined by what user you are using
call existstable('IPBLOCKS', 'SYSTEM');
CREATE TABLE ipblocks (
  ipb_id int NOT NULL,
  ipb_address varbinary(1024) NOT NULL,
  ipb_user int NOT NULL DEFAULT '0',
  ipb_by int NOT NULL DEFAULT '0',
  ipb_by_text varbinary(255) NOT NULL DEFAULT '',
  ipb_reason varbinary(1024) NOT NULL,
  ipb_timestamp varchar(14) NOT NULL DEFAULT '',
  ipb_auto smallint NOT NULL DEFAULT '0',
  ipb_anon_only smallint NOT NULL DEFAULT '0',
  ipb_create_account smallint NOT NULL DEFAULT '1',
  ipb_enable_autoblock smallint NOT NULL DEFAULT '1',
  ipb_expiry varchar(14) NOT NULL DEFAULT '',
  ipb_range_start varbinary(1024) NOT NULL,
  ipb_range_end varbinary(1024) NOT NULL,
  ipb_deleted smallint NOT NULL DEFAULT '0',
  ipb_block_email smallint NOT NULL DEFAULT '0',
  ipb_allow_usertalk smallint NOT NULL DEFAULT '0',
  PRIMARY KEY (ipb_id),
  UNIQUE (ipb_address,ipb_user,ipb_auto,ipb_anon_only)
);
CREATE INDEX IDX_IPB_USER ON ipblocks (ipb_user);
CREATE INDEX IDX_IPB_RANGE ON ipblocks (ipb_range_start,ipb_range_end);
CREATE INDEX IDX_IPB_TIMESTAMP ON ipblocks (ipb_timestamp);
CREATE INDEX IDX_IPB_EXPIRY ON ipblocks (ipb_expiry);

call existstable('USERACCT', 'SYSTEM');
CREATE TABLE useracct (
  user_id int NOT NULL,
  user_name varchar(255) NOT NULL DEFAULT '',
  user_real_name varchar(255) NOT NULL DEFAULT '',
  user_password varchar(255) NOT NULL,
  user_newpassword varchar(255) NOT NULL,
  user_newpass_time varchar(14) DEFAULT NULL,
  user_email varchar(255) NOT NULL,
  user_options varchar(255) NOT NULL,
  user_touched varchar(14) NOT NULL DEFAULT '',
  user_token varchar(32) NOT NULL DEFAULT '',
  user_email_authenticated varchar(14) DEFAULT NULL,
  user_email_token varchar(32) DEFAULT NULL,
  user_email_token_expires varchar(14) DEFAULT NULL,
  user_registration varchar(14) DEFAULT NULL,
  user_editcount int DEFAULT NULL,
  PRIMARY KEY (user_id),
  UNIQUE (user_name)
);
CREATE INDEX IDX_USER_EMAIL_TOKEN ON useracct (user_email_token);

call existstable('LOGGING', 'SYSTEM');
CREATE TABLE logging (
  log_id int NOT NULL,
  log_type varchar(32) NOT NULL,
  log_action varchar(32) NOT NULL,
  log_timestamp varchar(14) NOT NULL DEFAULT '19700101000000',
  log_user int NOT NULL DEFAULT '0',
  log_namespace int NOT NULL DEFAULT '0',
  log_title varchar(255) NOT NULL DEFAULT '',
  log_comment varchar(255) NOT NULL DEFAULT '',
  log_params varchar(255) NOT NULL,
  log_deleted smallint NOT NULL DEFAULT '0',
  log_user_text varchar(255) NOT NULL DEFAULT '',
  log_page int DEFAULT NULL,
  PRIMARY KEY (log_id)
);
CREATE INDEX IDX_LOG_TYPE_TIME ON logging (log_type,log_timestamp);
CREATE INDEX IDX_LOG_USER_TIME ON logging (log_user,log_timestamp);
CREATE INDEX IDX_LOG_PAGE_TIME ON logging (log_namespace,log_title,log_timestamp);
CREATE INDEX IDX_LOG_TIMES ON logging (log_timestamp);
CREATE INDEX IDX_LOG_USER_TYPE_TIME ON logging (log_user,log_type,log_timestamp);
CREATE INDEX IDX_LOG_PAGE_ID_TIME ON logging (log_page,log_timestamp);

call existstable('PAGE', 'SYSTEM');
CREATE TABLE page (
  page_id int NOT NULL,
  page_namespace int NOT NULL,
  page_title varchar(255) NOT NULL,
  page_restrictions varchar(255) NOT NULL,
  page_counter bigint NOT NULL DEFAULT '0',
  page_is_redirect smallint NOT NULL DEFAULT '0',
  page_is_new smallint NOT NULL DEFAULT '0',
  page_random double precision NOT NULL,
  page_touched varchar(14) NOT NULL DEFAULT '',
  page_latest int NOT NULL,
  page_len int NOT NULL,
  PRIMARY KEY (page_id),
  UNIQUE (page_namespace,page_title)
);
CREATE INDEX IDX_PAGE_RANDOM ON page (page_random);
CREATE INDEX IDX_PAGE_LEN ON page (page_len);

call existstable('PAGE_BACKUP', 'SYSTEM');
CREATE TABLE page_backup (
  page_id int NOT NULL,
  page_namespace int NOT NULL,
  page_title varchar(255) NOT NULL,
  page_restrictions varchar(255) NOT NULL,
  page_counter bigint NOT NULL DEFAULT '0',
  page_is_redirect smallint NOT NULL DEFAULT '0',
  page_is_new smallint NOT NULL DEFAULT '0',
  page_random double precision NOT NULL,
  page_touched varchar(14) NOT NULL DEFAULT '',
  page_latest int NOT NULL,
  page_len int NOT NULL,
  PRIMARY KEY (page_id),
  UNIQUE (page_namespace,page_title)
);
CREATE INDEX IDX_PAGE_BACKUP_RANDOM ON page_backup (page_random);
CREATE INDEX IDX_PAGE_BACKUP_LEN ON page_backup (page_len);

call existstable('PAGE_RESTRICTIONS', 'SYSTEM');
CREATE TABLE page_restrictions (
  pr_page int NOT NULL,
  pr_type varchar(60) NOT NULL,
  pr_level varchar(60) NOT NULL,
  pr_cascade smallint NOT NULL,
  pr_user int DEFAULT NULL,
  pr_expiry varchar(14) DEFAULT NULL,
  pr_id int NOT NULL,
  PRIMARY KEY (pr_id),
  UNIQUE (pr_page,pr_type)
);
CREATE INDEX IDX_PR_TYPELEVEL ON page_restrictions (pr_type,pr_level);
CREATE INDEX IDX_PR_LEVEL ON page_restrictions (pr_level);
CREATE INDEX IDX_PR_CASCADE ON page_restrictions (pr_cascade);

call existstable('RECENTCHANGES', 'SYSTEM');
CREATE TABLE recentchanges (
  rc_id int NOT NULL,
  rc_timestamp varchar(14) NOT NULL DEFAULT '',
  rc_cur_time varchar(14) NOT NULL DEFAULT '',
  rc_user int NOT NULL DEFAULT '0',
  rc_user_text varchar(255) NOT NULL,
  rc_namespace int NOT NULL DEFAULT '0',
  rc_title varchar(255) NOT NULL DEFAULT '',
  rc_comment varchar(255) NOT NULL DEFAULT '',
  rc_minor smallint NOT NULL DEFAULT '0',
  rc_bot smallint NOT NULL DEFAULT '0',
  rc_new smallint NOT NULL DEFAULT '0',
  rc_cur_id int NOT NULL DEFAULT '0',
  rc_this_oldid int NOT NULL DEFAULT '0',
  rc_last_oldid int NOT NULL DEFAULT '0',
  rc_type smallint NOT NULL DEFAULT '0',
  rc_moved_to_ns smallint NOT NULL DEFAULT '0',
  rc_moved_to_title varchar(255) NOT NULL DEFAULT '',
  rc_patrolled smallint NOT NULL DEFAULT '0',
  rc_ip varchar(40) NOT NULL DEFAULT '',
  rc_old_len int DEFAULT NULL,
  rc_new_len int DEFAULT NULL,
  rc_deleted smallint NOT NULL DEFAULT '0',
  rc_logid int NOT NULL DEFAULT '0',
  rc_log_type varchar(255) DEFAULT NULL,
  rc_log_action varchar(255) DEFAULT NULL,
  rc_params varchar(255),
  PRIMARY KEY (rc_id)
);
CREATE INDEX IDX_RC_TIMESTAMP ON recentchanges (rc_timestamp);
CREATE INDEX IDX_RC_NAMESPACE_TITLE ON recentchanges (rc_namespace,rc_title);
CREATE INDEX IDX_RC_CUR_ID ON recentchanges (rc_cur_id);
CREATE INDEX IDX_NEW_NAME_TIMESTAMP ON recentchanges (rc_new,rc_namespace,rc_timestamp);
CREATE INDEX IDX_RC_IP ON recentchanges (rc_ip);
CREATE INDEX IDX_RC_NS_USERTEXT ON recentchanges (rc_namespace,rc_user_text);
CREATE INDEX IDX_RC_USER_TEXT ON recentchanges (rc_user_text,rc_timestamp);

call existstable('REVISION', 'SYSTEM');
CREATE TABLE revision (
  rev_id int NOT NULL,
  rev_page int NOT NULL,
  rev_text_id int NOT NULL,
  rev_comment blob NOT NULL,
  rev_user int NOT NULL DEFAULT '0',
  rev_user_text varchar(255) NOT NULL DEFAULT '',
  rev_timestamp varchar(14) NOT NULL DEFAULT '',
  rev_minor_edit smallint NOT NULL DEFAULT '0',
  rev_deleted smallint NOT NULL DEFAULT '0',
  rev_len int DEFAULT NULL,
  rev_parent_id int DEFAULT NULL,
  PRIMARY KEY (rev_id),
  UNIQUE (rev_page,rev_id)
);
CREATE INDEX IDX_REV_TIMESTAMP ON revision (rev_timestamp);
CREATE INDEX IDX_PAGE_TIMESTAMP ON revision (rev_page,rev_timestamp);
CREATE INDEX IDX_USER_TIMESTAMP ON revision (rev_user,rev_timestamp);
CREATE INDEX IDX_USERTEXT_TIMESTAMP ON revision (rev_user_text,rev_timestamp);

call existstable('TEXT', 'SYSTEM');
CREATE TABLE text (
  old_id int NOT NULL,
  old_text blob NOT NULL,
  old_flags varchar(255) NOT NULL,
  old_page int DEFAULT NULL,
  PRIMARY KEY (old_id)
);

call existstable('USER_GROUPS', 'SYSTEM');
CREATE TABLE user_groups (
  ug_user int NOT NULL DEFAULT '0',
  ug_group varchar(16) NOT NULL DEFAULT '',
  UNIQUE (ug_user,ug_group)
);
CREATE INDEX IDX_UG_GROUP ON user_groups (ug_group);

call existstable('VALUE_BACKUP', 'SYSTEM');
CREATE TABLE value_backup (
  table_name varchar(255) DEFAULT NULL,
  maxid int DEFAULT NULL
);

call existstable('WATCHLIST', 'SYSTEM');
CREATE TABLE watchlist (
  wl_user int NOT NULL,
  wl_namespace int NOT NULL DEFAULT '0',
  wl_title varchar(255) NOT NULL DEFAULT '',
  wl_notificationtimestamp varchar(14) DEFAULT NULL,
  UNIQUE (wl_user,wl_namespace,wl_title)
);
CREATE INDEX IDX_WL_NAMESPACE_TITLE ON watchlist (wl_namespace, wl_title);

create sequence ipblocks_seq start with 1 increment by 1 no maxvalue;
create sequence logging_seq start with 1 increment by 1 no maxvalue;
create sequence page_seq start with 1 increment by 1 no maxvalue;
create sequence page_restrictions_seq start with 1 increment by 1 no maxvalue;
create sequence recentchanges_seq start with 1 increment by 1 no maxvalue;
create sequence revision_seq start with 1 increment by 1 no maxvalue;
create sequence text_seq start with 1 increment by 1 no maxvalue;
create sequence user_seq start with 1 increment by 1 no maxvalue;

-- CREATE TRIGGER user_seq_tr
-- INSTEAD OF INSERT ON USERACCT REFERENCING NEW ROW NEW
-- BEGIN
-- INSERT INTO USERACCT VALUES(user_seq.NEXTVAL, :NEW.user_name, :NEW.user_real_name, :NEW.user_password, :NEW.user_newpass_time, :NEW.user_email, :NEW.user_options, :NEW.user_touched, :NEW.user_token, :NEW.user_email_authenticated, :NEW.user_email_token, :NEW.user_email_token_expires, :NEW.user_registration, :NEW.user_editcount);
-- END;

-- user_id int NOT NULL,
-- user_name varchar(255) NOT NULL DEFAULT '',
-- user_real_name varchar(255) NOT NULL DEFAULT '',
-- user_password varchar(255) NOT NULL,
-- user_newpassword varchar(255) NOT NULL,
-- user_newpass_time varchar(14) DEFAULT NULL,
-- user_email varchar(255) NOT NULL,
-- user_options varchar(255) NOT NULL,
-- user_touched varchar(14) NOT NULL DEFAULT '',
-- user_token varchar(32) NOT NULL DEFAULT '',
-- user_email_authenticated varchar(14) DEFAULT NULL,
-- user_email_token varchar(32) DEFAULT NULL,
-- user_email_token_expires varchar(14) DEFAULT NULL,
-- user_registration varchar(14) DEFAULT NULL,
-- user_editcount int DEFAULT NULL,

-- -- Sequences' triggers
-- CREATE TRIGGER user_seq_tr
-- AFTER INSERT ON USERACCT
-- FOR EACH ROW
-- BEGIN
-- UPDATE USERACCT SET user_id = user_seq.NEXTVAL WHERE user_id = -1;
-- -- SELECT user_seq.NEXTVAL INTO :NEW.user_id FROM DUMMY;
-- END;
--
-- CREATE OR REPLACE TRIGGER page_seq_tr
-- BEFORE INSERT ON page FOR EACH ROW
-- WHEN (NEW.page_id IS NULL OR NEW.page_id = 0)
-- BEGIN
-- SELECT page_seq.NEXTVAL INTO :NEW.page_id FROM dual;END;;
--
-- CREATE OR REPLACE TRIGGER text_seq_tr
-- BEFORE INSERT ON text FOR EACH ROW
-- WHEN (NEW.old_id IS NULL OR NEW.old_id = 0)
-- BEGIN
-- SELECT text_seq.NEXTVAL INTO :NEW.old_id FROM dual;END;;
--
-- CREATE OR REPLACE TRIGGER revision_seq_tr
-- BEFORE INSERT ON revision FOR EACH ROW
-- WHEN (NEW.rev_id IS NULL OR NEW.rev_id = 0)
-- BEGIN
-- SELECT revision_seq.NEXTVAL INTO :NEW.rev_id FROM dual;END;;
--
-- CREATE OR REPLACE TRIGGER recentchanges_seq_tr
-- BEFORE INSERT ON recentchanges FOR EACH ROW
-- WHEN (NEW.rc_id IS NULL OR NEW.rc_id = 0)
-- BEGIN
-- SELECT recentchanges_seq.NEXTVAL INTO :NEW.rc_id FROM dual;END;;
--
-- CREATE OR REPLACE TRIGGER logging_seq_tr
-- BEFORE INSERT ON logging FOR EACH ROW
-- WHEN (NEW.log_id IS NULL OR NEW.log_id = 0)
-- BEGIN
-- SELECT logging_seq.NEXTVAL INTO :NEW.log_id FROM dual;END;;
--
-- CREATE TRIGGER user_seq_tr
-- BEFORE INSERT ON USERACCT REFERENCING NEW ROW NEW
-- FOR EACH ROW
-- BEGIN
-- SELECT USER_SEQ.NEXTVAL INTO NEW FROM DUMMY;
-- END;
