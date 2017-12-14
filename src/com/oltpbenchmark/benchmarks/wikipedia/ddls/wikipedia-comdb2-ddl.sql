DROP TABLE IF EXISTS ipblocks;
CREATE TABLE ipblocks (
  ipb_id int NOT NULL,
  ipb_address varchar(15) NOT NULL,
  ipb_user int NOT NULL,
  ipb_by int NOT NULL,
  ipb_by_text varchar(255) NOT NULL,
  ipb_reason varchar(255) NOT NULL,
  ipb_timestamp varchar(14) NOT NULL,
  ipb_auto smallint NOT NULL,
  ipb_anon_only smallint NOT NULL,
  ipb_create_account smallint NOT NULL,
  ipb_enable_autoblock smallint NOT NULL,
  ipb_expiry varchar(14) NOT NULL,
  ipb_range_start varchar(14) NOT NULL,
  ipb_range_end varchar(14) NOT NULL,
  ipb_deleted smallint NOT NULL,
  ipb_block_email smallint NOT NULL,
  ipb_allow_usertalk smallint NOT NULL,
  PRIMARY KEY (ipb_id)
);
CREATE UNIQUE INDEX UNIQUE_IDX_IPB ON ipblocks (ipb_address, ipb_user, ipb_auto, ipb_anon_only);
CREATE INDEX IDX_IPB_USER ON ipblocks (ipb_user);
CREATE INDEX IDX_IPB_RANGE ON ipblocks (ipb_range_start, ipb_range_end);
CREATE INDEX IDX_IPB_TIMESTAMP ON ipblocks (ipb_timestamp);
CREATE INDEX IDX_IPB_EXPIRY ON ipblocks (ipb_expiry);

DROP TABLE IF EXISTS useracct;
CREATE TABLE useracct (
  user_id int NOT NULL,
  user_name varchar(255) NOT NULL,
  user_real_name varchar(255) NOT NULL,
  user_password varchar(511) NOT NULL,
  user_newpassword varchar(511) NOT NULL,
  user_newpass_time varchar(14) DEFAULT NULL,
  user_email varchar(255) NOT NULL,
  user_options varchar(255) NOT NULL,
  user_touched varchar(14) NOT NULL DEFAULT "" WITH dbpad=0,
  user_token varchar(32) NOT NULL DEFAULT "" WITH dbpad=0,
  user_email_authenticated varchar(14) DEFAULT NULL,
  user_email_token varchar(32) DEFAULT NULL,
  user_email_token_expires varchar(14) DEFAULT NULL,
  user_registration varchar(14) DEFAULT NULL,
  user_editcount int DEFAULT '0',
  PRIMARY KEY (user_id)
);
CREATE UNIQUE INDEX UNIQUE_IDX_USER ON useracct (user_name);
CREATE INDEX IDX_USER_EMAIL_TOKEN ON useracct (user_email_token);

DROP TABLE IF EXISTS logging;
CREATE TABLE logging (
  log_id int NOT NULL,
  log_type varchar(32) NOT NULL,
  log_action varchar(32) NOT NULL,
  log_timestamp varchar(14) NOT NULL,
  log_user int NOT NULL,
  log_namespace int NOT NULL,
  log_title varchar(255) NOT NULL,
  log_comment varchar(255) NOT NULL,
  log_params varchar(255) NOT NULL,
  log_deleted smallint NOT NULL,
  log_user_text varchar(255) NOT NULL,
  log_page int DEFAULT '0',
  PRIMARY KEY (log_id)
);
CREATE INDEX IDX_LOG_TYPE_TIME ON logging (log_type,log_timestamp);
CREATE INDEX IDX_LOG_USER_TIME ON logging (log_user,log_timestamp);
CREATE INDEX IDX_LOG_PAGE_TIME ON logging (log_namespace,log_title,log_timestamp);
CREATE INDEX IDX_LOG_TIMES ON logging (log_timestamp);
CREATE INDEX IDX_LOG_USER_TYPE_TIME ON logging (log_user,log_type,log_timestamp);
CREATE INDEX IDX_LOG_PAGE_ID_TIME ON logging (log_page,log_timestamp);

DROP TABLE IF EXISTS page;
CREATE TABLE page (
  page_id int NOT NULL,
  page_namespace int NOT NULL,
  page_title varchar(255) NOT NULL,
  page_restrictions varchar(255) NOT NULL,
  page_counter bigint NOT NULL DEFAULT '0',
  page_is_redirect smallint NOT NULL DEFAULT '0',
  page_is_new smallint NOT NULL DEFAULT '0',
  page_random double NOT NULL,
  page_touched varchar(14) NOT NULL DEFAULT "" WITH dbpad=0,
  page_latest int NOT NULL,
  page_len int NOT NULL,
  PRIMARY KEY (page_id)
);
CREATE UNIQUE INDEX UNIQUE_IDX_PAGE ON page (page_namespace, page_title);
CREATE INDEX IDX_PAGE_RANDOM ON page (page_random);
CREATE INDEX IDX_PAGE_LEN ON page (page_len);

DROP TABLE IF EXISTS page_backup;
CREATE TABLE page_backup (
  page_id int NOT NULL,
  page_namespace int NOT NULL,
  page_title varchar(255) NOT NULL,
  page_restrictions varchar(255) NOT NULL,
  page_counter bigint NOT NULL,
  page_is_redirect smallint NOT NULL,
  page_is_new smallint NOT NULL,
  page_random double NOT NULL,
  page_touched varchar(14) NOT NULL,
  page_latest int NOT NULL,
  page_len int NOT NULL,
  PRIMARY KEY (page_id)
);
CREATE UNIQUE INDEX UNIQUE_IDX_PB ON page_backup (page_namespace, page_title);
CREATE INDEX IDX_PAGE_BACKUP_RANDOM ON page_backup (page_random);
CREATE INDEX IDX_PAGE_BACKUP_LEN ON page_backup (page_len);

DROP TABLE IF EXISTS page_restrictions;
CREATE TABLE page_restrictions (
  pr_page int NOT NULL,
  pr_type varchar(60) NOT NULL,
  pr_level varchar(60) NOT NULL,
  pr_cascade smallint NOT NULL,
  pr_user int DEFAULT '0',
  pr_expiry varchar(14) DEFAULT NULL,
  pr_id int NOT NULL,
  PRIMARY KEY (pr_id)
);
CREATE UNIQUE INDEX UNIQUE_IDX_PR ON page_restrictions (pr_page, pr_type);
CREATE INDEX IDX_PR_TYPELEVEL ON page_restrictions (pr_type,pr_level);
CREATE INDEX IDX_PR_LEVEL ON page_restrictions (pr_level);
CREATE INDEX IDX_PR_CASCADE ON page_restrictions (pr_cascade);

DROP TABLE IF EXISTS recentchanges;
CREATE TABLE recentchanges (
  rc_id int NOT NULL,
  rc_timestamp varchar(14) NOT NULL,
  rc_cur_time varchar(14) NOT NULL,
  rc_user int NOT NULL,
  rc_user_text varchar(255) NOT NULL,
  rc_namespace int NOT NULL,
  rc_title varchar(255) NOT NULL,
  rc_comment varchar(255) NOT NULL,
  rc_minor smallint NOT NULL,
  rc_bot smallint NOT NULL,
  rc_new smallint NOT NULL,
  rc_cur_id int NOT NULL,
  rc_this_oldid int NOT NULL,
  rc_last_oldid int NOT NULL,
  rc_type smallint NOT NULL,
  rc_moved_to_ns smallint NOT NULL,
  rc_moved_to_title varchar(255) NOT NULL,
  rc_patrolled smallint NOT NULL,
  rc_ip varchar(40) NOT NULL,
  rc_old_len int DEFAULT '0',
  rc_new_len int DEFAULT '0',
  rc_deleted smallint NOT NULL,
  rc_logid int NOT NULL,
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

DROP TABLE IF EXISTS revision;
CREATE TABLE revision (
  rev_id int NOT NULL,
  rev_page int NOT NULL,
  rev_text_id int NOT NULL,
  rev_comment text NOT NULL,
  rev_user int NOT NULL,
  rev_user_text varchar(255) NOT NULL,
  rev_timestamp varchar(14) NOT NULL,
  rev_minor_edit smallint NOT NULL,
  rev_deleted smallint NOT NULL,
  rev_len int DEFAULT '0',
  rev_parent_id int DEFAULT '0',
  PRIMARY KEY (rev_id)
);
CREATE UNIQUE INDEX UNIQUE_IDX_REV ON revision (rev_id, rev_page);
CREATE INDEX IDX_REV_TIMESTAMP ON revision (rev_timestamp);
CREATE INDEX IDX_PAGE_TIMESTAMP ON revision (rev_page,rev_timestamp);
CREATE INDEX IDX_USER_TIMESTAMP ON revision (rev_user,rev_timestamp);
CREATE INDEX IDX_USERTEXT_TIMESTAMP ON revision (rev_user_text,rev_timestamp);

DROP TABLE IF EXISTS text;
CREATE TABLE text (
  old_id int NOT NULL,
  old_text text NOT NULL,
  old_flags varchar(255) NOT NULL,
  old_page int DEFAULT '0',
  PRIMARY KEY (old_id)
);


DROP TABLE IF EXISTS user_groups;
CREATE TABLE user_groups (
  ug_user int NOT NULL,
  ug_group varchar(16) NOT NULL
);
CREATE UNIQUE INDEX UNIQUE_IDX_UG ON user_groups (ug_user, ug_group);
CREATE INDEX IDX_UG_GROUP ON user_groups (ug_group);

DROP TABLE IF EXISTS value_backup;
CREATE TABLE value_backup (
  table_name varchar(255) DEFAULT NULL,
  maxid int DEFAULT '0'
);

DROP TABLE IF EXISTS watchlist;
CREATE TABLE watchlist (
  wl_user int NOT NULL,
  wl_namespace int NOT NULL,
  wl_title varchar(255) NOT NULL,
  wl_notificationtimestamp varchar(14) DEFAULT NULL
);
CREATE UNIQUE INDEX UNIQUE_IDX_WL ON watchlist (wl_user, wl_namespace, wl_title);
CREATE INDEX IDX_WL_NAMESPACE_TITLE ON watchlist (wl_namespace, wl_title);