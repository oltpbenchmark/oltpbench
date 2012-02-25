package com.oltpbenchmark.benchmarks.wikipedia;

import java.io.File;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;

import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.benchmarks.wikipedia.data.PageHistograms;
import com.oltpbenchmark.benchmarks.wikipedia.data.RevisionHistograms;
import com.oltpbenchmark.benchmarks.wikipedia.data.TextHistograms;
import com.oltpbenchmark.benchmarks.wikipedia.data.UserHistograms;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.distributions.ZipfianGenerator;
import com.oltpbenchmark.jdbc.AutoIncrementPreparedStatement;
import com.oltpbenchmark.types.DatabaseType;
import com.oltpbenchmark.util.FileUtil;
import com.oltpbenchmark.util.Pair;
import com.oltpbenchmark.util.RandomDistribution.FlatHistogram;
import com.oltpbenchmark.util.SQLUtil;
import com.oltpbenchmark.util.StringUtil;
import com.oltpbenchmark.util.TextGenerator;
import com.oltpbenchmark.util.TimeUtil;

/**
 * Synthetic Wikipedia Data Loader
 * @author pavlo
 * @author djellel
 */
public class WikipediaLoader extends Loader {
    private static final Logger LOG = Logger.getLogger(WikipediaLoader.class);

    private final int num_users;
    private final int num_pages;
    
    /**
     * UserId -> # of Revisions
     */
    private final int user_revision_ctr[];

    /**
     * PageId -> Last Revision Id
     */
    private final int page_last_rev_id[];
    
    /**
     * PageId -> Last Revision Length
     */
    private final int page_last_rev_length[];
    
    /**
     * Pair<PageNamespace, PageTitle>
     */
    private List<Pair<Integer, String>> titles = new ArrayList<Pair<Integer, String>>();

    /**
     * Constructor
     * @param benchmark
     * @param c
     */
    public WikipediaLoader(WikipediaBenchmark benchmark, Connection c) {
        super(benchmark, c);
        this.num_users = (int) Math.round(WikipediaConstants.USERS * this.scaleFactor);
        this.num_pages = (int) Math.round(WikipediaConstants.PAGES * this.scaleFactor);
        
        this.user_revision_ctr = new int[this.num_users];
        Arrays.fill(this.user_revision_ctr, 0);
        
        this.page_last_rev_id = new int[this.num_pages];
        Arrays.fill(this.page_last_rev_id, -1);
        this.page_last_rev_length = new int[this.num_pages];
        Arrays.fill(this.page_last_rev_length, -1);
        
        if (LOG.isDebugEnabled()) {
            LOG.debug("# of USERS:  " + this.num_users);
            LOG.debug("# of PAGES: " + this.num_pages);
        }
    }

    @Override
    public void load() {
        try {
            // Load Data
            this.loadUsers();
//            if (num_users > 0) return;
            this.loadPages();
            this.loadWatchlist();
            this.loadRevision();

            // Generate Trace File
            if (this.workConf.getXmlConfig() != null && this.workConf.getXmlConfig().containsKey("traceOut")) {
                this.genTrace(this.workConf.getXmlConfig().getInt("traceOut", 0));
            }
            
        } catch (SQLException e) {
            e.printStackTrace();
            e = e.getNextException();
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    private File genTrace(int trace) throws Exception {
        if (trace == 0) return (null);
        
        assert(this.num_pages == this.titles.size());
        ZipfianGenerator pages = new ZipfianGenerator(this.num_pages);
        
        File file = null;
        if (isTesting()) {
            file = FileUtil.getTempFile("wiki", true);
        } else {
            file = new File("wikipedia-" + trace + "k.trace");
        }
        LOG.info(String.format("Generating a %dk trace to '%s'", trace, file));
        
        PrintStream ps = new PrintStream(file);
        for (int i = 0, cnt = (trace * 1000); i < cnt; i++) {
            int user_id = rng().nextInt(this.num_users);
            // lets 10% be unauthenticated users
            if (user_id % 10 == 0) {
                user_id = 0;
            }
            String title = this.titles.get(pages.nextInt()).getSecond();
            ps.println(user_id + " " + title);
        } // FOR
        ps.close();
        return (file);
    }
    
    /**
     * Load Wikipedia USER table
     */
    private void loadUsers() throws SQLException {
        Table catalog_tbl = this.getTableCatalog(WikipediaConstants.TABLENAME_USER);
        assert(catalog_tbl != null);

        String sql = SQLUtil.getInsertSQL(catalog_tbl);
        PreparedStatement userInsert = this.conn.prepareStatement(sql);

        FlatHistogram<Integer> h_nameLength = new FlatHistogram<Integer>(this.rng(), UserHistograms.NAME_LENGTH);
        FlatHistogram<Integer> h_realNameLength = new FlatHistogram<Integer>(this.rng(), UserHistograms.REAL_NAME_LENGTH);
        FlatHistogram<Integer> h_revCount = new FlatHistogram<Integer>(this.rng(), UserHistograms.REVISION_COUNT);

        int types[] = catalog_tbl.getColumnTypes();
        int batch_size = 0;
        for (int i = 1; i <= this.num_users; i++) {
            String name = TextGenerator.randomStr(rng(), h_nameLength.nextValue().intValue());
            String realName = TextGenerator.randomStr(rng(), h_realNameLength.nextValue().intValue());
            int revCount = h_revCount.nextValue().intValue();
            String password = StringUtil.repeat("*", rng().nextInt(32));
            
            char eChars[] = TextGenerator.randomChars(rng(), rng().nextInt(32) + 5);
            eChars[4 + rng().nextInt(eChars.length-4)] = '@';
            String email = new String(eChars);
            
            String token = TextGenerator.randomStr(rng(), WikipediaConstants.TOKEN_LENGTH);
            String userOptions = "fake_longoptionslist";
            String newPassTime = TimeUtil.getCurrentTimeString14();
            String touched = TimeUtil.getCurrentTimeString14();

            int param = 1;
            userInsert.setInt(param++, i);                // user_id
            userInsert.setString(param++, name);          // user_name
            userInsert.setString(param++, realName);      // user_real_name
            userInsert.setString(param++, password);      // user_password
            userInsert.setString(param++, password);      // user_newpassword
            userInsert.setString(param++, newPassTime);   // user_newpass_time
            userInsert.setString(param++, email);         // user_email
            userInsert.setString(param++, userOptions);   // user_options
            userInsert.setString(param++, touched);       // user_touched
            userInsert.setString(param++, token);         // user_token
            userInsert.setNull(param++, types[param-2]);    // user_email_authenticated
            userInsert.setNull(param++, types[param-2]);    // user_email_token
            userInsert.setNull(param++, types[param-2]);    // user_email_token_expires
            userInsert.setNull(param++, types[param-2]);    // user_registration
            userInsert.setInt(param++, revCount);         // user_editcount
            userInsert.addBatch();

            if (++batch_size % WikipediaConstants.BATCH_SIZE == 0) {
                userInsert.executeBatch();
                this.conn.commit();
                userInsert.clearBatch();
                batch_size = 0;
                if (LOG.isDebugEnabled())
                    LOG.debug("Users  % " + i);
            }
        } // FOR
        if (batch_size > 0) {
            userInsert.executeBatch();
            this.conn.commit();
            userInsert.clearBatch();
        }
        if (this.getDatabaseType() == DatabaseType.POSTGRES) {
            this.updateAutoIncrement(catalog_tbl.getColumn(0), this.num_users);
        }
        if (LOG.isDebugEnabled())
            LOG.debug("Users  % " + this.num_users);
    }

    /**
     * PAGE
     */
    private void loadPages() throws SQLException {
        Table catalog_tbl = this.getTableCatalog(WikipediaConstants.TABLENAME_PAGE);
        assert(catalog_tbl != null);

        String sql = SQLUtil.getInsertSQL(catalog_tbl);
        PreparedStatement pageInsert = this.conn.prepareStatement(sql);
        
        FlatHistogram<Integer> h_titleLength = new FlatHistogram<Integer>(this.rng(), PageHistograms.TITLE_LENGTH);
        FlatHistogram<Integer> h_namespace = new FlatHistogram<Integer>(this.rng(), PageHistograms.NAMESPACE);
        FlatHistogram<String> h_restrictions = new FlatHistogram<String>(this.rng(), PageHistograms.RESTRICTIONS);

        int batch_size = 0;
        for (int i = 1; i <= this.num_pages; i++) {
            String title = TextGenerator.randomStr(rng(), h_titleLength.nextValue().intValue());
            int namespace = h_namespace.nextValue().intValue();
            String restrictions = h_restrictions.nextValue();
            double pageRandom = rng().nextDouble();
            String pageTouched = TimeUtil.getCurrentTimeString14();
            
            int param = 1;
            pageInsert.setInt(param++, i);              // page_id
            pageInsert.setInt(param++, namespace);      // page_namespace
            pageInsert.setString(param++, title);       // page_title
            pageInsert.setString(param++, restrictions);// page_restrictions
            pageInsert.setInt(param++, 0);              // page_counter
            pageInsert.setInt(param++, 0);              // page_is_redirect
            pageInsert.setInt(param++, 0);              // page_is_new
            pageInsert.setDouble(param++, pageRandom);  // page_random
            pageInsert.setString(param++, pageTouched); // page_touched
            pageInsert.setInt(param++, 0);              // page_latest
            pageInsert.setInt(param++, 0);              // page_len
            pageInsert.addBatch();
            this.titles.add(Pair.of(namespace, title));

            if (++batch_size % WikipediaConstants.BATCH_SIZE == 0) {
                pageInsert.executeBatch();
                this.conn.commit();
                pageInsert.clearBatch();
                batch_size = 0;
                if (LOG.isDebugEnabled())
                    LOG.debug("Page  % " + batch_size);
            }
        } // FOR
        if (batch_size > 0) {
            pageInsert.executeBatch();
            this.conn.commit();
            pageInsert.clearBatch();
        }
        if (this.getDatabaseType() == DatabaseType.POSTGRES) {
            this.updateAutoIncrement(catalog_tbl.getColumn(0), this.num_pages);
        }
        if (LOG.isDebugEnabled())
            LOG.debug("Users  % " + this.num_pages);
    }

    /**
     * WATCHLIST
     */
    private void loadWatchlist() throws SQLException {
        Table catalog_tbl = this.getTableCatalog(WikipediaConstants.TABLENAME_WATCHLIST);
        assert(catalog_tbl != null);
        
        String sql = SQLUtil.getInsertSQL(catalog_tbl, 1);
        PreparedStatement watchInsert = this.conn.prepareStatement(sql);
        
        ZipfianGenerator zipPages = new ZipfianGenerator(this.num_pages);

        int batchSize = 0;
        for (int user_id = 1; user_id <= this.num_users; user_id++) {
            Pair<Integer, String> page = this.titles.get(zipPages.nextInt());
            
            int col = 1;
            watchInsert.setInt(col++, user_id); // wl_user
            watchInsert.setInt(col++, page.getFirst()); // wl_namespace
            watchInsert.setString(col++, page.getSecond()); // wl_title
            watchInsert.setNull(col++, java.sql.Types.VARCHAR); // wl_notificationtimestamp
            watchInsert.addBatch();

            if ((++batchSize % WikipediaConstants.BATCH_SIZE) == 0) {
                watchInsert.executeBatch();
                this.conn.commit();
                watchInsert.clearBatch();
                batchSize = 0;
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Watchlist  % " + (int) (((double) user_id / (double) this.num_users) * 100));
                }
            }
        } // FOR
        if (batchSize > 0) {
            watchInsert.executeBatch();
            watchInsert.clearBatch();
            this.conn.commit();
        }
        if (LOG.isDebugEnabled())
            LOG.debug("Watchlist Loaded");
    }

    /**
     * REVISIONS
     */
    private void loadRevision() throws SQLException {
        
        // TEXT
        Table textTable = this.getTableCatalog(WikipediaConstants.TABLENAME_TEXT);
        String textSQL = SQLUtil.getInsertSQL(textTable);
        PreparedStatement textInsert = this.conn.prepareStatement(textSQL);

        // REVISION
        Table revTable = this.getTableCatalog(WikipediaConstants.TABLENAME_REVISION);
        String revSQL = SQLUtil.getInsertSQL(revTable);
        PreparedStatement revisionInsert = this.conn.prepareStatement(revSQL);

        int batchSize = 1;
        ZipfianGenerator h_users = new ZipfianGenerator(this.num_users);
        FlatHistogram<Integer> h_textLength = new FlatHistogram<Integer>(this.rng(), TextHistograms.TEXT_LENGTH);
        FlatHistogram<Integer> h_commentLength = new FlatHistogram<Integer>(this.rng(), RevisionHistograms.COMMENT_LENGTH);
        FlatHistogram<Integer> h_minorEdit = new FlatHistogram<Integer>(this.rng(), RevisionHistograms.MINOR_EDIT);
        FlatHistogram<Integer> h_nameLength = new FlatHistogram<Integer>(this.rng(), UserHistograms.NAME_LENGTH);
        FlatHistogram<Integer> h_numRevisions = new FlatHistogram<Integer>(this.rng(), PageHistograms.REVISIONS_PER_PAGE);

        int rev_id = 1;
        for (int page_id = 1; page_id <= this.num_pages; page_id++) {
            // There must be at least one revision per page
            int num_revised = h_numRevisions.nextValue().intValue();
            
            // Generate what the new revision is going to be
            int old_text_length = h_textLength.nextValue().intValue();
            char old_text[] = TextGenerator.randomChars(rng(), old_text_length);
            
            for (int i = 0; i < num_revised; i++) {
                // Generate the User who's doing the revision and the Page revised
                // Makes sure that we always update their counter
                int user_id = h_users.nextInt() + 1;
                assert(user_id > 0 && user_id <= this.num_users) : "Invalid UserId '" + user_id + "'";
                this.user_revision_ctr[user_id-1]++;
                
                // Generate what the new revision is going to be
                if (i > 0) {
                    old_text_length = h_textLength.nextValue().intValue();
                    
                    // For now just make it a little bit bigger
                    old_text = TextGenerator.increaseText(rng(), old_text, rng().nextInt(100));
                    old_text_length = old_text.length;
                    
                    // And permute it a little bit. This ensures that the text is slightly
                    // different than the last revision
                    old_text = TextGenerator.permuteText(rng(), old_text);
                }
                
                char rev_comment[] = TextGenerator.randomChars(rng(), h_commentLength.nextValue().intValue());

                // The REV_USER_TEXT field is usually the username, but we'll just 
                // put in gibberish for now
                char user_text[] = TextGenerator.randomChars(rng(), h_nameLength.nextValue().intValue());
                
                // Insert the text
                int col = 1;
                textInsert.setInt(col++, rev_id); // old_id
                textInsert.setString(col++, new String(old_text)); // old_text
                textInsert.setString(col++, "utf-8"); // old_flags
                textInsert.setInt(col++, page_id); // old_page
                textInsert.addBatch();

                // Insert the revision
                col = 1;
                revisionInsert.setInt(col++, rev_id); // rev_id
                revisionInsert.setInt(col++, page_id); // rev_page
                revisionInsert.setInt(col++, rev_id); // rev_text_id
                revisionInsert.setString(col++, new String(rev_comment)); // rev_comment
                revisionInsert.setInt(col++, user_id); // rev_user
                revisionInsert.setString(col++, new String(user_text)); // rev_user_text
                revisionInsert.setString(col++, TimeUtil.getCurrentTimeString14()); // rev_timestamp
                revisionInsert.setInt(col++, h_minorEdit.nextValue().intValue()); // rev_minor_edit
                revisionInsert.setInt(col++, 0); // rev_deleted
                revisionInsert.setInt(col++, 0); // rev_len
                revisionInsert.setInt(col++, 0); // rev_parent_id
                revisionInsert.addBatch();
                
                // Update Last Revision Stuff
                this.page_last_rev_id[page_id-1] = rev_id;
                this.page_last_rev_length[page_id-1] = old_text_length;
                rev_id++;
                batchSize++;
            } // FOR (revision)
            if (batchSize > WikipediaConstants.BATCH_SIZE) {
                textInsert.executeBatch();
                revisionInsert.executeBatch();
                this.conn.commit();
                batchSize = 0;
            }
        } // FOR (page)
        
        // UPDATE USER
        revTable = this.getTableCatalog(WikipediaConstants.TABLENAME_USER);
        String updateUserSql = "UPDATE " + revTable.getEscapedName() + 
                               "   SET user_editcount = ?, " +
                               "       user_touched = ? " +
                               " WHERE user_id = ?";
        PreparedStatement userUpdate = this.conn.prepareStatement(updateUserSql);
        batchSize = 0;
        for (int i = 0; i < this.num_users; i++) {
            int col = 1;
            userUpdate.setInt(col++, this.user_revision_ctr[i]);
            userUpdate.setString(col++, TimeUtil.getCurrentTimeString14());
            userUpdate.setInt(col++, i+1); // ids start at 1
            userUpdate.addBatch();
            if ((++batchSize % WikipediaConstants.BATCH_SIZE) == 0) {
                userUpdate.executeBatch();
                this.conn.commit();
                userUpdate.clearBatch();
                batchSize = 0;
            }
        } // FOR
        if (batchSize > 0) {
            userUpdate.executeBatch();
            this.conn.commit();
            userUpdate.clearBatch();
        }
        if (this.getDatabaseType() == DatabaseType.POSTGRES) {
            this.updateAutoIncrement(textTable.getColumn(0), this.num_pages);
            this.updateAutoIncrement(revTable.getColumn(0), this.num_pages);
        }
        
        // UPDATE PAGES
        revTable = this.getTableCatalog(WikipediaConstants.TABLENAME_PAGE);
        String updatePageSql = "UPDATE " + revTable.getEscapedName() + 
                               "   SET page_latest = ?, " +
                               "       page_touched = ?, " +
                               "       page_is_new = 0, " +
                               "       page_is_redirect = 0, " +
                               "       page_len = ? " +
                               " WHERE page_id = ?";
        PreparedStatement pageUpdate = this.conn.prepareStatement(updatePageSql);
        batchSize = 0;
        for (int i = 0; i < this.num_pages; i++) {
            if (this.page_last_rev_id[i] == -1) continue;
            
            int col = 1;
            pageUpdate.setInt(col++, this.page_last_rev_id[i]);
            pageUpdate.setString(col++, TimeUtil.getCurrentTimeString14());
            pageUpdate.setInt(col++, this.page_last_rev_length[i]);
            pageUpdate.setInt(col++, i+1); // ids start at 1
            pageUpdate.addBatch();
            if ((++batchSize % WikipediaConstants.BATCH_SIZE) == 0) {
                pageUpdate.executeBatch();
                this.conn.commit();
                pageUpdate.clearBatch();
                batchSize = 0;
            }
        } // FOR
        if (batchSize > 0) {
            pageUpdate.executeBatch();
            this.conn.commit();
            pageUpdate.clearBatch();
        }
        
        if (LOG.isDebugEnabled()) {
            LOG.debug("Revision loaded");
        }
    }
}