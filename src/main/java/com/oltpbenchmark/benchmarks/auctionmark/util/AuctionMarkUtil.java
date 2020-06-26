/*
 * Copyright 2020 by OLTPBenchmark Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */


package com.oltpbenchmark.benchmarks.auctionmark.util;

import com.oltpbenchmark.benchmarks.auctionmark.AuctionMarkConstants;
import com.oltpbenchmark.benchmarks.auctionmark.AuctionMarkProfile;
import com.oltpbenchmark.util.FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;

public abstract class AuctionMarkUtil {
    private static final Logger LOG = LoggerFactory.getLogger(AuctionMarkUtil.class);

    public static File getDataDirectory() {
        File dataDir = null;

        // If we weren't given a path, then we need to look for the tests directory and
        // then walk our way up the tree to get to our benchmark's directory
        try {
            File tests_dir = FileUtil.findDirectory("tests");


            dataDir = new File(tests_dir.getAbsolutePath() + File.separator + "frontend" + File.separator +
                    AuctionMarkProfile.class.getPackage().getName().replace('.', File.separatorChar) +
                    File.separator + "data").getCanonicalFile();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Default data directory path = {}", dataDir);
            }
            if (!dataDir.exists()) {
                throw new RuntimeException("The default data directory " + dataDir + " does not exist");
            } else if (!dataDir.isDirectory()) {
                throw new RuntimeException("The default data path " + dataDir + " is not a directory");
            }
        } catch (IOException ex) {
            throw new RuntimeException("Unexpected error", ex);
        }
        return (dataDir);
    }

    private static final long ITEM_ID_MASK = 0xFFFFFFFFFFFFFFL; // 56 bits (ITEM_ID)

    /**
     * @param item_id
     * @param idx
     * @return
     */
    public static long getUniqueElementId(long item_id, int idx) {
        // The idx cannot be more than 7bits
        return (((long) idx << 56) | (item_id & ITEM_ID_MASK));
    }

    /**
     * @param benchmarkTimes
     * @return
     */
    public static Timestamp getProcTimestamp(Timestamp[] benchmarkTimes) {


        Timestamp tmp = new Timestamp(System.currentTimeMillis());
        long timestamp = getScaledTimestamp(benchmarkTimes[0], benchmarkTimes[1], tmp);
        tmp.setTime(timestamp);

        return (tmp);
    }

    /**
     * @param benchmarkStart
     * @param clientStart
     * @param current
     * @return
     */
    public static long getScaledTimestamp(Timestamp benchmarkStart, Timestamp clientStart, Timestamp current) {
        // First get the offset between the benchmarkStart and the clientStart
        // We then subtract that value from the current time. This gives us the total elapsed 
        // time from the current time to the time that the benchmark start (with the gap 
        // from when the benchmark was loading data cut out) 
        long base = benchmarkStart.getTime();
        long offset = current.getTime() - (clientStart.getTime() - base);
        long elapsed = (offset - base) * AuctionMarkConstants.TIME_SCALE_FACTOR;
        return (base + elapsed);
    }

}
