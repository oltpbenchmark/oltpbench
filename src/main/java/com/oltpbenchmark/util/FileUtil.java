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


package com.oltpbenchmark.util;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * @author pavlo
 */
public abstract class FileUtil {

    /**
     * Join path components
     *
     * @param args
     * @return
     */
    public static String joinPath(String... args) {
        StringBuilder result = new StringBuilder();
        boolean first = true;
        for (String a : args) {
            if (a != null && a.length() > 0) {
                if (!first) {
                    result.append("/");
                }
                result.append(a);
                first = false;
            }
        }
        return result.toString();
    }

    public static boolean exists(String path) {
        return (new File(path).exists());
    }

    /**
     * Create any directory in the list paths if it doesn't exist
     *
     * @param paths
     */
    public static void makeDirIfNotExists(String... paths) {
        for (String p : paths) {
            if (p == null) {
                continue;
            }
            File f = new File(p);
            if (!f.exists()) {
                f.mkdirs();
            }
        }
    }

    public static void writeStringToFile(File file, String content) throws IOException {
        try (FileWriter writer = new FileWriter(file)) {
            writer.write(content);
            writer.flush();
        }
    }

    /**
     * Write the given string to a temporary file with the given extension as
     * the suffix Will not delete the file after the JVM exits
     *
     * @param content
     * @param ext
     * @return
     */
    public static File writeStringToTempFile(String content, String ext) {
        return (writeStringToTempFile(content, ext, false));
    }

    /**
     * Write the given string to a temporary file with the given extension as
     * the suffix If deleteOnExit is true, then the file will be removed when
     * the JVM exits
     *
     * @param content
     * @param ext
     * @param deleteOnExit
     * @return
     */
    public static File writeStringToTempFile(String content, String ext, boolean deleteOnExit) {
        File tempFile = FileUtil.getTempFile(ext, deleteOnExit);
        try {
            FileUtil.writeStringToFile(tempFile, content);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return tempFile;
    }

    /**
     * Return a File handle to a temporary file location
     *
     * @param ext
     *            the suffix of the filename
     * @param deleteOnExit
     *            whether to delete this file after the JVM exits
     * @return
     */
    public static File getTempFile(String ext, boolean deleteOnExit) {
        return getTempFile(null, ext, deleteOnExit);
    }

    public static File getTempFile(String prefix, String suffix, boolean deleteOnExit) {
        File tempFile;
        if (suffix != null && suffix.startsWith(".") == false)
            suffix = "." + suffix;
        if (prefix == null)
            prefix = "hstore";

        try {
            tempFile = File.createTempFile(prefix, suffix);
            if (deleteOnExit)
                tempFile.deleteOnExit();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        return (tempFile);
    }

}
