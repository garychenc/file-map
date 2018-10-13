/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.garychenc.filemap.util;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.io.Reader;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.util.Set;

/**
 *
 * @author Gary CHEN
 *
 */
public final class Utils {
    
    private Utils() {
    }
    
    private static final String[] ZERO_ARRAY = new String[0];
    public  static final String DEFAULT_SPLITTER = ",";
    public  static final String[] splitString2StringArray(String target) {
        return splitString2StringArray(target, DEFAULT_SPLITTER);
    }
    
    public  static final String[] splitString2StringArray(String target, String spliter) {
        if (target == null || target.trim().length() == 0 ||
            spliter == null || spliter.trim().length() == 0) {
            return ZERO_ARRAY;
        } else {
            String[] array = target.split(spliter);
            if (array == null) {
                array = ZERO_ARRAY;
            }
            
            for (int i = 0; i < array.length; ++i) {
                if (array[i] != null) {
                    array[i] = array[i].trim();
                }
            }
            
            return array;
        }
    }
    
    public  static final String joinStringArray2String(Object[] target, String spliter) {
        if (target == null || target.length == 0) {
            return "";
        }
        
        if (spliter == null || spliter.trim().length() == 0) {
            spliter = DEFAULT_SPLITTER;
        }
        
        StringBuilder ret = new StringBuilder();
        for (Object obj : target) {
            if (obj != null) {
                ret.append(obj.toString()).append(spliter);
            }            
        }
        
        ret.delete(ret.length() - spliter.length(), ret.length());
        return ret.toString();
    }
    
    public  static final String joinStringArray2String(Object[] target) {
        return joinStringArray2String(target, DEFAULT_SPLITTER);
    }
    
    public  static final String joinStringSet2String(Set<String> target, String spliter) {
        if (target == null) {
            return "";
        }
        
        return joinStringArray2String(target.toArray(), spliter);
    }
    
    public  static final String joinStringSet2String(Set<String> target) {
        return joinStringSet2String(target, DEFAULT_SPLITTER);
    }
    
    public static byte[] readFileContentAsBytes(String path) throws IOException {
        return readFileContentAsBytes(path, 1024 * 8);
    }
    
    public static final byte[] readFileContentAsBytes(String path, int bufferSize) throws IOException {
        File p = new File(path);
        return readFileContentAsBytes(p, bufferSize);
    }
    
    public static final byte[] readFileContentAsBytes(File path) throws IOException {
        return readFileContentAsBytes(path, 1024 * 8);
    }
    
    public static final byte[] readFileContentAsBytes(File path, int bufferSize) throws IOException {
        if (path == null) {
            throw new IllegalArgumentException("File path to be read can not be null.");
        }
        
        if (!path.exists()) {
            throw new IllegalArgumentException("The path is not existent in file system. Path: " + path.getAbsolutePath() + ".");
        }
        
        if (!path.isFile()) {
            throw new IllegalArgumentException("The path is not a file. Path: " + path.getAbsolutePath() + ".");
        }        
        
        if (bufferSize < 1024) {
            bufferSize = 1024;
        }
        
        FileInputStream fileInput = null;
        BufferedInputStream bufferedInput = null;
        ByteArrayOutputStream bytesOutput = null;
        try {
            fileInput = new FileInputStream(path);
            bufferedInput = new BufferedInputStream(fileInput, 5 * bufferSize);
            bytesOutput = new ByteArrayOutputStream(5 * bufferSize);
            byte[] buffer = new byte[bufferSize];
            int length = -1;
            while ((length = bufferedInput.read(buffer)) != -1) {
                bytesOutput.write(buffer, 0, length);
            }
            bytesOutput.flush();
            return bytesOutput.toByteArray();
        } finally {
            closeOutputStream(bytesOutput);
            closeInputStream(bufferedInput);
            closeInputStream(fileInput);
        }
    }
    
    public static final File getOrCreateDir(String path) {
        Asserts.stringNotEmpty("path", path);

        File dir = new File(path);
        return getOrCreateDir(dir);
    }
    
    public static final File getOrCreateDir(String parentPath, String childPath) {
        Asserts.stringNotEmpty("parentPath", parentPath);
        Asserts.stringNotEmpty("childPath", childPath);
        File parentFile = getOrCreateDir(parentPath);
        File childFile = new File(parentFile, childPath);
        return getOrCreateDir(childFile);
    }

    public static final File getOrCreateDir(File dir) {
        Asserts.notNull("dir", dir);

        if (!dir.exists() && !dir.mkdirs()) {
            throw new IllegalStateException("Carete directories for path: [" + dir.getAbsolutePath() + "] failed.");
        }
        
        if (!dir.isDirectory()) {
            throw new IllegalStateException("The path: [" + dir.getAbsolutePath() + "] is not a directory.");
        }
        
        return dir;
    }
        
    public static final ClassLoader retrieveClassLoader() {
        ClassLoader cl = Utils.class.getClassLoader();
        
        if (cl == null) {
            throw new IllegalStateException("Can not get a ClassLoader to load classes.");
        }
        
        return cl;
    }
        
    public static final void closeRandomAccessFile(RandomAccessFile accessFile) {
        if (accessFile != null) {
            try { accessFile.close(); } catch (Throwable ex) {}
        }
    }

    public static final void closeOutputStream(OutputStream out) {
        if (out != null) {
            try { out.close(); } catch (Throwable ex) {}
        }        
    }
    
    public static final void closeInputStream(InputStream in) {
        if (in != null) {
            try { in.close(); } catch (Throwable ex) {}
        }
    }
    
    public static final void closeReader(Reader reader) {
        if (reader != null) {
            try { reader.close(); } catch (Throwable ex) {}
        }
    }
    
    public static final void closeWriter(Writer writer) {
        if (writer != null) {
            try { writer.close(); } catch (Throwable ex) {}
        }
    }
    
    public static final void disconnectUrlConnection(URLConnection conn) {
        if (conn instanceof HttpURLConnection) {
            try { ((HttpURLConnection)conn).disconnect(); } catch (Throwable ex) {}
        }
    }
    
    public static final boolean isUrlHttpCompatible(URL url) {
        assert url != null;
        return "http".equalsIgnoreCase(url.getProtocol().trim());
    }
    
    public static final void closeIOResource(InputStream in, OutputStream out, 
            Reader reader, Writer writer) {
        closeInputStream(in);
        closeOutputStream(out);
        closeReader(reader);
        closeWriter(writer);
    }
    
    public static final void closeIOResource(InputStream in, OutputStream out, 
            Reader reader) {
        closeInputStream(in);
        closeOutputStream(out);
        closeReader(reader);
    }
    
    public static final void closeIOResource(InputStream in, OutputStream out) {
        closeInputStream(in);
        closeOutputStream(out);
    }
    
    public static final void closeIOResource(Reader reader, Writer writer) {
        closeReader(reader);
        closeWriter(writer);
    }
    
    public static final void closeIOResource(InputStream in, OutputStream out, 
            Writer writer) {
        closeInputStream(in);
        closeOutputStream(out);
        closeWriter(writer);
    }

    public static final void closeIOResource(InputStream in, Reader reader) {
        closeInputStream(in);
        closeReader(reader);
    }
    
    public static final void closeIOResource(OutputStream out, Writer writer) {
        closeOutputStream(out);
        closeWriter(writer);
    }
    
    public static final void closeIOResource(InputStream in, Reader reader, Writer writer) {
        closeInputStream(in);
        closeReader(reader);
        closeWriter(writer);
    }
    
    public static final void closeIOResource(OutputStream out, Reader reader, Writer writer) {
        closeOutputStream(out);
        closeReader(reader);
        closeWriter(writer);
    }
    
    public static final void closeIOResource(InputStream in, Writer writer) {
        closeInputStream(in);
        closeWriter(writer);
    }
    
    public static final void closeIOResource(OutputStream out, Reader reader) {
        closeOutputStream(out);
        closeReader(reader);
    }
    
    public static final void deleteTheWholeDirectory(File dir) throws DeleteDirectoryContentsFailedException {
        deleteTheWholeDirectory(dir, null);
    }
    
    public static final void deleteTheWholeDirectory(File dir, FileFilter filter) throws DeleteDirectoryContentsFailedException {
        Asserts.notNull("directory", dir);
        
        if (!dir.isDirectory()) {
            throw new IllegalArgumentException("Not a directory.");
        }
        
        File[] files = dir.listFiles(filter);
        if (files == null || files.length == 0) {
            if (!dir.delete()) {
                throw new DeleteDirectoryContentsFailedException("Path: " + dir.getAbsolutePath());
            }
        } else {
            for (File file : files) {
                if (file != null) {
                    if (file.isDirectory()) {
                        deleteTheWholeDirectory(file, filter);
                    } else {
                        if (!file.delete()) {
                            throw new DeleteDirectoryContentsFailedException("Path: " + file.getAbsolutePath());
                        }
                    }
                }
            }
            
            if (!dir.delete()) {
                throw new DeleteDirectoryContentsFailedException("Path: " + dir.getAbsolutePath());
            }
        }
    }

    /**
     * Copies the specified range of the specified array into a new array.
     * The initial index of the range (<tt>from</tt>) must lie between zero
     * and <tt>original.length</tt>, inclusive.  The value at
     * <tt>original[from]</tt> is placed into the initial element of the copy
     * (unless <tt>from == original.length</tt> or <tt>from == to</tt>).
     * Values from subsequent elements in the original array are placed into
     * subsequent elements in the copy.  The final index of the range
     * (<tt>to</tt>), which must be greater than or equal to <tt>from</tt>,
     * may be greater than <tt>original.length</tt>, in which case
     * <tt>(byte)0</tt> is placed in all elements of the copy whose index is
     * greater than or equal to <tt>original.length - from</tt>.  The length
     * of the returned array will be <tt>to - from</tt>.
     *
     * @param original the array from which a range is to be copied
     * @param from the initial index of the range to be copied, inclusive
     * @param to the final index of the range to be copied, exclusive.
     *     (This index may lie outside the array.)
     * @return a new array containing the specified range from the original array,
     *     truncated or padded with zeros to obtain the required length
     * @throws ArrayIndexOutOfBoundsException if <tt>from &lt; 0</tt>
     *     or <tt>from &gt; original.length()</tt>
     * @throws IllegalArgumentException if <tt>from &gt; to</tt>
     * @throws NullPointerException if <tt>original</tt> is null
     * @since 1.6
     */
    public static byte[] copyOfRange(byte[] original, int from, int to) {
        int newLength = to - from;
        if (newLength < 0)
            throw new IllegalArgumentException(from + " > " + to);
        byte[] copy = new byte[newLength];
        System.arraycopy(original, from, copy, 0,
                         Math.min(original.length - from, newLength));
        return copy;
    }
    
    private static final ObjectCache<String, Class<Object>> clazzCache = (new SimpleObjectCacheFactoryImpl(256)).createObjectCache();
    
    @SuppressWarnings("unchecked")
    public static final Object createObject(String className) throws Exception {
        Asserts.stringNotEmpty("className", className);
        Class<Object> clazz = clazzCache.get(className);
        if (clazz == null) {
            ClassLoader cl = retrieveClassLoader();
            clazz = (Class<Object>) cl.loadClass(className);
        }
        
        Object obj = clazz.newInstance();
        if (clazzCache.get(className) == null) {
            synchronized (Utils.class) {
                if (clazzCache.get(className) == null) {
                    clazzCache.put(className, clazz);
                }
            }
        }
        
        return obj;
    }
    
    public static final int combine4Bytes2Int(int byte1, int byte2, int byte3, int byte4) {
        return (((byte1 << 24) & 0xFF000000) | 
                ((byte2 << 16) & 0x00FF0000) |
                ((byte3 <<  8) & 0x0000FF00) |
                 (byte4        & 0x000000FF));
    }
    
}






































