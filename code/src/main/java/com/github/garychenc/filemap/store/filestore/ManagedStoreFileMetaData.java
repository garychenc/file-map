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
package com.github.garychenc.filemap.store.filestore;

import static com.github.garychenc.filemap.util.Asserts.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import com.github.garychenc.filemap.util.IOHelper;
import com.github.garychenc.filemap.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Gary CHEN
 *
 */
public class ManagedStoreFileMetaData implements Comparable <ManagedStoreFileMetaData> {

    private final static Logger LOGGER = LoggerFactory.getLogger(ManagedStoreFileMetaData.class);
    
    public final static int MANAGED_FILE_META_BYTES_LENGTH = 16;
    public final static long NULL_FILE_SIZE = -1;
    public final static long TOTALLY_REMOVED_FILE_SIZE = -16;
    public final static byte[] NULL_MANAGED_FILE_META_BYTES;
    
    static {
        ByteArrayOutputStream outputstream = null;
        try {
            outputstream = new ByteArrayOutputStream();
            IOHelper.write64bit(outputstream, NULL_FILE_SIZE);
            IOHelper.write64bit(outputstream, NULL_FILE_SIZE);
            outputstream.flush();
            NULL_MANAGED_FILE_META_BYTES = outputstream.toByteArray();
        } catch (Throwable ex) {
            throw new IllegalStateException("Write null managed file meta bytes failed.", ex);
        } finally {
            if (outputstream != null) {
                try { outputstream.close(); } catch (IOException exce) { }
            }
        }
    }

    //cache line padding 1
    private long p1, p2, p3, p4, p5, p6, p7, p8;

    private final long fileNumber;
    private volatile long fileTotalSize;
    private long fileUsedSize;

    //cache line padding 2
    private long p9, p10, p11, p12, p13, p14, p15, p16;

    public ManagedStoreFileMetaData(long fileNumber, long fileTotalSize, long fileUsedSize) {
        longIsPositive("fileNumber", fileNumber);       
                
        this.fileNumber = fileNumber;
        this.fileTotalSize = fileTotalSize;
        this.fileUsedSize = fileUsedSize;
    }
    
    public ManagedStoreFileMetaData(int fileNumber, long fileTotalSize) {
        this(fileNumber, fileTotalSize, 0);
    }

    public long getFileNumber() {
        return fileNumber;
    }

    public long getFileTotalSize() {
        return fileTotalSize;
    }

    public void setFileTotalSize(long fileTotalSize) {
        this.fileTotalSize = fileTotalSize;
    }
    
    public synchronized long getFileUsedSize() {
        return fileUsedSize;
    }
    
    public synchronized void resetFileUsedSize() {
        fileUsedSize = 0;
    }

    public synchronized long getCurrentStartAddress() {
        return fileUsedSize;
    }
    
    public synchronized long increaseUsedSize(long delta) {
        fileUsedSize = fileUsedSize + delta;
        return fileUsedSize;
    }
    
    public long getFileSpareSize() {
        return getFileTotalSize() - getFileUsedSize();
    }

    public long getP1() {
        return p1;
    }

    public long getP2() {
        return p2;
    }

    public long getP3() {
        return p3;
    }

    public long getP4() {
        return p4;
    }

    public long getP5() {
        return p5;
    }

    public long getP6() {
        return p6;
    }

    public long getP7() {
        return p7;
    }

    public long getP8() {
        return p8;
    }

    public long getP9() {
        return p9;
    }

    public long getP10() {
        return p10;
    }

    public long getP11() {
        return p11;
    }

    public long getP12() {
        return p12;
    }

    public long getP13() {
        return p13;
    }

    public long getP14() {
        return p14;
    }

    public long getP15() {
        return p15;
    }

    public long getP16() {
        return p16;
    }

    @Override
    public int compareTo(ManagedStoreFileMetaData another) {
        if (this.getFileSpareSize() > another.getFileSpareSize()) {
            return 1;
        } else if (this.getFileSpareSize() < another.getFileSpareSize()) {
            return -1;
        } else {
            return 0;
        }
    }
    
    
    public static ManagedStoreFileMetaData fromBytes(byte[] bytes, long fileNumber, boolean ignoreErrorEntry) throws IOException {
        if (ignoreErrorEntry) {
            if (bytes == null || bytes.length != MANAGED_FILE_META_BYTES_LENGTH) {
                return new ManagedStoreFileMetaData(fileNumber, TOTALLY_REMOVED_FILE_SIZE, 0);
            }

            ByteArrayInputStream inputstream = null;
            try {
                inputstream = new ByteArrayInputStream(bytes);
                long totalSize = IOHelper.read64bit(inputstream);
                long usedSize = IOHelper.read64bit(inputstream);
                if (totalSize == NULL_FILE_SIZE || usedSize == NULL_FILE_SIZE) {
                    return null;
                } else {
                    return new ManagedStoreFileMetaData(fileNumber, totalSize, usedSize);
                }            
            } catch (Throwable ex) {
                LOGGER.error("Read ManagedStoreFileMetaData failed. fileNumber : " + fileNumber, ex);
                return new ManagedStoreFileMetaData(fileNumber, TOTALLY_REMOVED_FILE_SIZE, 0);
            } finally {
                Utils.closeInputStream(inputstream);
            }
        } else {
            if (bytes == null || bytes.length != MANAGED_FILE_META_BYTES_LENGTH) {
                throw new IllegalArgumentException("Bytes length error. Expect length : " + MANAGED_FILE_META_BYTES_LENGTH);
            }

            ByteArrayInputStream inputstream = null;
            try {
                inputstream = new ByteArrayInputStream(bytes);
                long totalSize = IOHelper.read64bit(inputstream);
                long usedSize = IOHelper.read64bit(inputstream);
                if (totalSize == NULL_FILE_SIZE || usedSize == NULL_FILE_SIZE) {
                    return null;
                } else {
                    return new ManagedStoreFileMetaData(fileNumber, totalSize, usedSize);
                }            
            } finally {
                Utils.closeInputStream(inputstream);
            }
        }
    }
    
    public static byte[] toBytes(ManagedStoreFileMetaData fileMeta) throws IOException {
        try (ByteArrayOutputStream outputstream = new ByteArrayOutputStream(IOHelper.BUFFER_DEFAULT_ALLOCATED_SIZE)) {
            if (fileMeta == null) {
                IOHelper.write64bit(outputstream, NULL_FILE_SIZE);
                IOHelper.write64bit(outputstream, NULL_FILE_SIZE);
            } else {
                IOHelper.write64bit(outputstream, fileMeta.getFileTotalSize());
                IOHelper.write64bit(outputstream, fileMeta.getFileUsedSize());
            }
            
            outputstream.flush();
            return outputstream.toByteArray();
        }
    }

}
