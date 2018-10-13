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

import static com.github.garychenc.filemap.util.Asserts.longIsPositive;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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
public class ContentStoreSlaveIndex implements Comparable <ContentStoreSlaveIndex> {

    private final static Logger LOGGER = LoggerFactory.getLogger(ContentStoreSlaveIndex.class);

    public final static int STORE_SLAVE_INDEX_BYTES_LENGTH = 21;
    public final static long NULL_STORE_SIZE = -1;
    public final static int  NULL_FILE_NUMBER = -1;
    public final static long NULL_STORE_START_ADDRESS = -1;
    public final static long TOTALLY_REMOVED_STORE_SIZE = -21;
    public final static byte[] NULL_STORE_SLAVE_INDEX_BYTES;

    static {
        ByteArrayOutputStream outputstream = null;
        try {
            outputstream = new ByteArrayOutputStream();
            IOHelper.writeBoolean(outputstream, false);
            IOHelper.write64bit(outputstream, NULL_STORE_SIZE);
            IOHelper.write32bit(outputstream, NULL_FILE_NUMBER);
            IOHelper.write64bit(outputstream, NULL_STORE_START_ADDRESS);
            outputstream.flush();
            NULL_STORE_SLAVE_INDEX_BYTES = outputstream.toByteArray();
        } catch (Throwable ex) {
            throw new IllegalStateException("Write null store slave index bytes failed.", ex);
        } finally {
            if (outputstream != null) {
                try { outputstream.close(); } catch (IOException exce) { }
            }
        }
    }

    //cache line padding 1
    private long p1, p2, p3, p4, p5, p6, p7, p8;

    private volatile boolean removed;
    private volatile long storeSize;
    private volatile int fileNumber;
    private volatile long storeStartAddress;
    private volatile long slaveNumber;

    //cache line padding 2
    private long p9, p10, p11, p12, p13, p14, p15, p16;

    public ContentStoreSlaveIndex(boolean removed, long storeSize, long slaveNumber, int fileNumber, long storeStartAddress) {
        longIsPositive("slaveNumber", slaveNumber);
        if (storeSize < 0 && storeSize != TOTALLY_REMOVED_STORE_SIZE) {
            throw new IllegalArgumentException("storeSize error! storeSize : " + storeSize);
        }

        this.removed = removed;
        this.storeSize = storeSize;
        this.fileNumber = fileNumber;
        this.storeStartAddress = storeStartAddress;
        this.slaveNumber = slaveNumber;
    }

    public boolean isRemoved() {
        return removed;
    }

    public void setRemoved(boolean removed) {
        this.removed = removed;
    }

    public long getStoreSize() {
        return storeSize;
    }

    public void setStoreSize(long storeSize) {
        if (storeSize < 0 && storeSize != TOTALLY_REMOVED_STORE_SIZE) {
            throw new IllegalArgumentException("storeSize error! storeSize : " + storeSize);
        }

        this.storeSize = storeSize;
    }

    public long getStoreStartAddress() {
        return storeStartAddress;
    }

    public void setStoreStartAddress(long storeStartAddress) {
        this.storeStartAddress = storeStartAddress;
    }

    public int getFileNumber() {
        return fileNumber;
    }

    public void setFileNumber(int fileNumber) {
        this.fileNumber = fileNumber;
    }

    public long getSlaveNumber() {
        return slaveNumber;
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
    public int compareTo(ContentStoreSlaveIndex another) {
        if (this.getStoreSize() > another.getStoreSize()) {
            return 1;
        } else if (this.getStoreSize() < another.getStoreSize()) {
            return -1;
        } else {
            return 0;
        }
    }

    public static ContentStoreSlaveIndex fromBytes(byte[] bytes, long slaveNumber, boolean ignoreErrorEntry) throws IOException {
        if (ignoreErrorEntry) {
            if (bytes == null || bytes.length != STORE_SLAVE_INDEX_BYTES_LENGTH) {
                return new ContentStoreSlaveIndex(true, TOTALLY_REMOVED_STORE_SIZE, slaveNumber, NULL_FILE_NUMBER, NULL_STORE_START_ADDRESS);
            }

            ByteArrayInputStream inputstream = null;
            try {
                inputstream = new ByteArrayInputStream(bytes);
                boolean removed = IOHelper.readBoolean(inputstream);
                long size = IOHelper.read64bit(inputstream);
                int fileNumber = IOHelper.read32bit(inputstream);
                long startAddress = IOHelper.read64bit(inputstream);
                if (size == NULL_STORE_SIZE) {
                    return null;
                } else {
                    return new ContentStoreSlaveIndex(removed, size, slaveNumber, fileNumber, startAddress);
                }
            } catch (Throwable ex) {
                LOGGER.error("Read ContentStoreSlaveIndex failed. slaveNumber : " + slaveNumber, ex);
                return new ContentStoreSlaveIndex(true, TOTALLY_REMOVED_STORE_SIZE, slaveNumber, NULL_FILE_NUMBER, NULL_STORE_START_ADDRESS);
            } finally {
                Utils.closeInputStream(inputstream);
            }
        } else {
            if (bytes == null || bytes.length != STORE_SLAVE_INDEX_BYTES_LENGTH) {
                throw new IllegalArgumentException("Bytes length error. Expect length : " + STORE_SLAVE_INDEX_BYTES_LENGTH);
            }

            ByteArrayInputStream inputstream = null;
            try {
                inputstream = new ByteArrayInputStream(bytes);
                boolean removed = IOHelper.readBoolean(inputstream);
                long size = IOHelper.read64bit(inputstream);
                int fileNumber = IOHelper.read32bit(inputstream);
                long startAddress = IOHelper.read64bit(inputstream);
                if (size == NULL_STORE_SIZE) {
                    return null;
                } else {
                    return new ContentStoreSlaveIndex(removed, size, slaveNumber, fileNumber, startAddress);
                }
            } finally {
                Utils.closeInputStream(inputstream);
            }
        }
    }

    public static byte[] toBytes(ContentStoreSlaveIndex slaveIndex) throws IOException {
        try (ByteArrayOutputStream outputstream = new ByteArrayOutputStream(IOHelper.BUFFER_DEFAULT_ALLOCATED_SIZE)) {
            if (slaveIndex == null) {
                IOHelper.writeBoolean(outputstream, false);
                IOHelper.write64bit(outputstream, NULL_STORE_SIZE);
                IOHelper.write32bit(outputstream, NULL_FILE_NUMBER);
                IOHelper.write64bit(outputstream, NULL_STORE_START_ADDRESS);
            } else {
                IOHelper.writeBoolean(outputstream, slaveIndex.isRemoved());
                IOHelper.write64bit(outputstream, slaveIndex.getStoreSize());
                IOHelper.write32bit(outputstream, slaveIndex.getFileNumber());
                IOHelper.write64bit(outputstream, slaveIndex.getStoreStartAddress());
            }

            outputstream.flush();
            return outputstream.toByteArray();
        }
    }

}
