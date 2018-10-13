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

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Gary CHEN
 *
 */
class ContentStoreSlaveStoreFile {
    
    private final static Logger LOGGER = LoggerFactory.getLogger(ContentStoreSlaveStoreFile.class);

    //cache line padding 1
    private long p1, p2, p3, p4, p5, p6, p7, p8;

    private final ManagedStoreFileMetaData managedFileMeta;
    private volatile File slaveStoreFilePath;
    private volatile RandomAccessFile slaveStoreFile;
    private volatile FileChannel slaveStoreFileChannel;
    private final List<ContentStoreSlaveIndex> slavesInThisFile;
    private int slavesInThisFileNumber;

    //cache line padding 2
    private long p9, p10, p11, p12, p13, p14, p15, p16;

    ContentStoreSlaveStoreFile(ManagedStoreFileMetaData managedFileMeta) {
        this.managedFileMeta = managedFileMeta;
        this.slavesInThisFile = new LinkedList<ContentStoreSlaveIndex>();
        this.slavesInThisFileNumber = 0;
    }
    
    ManagedStoreFileMetaData getManagedFileMeta() {
        return managedFileMeta;
    }

    File getSlaveStoreFilePath() {
        return slaveStoreFilePath;
    }

    void setSlaveStoreFilePath(File slaveStoreFilePath) {
        this.slaveStoreFilePath = slaveStoreFilePath;
    }

    RandomAccessFile getSlaveStoreFile() {
        return slaveStoreFile;
    }

    void setSlaveStoreFile(RandomAccessFile slaveStoreFile) {
        this.slaveStoreFile = slaveStoreFile;
        this.slaveStoreFileChannel = slaveStoreFile.getChannel();
    }

    FileChannel getSlaveStoreFileChannel() {
        return slaveStoreFileChannel;
    }

    List<ContentStoreSlaveIndex> getSlavesInThisFile() {
        return Collections.unmodifiableList(slavesInThisFile);
    }

    boolean addSlaveIndexInThisFile(ContentStoreSlaveIndex e) {
        return slavesInThisFile.add(e);
    }

    void clearSlavesInThisFile() {
        slavesInThisFile.clear();
    }

    synchronized int getSlavesInThisFileNumber() {
        return slavesInThisFileNumber;
    }

    synchronized int incrementAndGetSlavesInThisFileNumber() {
        ++slavesInThisFileNumber;
        return slavesInThisFileNumber;
    }

    synchronized void resetSlavesInThisFileNumber() {
        slavesInThisFileNumber = 0;
    }

    synchronized int decrementAndGetSlavesInThisFileNumber() {
        --slavesInThisFileNumber;
        return slavesInThisFileNumber;
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

    void closeFile() {
        try {
            slaveStoreFileChannel.force(true);
        } catch (Throwable ex) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Force write content to persistence file failed. File path:  " + slaveStoreFilePath.getAbsolutePath() + ".", ex);
            }
        }
        
        try {
            slaveStoreFileChannel.close();
        } catch (Throwable ex) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Close persistence file channel failed. File path:  " + slaveStoreFilePath.getAbsolutePath() + ".", ex);
            } 
        }
        
        try {
            slaveStoreFile.close();
        } catch (Throwable ex) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Close persistence file failed. File path:  " + slaveStoreFilePath.getAbsolutePath() + ".", ex);
            } 
        }
    }
}






























































