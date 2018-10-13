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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.github.garychenc.filemap.util.Asserts;
import com.github.garychenc.filemap.util.IOHelper;
import com.github.garychenc.filemap.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Gary CHEN
 *
 */
public class FileBaseContentStoreMaster implements ContentStoreMaster {

    private final static Logger LOGGER = LoggerFactory.getLogger(FileBaseContentStoreMaster.class);
    private final static String SLAVES_MANAGEMENT_FILE_POSTFIX = "-slaves.management";
    private final static long SLAVES_MANAGEMENT_FILE_MAGIC_NUMBER = 0x1317EA5;
    private final static long SLAVES_MANAGEMENT_FILE_STORE_START_ADDRESS = 20;
    
    private final static String STORE_FILES_MANAGEMENT_FILE_POSTFIX = "-store-files.management";
    private final static long STORE_FILES_MANAGEMENT_FILE_MAGIC_NUMBER = 0x131CCC5;
    private final static long STORE_FILES_MANAGEMENT_FILE_START_ADDRESS = 20;
    
    private final static int MASTER_CLASS_VERSION = 1;
    
    private final static String SLAVE_DATA_STORE_FILE_POSTFIX = "-slave.data-";
    
    private final File storeDirectory;
    private final String storeName;
    private final long individualSlaveStoreFileSize;
    
    private final File slavesManagementFilePath;    
    private final RandomAccessFile slavesManagementFile;
    private final FileChannel slavesManagementFileChannel;
    
    private final File storeFilesManagementFilePath;    
    private final RandomAccessFile storeFilesManagementFile;
    private final FileChannel storeFilesManagementFileChannel;
    
    private final boolean forceWriteData;
    private final boolean forceWriteMetaData;
    private long maxSlaveNumber;

    private final ReadWriteLock masterLock;
    private volatile boolean closed = false;
    private final Condition masterClosed;
    private final long dataStoreFilesGCInterval;
    
    private Map<Long, FileBaseContentStoreSlave> slaves;
    private AtomicLong slavesNumber;
    private LinkedList<ContentStoreSlaveIndex> removedSortedSlaves;
    private volatile boolean needSortRemovedSlaves = false;
    
    private Map<Long, ContentStoreSlaveStoreFile> slaveStoreFiles;
    private LinkedList<ManagedStoreFileMetaData> sortedManagedFiles;

    public FileBaseContentStoreMaster(String storeDirPath, String storeName, boolean forceWriteData, long maxSlaveNumber,
                                      long individualSlaveStoreFileSize, boolean forceWriteMetaData, long dataStoreFilesGCInterval, boolean ignoreErrorEntry) {
        Asserts.stringNotEmpty("storeDirPath", storeDirPath);
        Asserts.stringNotEmpty("storeName", storeName);
        Asserts.longIsPositive("maxSlaveNumber", maxSlaveNumber);
        Asserts.longIsPositive("individualSlaveStoreFileSize", individualSlaveStoreFileSize);
        Asserts.longIsPositive("dataStoreFilesGCInterval", dataStoreFilesGCInterval);
        
        this.storeDirectory = Utils.getOrCreateDir(storeDirPath, storeName);
        this.storeName = storeName;
        this.individualSlaveStoreFileSize = individualSlaveStoreFileSize;
        this.dataStoreFilesGCInterval = dataStoreFilesGCInterval;
        this.slavesManagementFilePath = new File(storeDirectory, storeName + SLAVES_MANAGEMENT_FILE_POSTFIX);
        this.storeFilesManagementFilePath = new File(storeDirectory, storeName + STORE_FILES_MANAGEMENT_FILE_POSTFIX);
        
        try {
            this.slavesManagementFile = new RandomAccessFile(slavesManagementFilePath, "rw");
        } catch (FileNotFoundException ex) {
            throw new IllegalStateException("Can not open or create local file : " + slavesManagementFilePath.getAbsolutePath() + ".", ex);
        }
        
        try {
            this.storeFilesManagementFile = new RandomAccessFile(storeFilesManagementFilePath, "rw");
        } catch (FileNotFoundException ex) {
            throw new IllegalStateException("Can not open or create local file : " + storeFilesManagementFilePath.getAbsolutePath() + ".", ex);
        }

        this.slavesManagementFileChannel = slavesManagementFile.getChannel();
        this.storeFilesManagementFileChannel = storeFilesManagementFile.getChannel();
        this.forceWriteData = forceWriteData;
        this.forceWriteMetaData = forceWriteMetaData;
        
        this.masterLock = new ReentrantReadWriteLock();
        this.closed = false;
        this.masterClosed = this.masterLock.writeLock().newCondition();
        
        try {
            if (storeFilesManagementFile.length() <= 0) {
                createNewStoreFilesManagementFile(maxSlaveNumber);
            } else {
                if (ignoreErrorEntry) {
                    try {
                        storeFilesManagementFile.seek(0);
                        long magicNumber = storeFilesManagementFile.readLong();
                        if (magicNumber != STORE_FILES_MANAGEMENT_FILE_MAGIC_NUMBER) {
                            throw new IllegalStateException("FileBaseContentStoreMaster management file magic number error.");
                        }                        
                    } catch (Throwable ex) {
                        LOGGER.error("Restore store files management file failed, create an new one. File : " + storeFilesManagementFilePath.getAbsolutePath(), ex);
                        createNewStoreFilesManagementFile(maxSlaveNumber);
                        return;
                    }

                    try {
                        int classVersion = storeFilesManagementFile.readInt();
                        if (classVersion != MASTER_CLASS_VERSION) {
                            throw new IllegalStateException("FileBaseContentStoreMaster management file class version error.");
                        }                        
                    } catch (Throwable ex) {
                        LOGGER.error("Restore store files management file failed, create an new one. File : " + storeFilesManagementFilePath.getAbsolutePath(), ex);
                        createNewStoreFilesManagementFile(maxSlaveNumber);
                        return;
                    }

                    try {
                        maxSlaveNumber = storeFilesManagementFile.readLong();    
                    } catch (Throwable ex) {
                        LOGGER.error("Restore store files management file failed, create an new one. File : " + storeFilesManagementFilePath.getAbsolutePath(), ex);
                        createNewStoreFilesManagementFile(maxSlaveNumber);
                        return;
                    }
                    
                    this.maxSlaveNumber = maxSlaveNumber;
                    this.sortedManagedFiles = new LinkedList<ManagedStoreFileMetaData>();
                    this.slaveStoreFiles = new HashMap<Long, ContentStoreSlaveStoreFile>(128 * 1024);
                    
                    byte[] bytes = new byte[ManagedStoreFileMetaData.MANAGED_FILE_META_BYTES_LENGTH];
                    for (long i = 0; i < maxSlaveNumber; ++i) {                        
                        try {
                            IOHelper.readFully(storeFilesManagementFile, bytes);
                        } catch (Throwable ex) {
                            LOGGER.error("Read ManagedStoreFileMetaData bytes failed.", ex);
                            bytes = null;
                        }
                        
                        ManagedStoreFileMetaData fileMeta = ManagedStoreFileMetaData.fromBytes(bytes, i, ignoreErrorEntry);
                        if (fileMeta != null) {
                            sortedManagedFiles.add(fileMeta);
                            if (fileMeta.getFileTotalSize() >= 0) {
                                ContentStoreSlaveStoreFile file = new ContentStoreSlaveStoreFile(fileMeta);
                                slaveStoreFiles.put(i, file);                            
                            }
                        } else {
                            break;
                        }
                    }
                    
                    Collections.sort(sortedManagedFiles);                    
                } else {
                    storeFilesManagementFile.seek(0);
                    long magicNumber = storeFilesManagementFile.readLong();
                    if (magicNumber != STORE_FILES_MANAGEMENT_FILE_MAGIC_NUMBER) {
                        throw new IllegalStateException("FileBaseContentStoreMaster management file magic number error.");
                    }
                    
                    int classVersion = storeFilesManagementFile.readInt();
                    if (classVersion != MASTER_CLASS_VERSION) {
                        throw new IllegalStateException("FileBaseContentStoreMaster management file class version error.");
                    }

                    maxSlaveNumber = storeFilesManagementFile.readLong();
                    this.maxSlaveNumber = maxSlaveNumber;
                    this.sortedManagedFiles = new LinkedList<ManagedStoreFileMetaData>();
                    this.slaveStoreFiles = new HashMap<Long, ContentStoreSlaveStoreFile>(128 * 1024);
                    
                    byte[] bytes = new byte[ManagedStoreFileMetaData.MANAGED_FILE_META_BYTES_LENGTH];
                    for (long i = 0; i < maxSlaveNumber; ++i) {
                        IOHelper.readFully(storeFilesManagementFile, bytes);
                        ManagedStoreFileMetaData fileMeta = ManagedStoreFileMetaData.fromBytes(bytes, i, ignoreErrorEntry);
                        if (fileMeta != null) {
                            sortedManagedFiles.add(fileMeta);
                            if (fileMeta.getFileTotalSize() >= 0) {
                                ContentStoreSlaveStoreFile file = new ContentStoreSlaveStoreFile(fileMeta);
                                slaveStoreFiles.put(i, file);                            
                            }
                        } else {
                            break;
                        }
                    }
                    
                    Collections.sort(sortedManagedFiles);
                }
            }
            
            if (slavesManagementFile.length() <= 0) {
                createNewSlavesManagementFile(maxSlaveNumber);
            } else {
                if (ignoreErrorEntry) {
                    try {
                        slavesManagementFile.seek(0);
                        long magicNumber = slavesManagementFile.readLong();
                        if (magicNumber != SLAVES_MANAGEMENT_FILE_MAGIC_NUMBER) {
                            throw new IllegalStateException("FileBaseContentStoreMaster management file magic number error.");
                        }                        
                    } catch (Throwable ex) {
                        LOGGER.error("Restore slaves management file failed, create an new one. File : " + slavesManagementFilePath.getAbsolutePath(), ex);
                        createNewSlavesManagementFile(maxSlaveNumber);
                        return;
                    }

                    try {
                        int classVersion = slavesManagementFile.readInt();
                        if (classVersion != MASTER_CLASS_VERSION) {
                            throw new IllegalStateException("FileBaseContentStoreMaster management file class version error.");
                        }
                    } catch (Throwable ex) {
                        LOGGER.error("Restore slaves management file failed, create an new one. File : " + slavesManagementFilePath.getAbsolutePath(), ex);
                        createNewSlavesManagementFile(maxSlaveNumber);
                        return;
                    }

                    
                    try {
                        maxSlaveNumber = slavesManagementFile.readLong();
                        if (maxSlaveNumber != this.maxSlaveNumber) {
                            throw new IllegalStateException("maxSlaveNumber in slaves management file error. Expected: " + this.maxSlaveNumber + ", Actual: " + maxSlaveNumber);
                        }
                    } catch (Throwable ex) {
                        LOGGER.error("Restore slaves management file failed, create an new one. File : " + slavesManagementFilePath.getAbsolutePath(), ex);
                        createNewSlavesManagementFile(maxSlaveNumber);
                        return;
                    }
                    
                    this.slaves = new HashMap<Long, FileBaseContentStoreSlave>((int)(maxSlaveNumber / 2));
                    this.slavesNumber = new AtomicLong(0);
                    this.removedSortedSlaves = new LinkedList<ContentStoreSlaveIndex>();
                    
                    byte[] bytes = new byte[ContentStoreSlaveIndex.STORE_SLAVE_INDEX_BYTES_LENGTH];
                    for (long i = 0; i < maxSlaveNumber; ++i) {
                        try {
                            IOHelper.readFully(slavesManagementFile, bytes);
                        } catch (Throwable ex) {
                            LOGGER.error("Read ContentStoreSlaveIndex bytes failed.", ex);
                            bytes = null;
                        }

                        ContentStoreSlaveIndex index = ContentStoreSlaveIndex.fromBytes(bytes, i, ignoreErrorEntry);
                        if (index != null) {
                            if (index.isRemoved()) {
                                removedSortedSlaves.add(index);
                                if (index.getStoreSize() >= 0) {
                                    loadFileBaseContentStoreSlaveAndAdd(index, true);
                                } else {
                                    slavesNumber.incrementAndGet();
                                }
                            } else {
                                loadFileBaseContentStoreSlaveAndAdd(index, true);
                            }
                        } else {
                            break;
                        }
                    }
                    
                    Collections.sort(removedSortedSlaves);
                    needSortRemovedSlaves = false;
                } else {
                    slavesManagementFile.seek(0);
                    long magicNumber = slavesManagementFile.readLong();
                    if (magicNumber != SLAVES_MANAGEMENT_FILE_MAGIC_NUMBER) {
                        throw new IllegalStateException("FileBaseContentStoreMaster management file magic number error.");
                    }
                    
                    int classVersion = slavesManagementFile.readInt();
                    if (classVersion != MASTER_CLASS_VERSION) {
                        throw new IllegalStateException("FileBaseContentStoreMaster management file class version error.");
                    }

                    maxSlaveNumber = slavesManagementFile.readLong();
                    if (maxSlaveNumber != this.maxSlaveNumber) {
                        throw new IllegalStateException("maxSlaveNumber in slaves management file error. Expected: " + this.maxSlaveNumber + ", Actual: " + maxSlaveNumber);
                    }
                    
                    this.slaves = new HashMap<Long, FileBaseContentStoreSlave>((int)(maxSlaveNumber / 2));
                    this.slavesNumber = new AtomicLong(0);
                    this.removedSortedSlaves = new LinkedList<ContentStoreSlaveIndex>();
                    
                    byte[] bytes = new byte[ContentStoreSlaveIndex.STORE_SLAVE_INDEX_BYTES_LENGTH];
                    for (long i = 0; i < maxSlaveNumber; ++i) {
                        IOHelper.readFully(slavesManagementFile, bytes);                    
                        ContentStoreSlaveIndex index = ContentStoreSlaveIndex.fromBytes(bytes, i, ignoreErrorEntry);
                        if (index != null) {
                            if (index.isRemoved()) {
                                removedSortedSlaves.add(index);
                                if (index.getStoreSize() >= 0) {
                                    loadFileBaseContentStoreSlaveAndAdd(index, true);
                                } else {
                                    slavesNumber.incrementAndGet();
                                }
                            } else {
                                loadFileBaseContentStoreSlaveAndAdd(index, true);
                            }
                        } else {
                            break;
                        }
                    }
                    
                    Collections.sort(removedSortedSlaves);
                    needSortRemovedSlaves = false;
                }
            }
        } catch (IOException ex) {
            throw new IllegalStateException("FileBaseContentStoreMaster management file format error.", ex);
        }
        
        Thread gcThread = new Thread(new DataStoreFilesGC(), "FileBaseContentStoreMaster-DataFilesGC");
        gcThread.setDaemon(true);
        gcThread.setPriority(Thread.NORM_PRIORITY);
        gcThread.start();
    }

    private void createNewSlavesManagementFile(long maxSlaveNumber) throws IOException {
        slavesManagementFile.seek(0);
        slavesManagementFile.writeLong(SLAVES_MANAGEMENT_FILE_MAGIC_NUMBER);
        slavesManagementFile.writeInt(MASTER_CLASS_VERSION);

        slavesManagementFile.writeLong(maxSlaveNumber);
        for (long i = 0; i < maxSlaveNumber; ++i) {
            slavesManagementFile.write(ContentStoreSlaveIndex.NULL_STORE_SLAVE_INDEX_BYTES);
        }

        this.slaves = new HashMap<Long, FileBaseContentStoreSlave>((int)(maxSlaveNumber / 2));
        this.slavesNumber = new AtomicLong(0);
        this.removedSortedSlaves = new LinkedList<ContentStoreSlaveIndex>();
    }

    private void createNewStoreFilesManagementFile(long maxSlaveNumber) throws IOException {
        storeFilesManagementFile.seek(0);
        storeFilesManagementFile.writeLong(STORE_FILES_MANAGEMENT_FILE_MAGIC_NUMBER);
        storeFilesManagementFile.writeInt(MASTER_CLASS_VERSION);

        storeFilesManagementFile.writeLong(maxSlaveNumber);
        for (long i = 0; i < maxSlaveNumber; ++i) {
            storeFilesManagementFile.write(ManagedStoreFileMetaData.NULL_MANAGED_FILE_META_BYTES);
        }

        this.maxSlaveNumber = maxSlaveNumber;
        this.sortedManagedFiles = new LinkedList<ManagedStoreFileMetaData>();
        this.slaveStoreFiles = new HashMap<Long, ContentStoreSlaveStoreFile>(128 * 1024);
    }
    
    private final class DataStoreFilesGC implements Runnable {
        @Override
        public void run() {    
            masterLock.writeLock().lock();
            try {
                while (!closed) {                    
                    for (ManagedStoreFileMetaData fileMeta : sortedManagedFiles) {
                        if (fileMeta.getFileTotalSize() < 0) {
                            File path = createDataStoreFilePath((int)fileMeta.getFileNumber());
                            if (path.isFile()) {
                                if (!path.delete()) {
                                    LOGGER.debug("Delete data store file : " + path.getAbsolutePath() + " failed.");
                                }
                            }
                        } else {
                            break;
                        }
                    }
                    
                    try {
                        masterClosed.await(dataStoreFilesGCInterval, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException ex) {
                        LOGGER.debug("Data store files GC thread ended by interrupted.", ex);
                        return;
                    }
                }
            } finally {
                masterLock.writeLock().unlock();
            }
        }        
    }
    
    private FileBaseContentStoreSlave loadFileBaseContentStoreSlaveAndAdd(ContentStoreSlaveIndex index, boolean needIncreaseSlavesNumber) {
        int fileNumber = index.getFileNumber();
        ContentStoreSlaveStoreFile file = slaveStoreFiles.get((long)fileNumber);
        if (file == null) {
            throw new IllegalStateException("Can not get file meta by file number: " + fileNumber);
        }
        
        if (file.getSlaveStoreFileChannel() == null) {
            createSlaveStoreFiles(file, fileNumber);
        }
        
        FileBaseContentStoreSlave slave = new FileBaseContentStoreSlave(file, this, index);
        slaves.put(index.getSlaveNumber(), slave);
        
        if (needIncreaseSlavesNumber) {
            slavesNumber.incrementAndGet();
        }        
        
        file.addSlaveIndexInThisFile(index);
        file.incrementAndGetSlavesInThisFileNumber();
        
        return slave;
    }
        
    private ManagedStoreFileMetaData getOrCreateManagedFileMeta(long mallocSize, long fileTotalSize) throws MallocContentStoreSpaceException, IOException {
        if (sortedManagedFiles.size() <= 0) {
            return createNewManagedFileMeta(mallocSize, fileTotalSize);
        } else {
            Collections.sort(sortedManagedFiles);
            ManagedStoreFileMetaData firstManagedFileMeta = sortedManagedFiles.get(0);
            ManagedStoreFileMetaData lastManagedFileMeta = sortedManagedFiles.get(sortedManagedFiles.size() - 1);
            if (lastManagedFileMeta.getFileSpareSize() < mallocSize) {
                if (firstManagedFileMeta.getFileTotalSize() < 0) {
                    long fileSize = determineFileStoreSize(mallocSize, fileTotalSize);
                    storeManagedFileMeta2ManagementFile(firstManagedFileMeta, fileSize, false, mallocSize);
                    return firstManagedFileMeta;
                } else {
                    return createNewManagedFileMeta(mallocSize, fileTotalSize);
                }
            } else if (lastManagedFileMeta.getFileSpareSize() == mallocSize)  {
                // use the last
                return reuseManagedFileMeta(mallocSize, lastManagedFileMeta);
            } else if (firstManagedFileMeta.getFileSpareSize() >= mallocSize) {
                // use the first
                return reuseManagedFileMeta(mallocSize, firstManagedFileMeta);
            } else {
                // search one to be used
                ManagedStoreFileMetaData searchKey = new ManagedStoreFileMetaData(0, mallocSize);
                int searchedIndex = Collections.binarySearch(sortedManagedFiles, searchKey);
                if (searchedIndex < 0) {
                    // uses the one after the insert position
                    searchedIndex = -searchedIndex - 1;
                }
                
                // uses the searched                
                return reuseManagedFileMeta(mallocSize, sortedManagedFiles.get(searchedIndex));
            }
        }
    }

    private ManagedStoreFileMetaData reuseManagedFileMeta(long mallocSize, ManagedStoreFileMetaData managedFileMeta) throws IOException {
        storeManagedFileMeta2ManagementFile(managedFileMeta, managedFileMeta.getFileTotalSize(), false, mallocSize);
        return managedFileMeta;
    }

    private ManagedStoreFileMetaData createNewManagedFileMeta(long mallocSize, long fileTotalSize) throws IOException {
        long fileSize = determineFileStoreSize(mallocSize, fileTotalSize);
        ManagedStoreFileMetaData newMeta = new ManagedStoreFileMetaData(sortedManagedFiles.size(), fileSize);
        storeManagedFileMeta2ManagementFile(newMeta, newMeta.getFileTotalSize(), false, mallocSize);
        sortedManagedFiles.add(newMeta);
        return newMeta;
    }

    private long determineFileStoreSize(long mallocSize, long fileTotalSize) {
        long fileSize = fileTotalSize >= mallocSize ? fileTotalSize : mallocSize;
        return fileSize;
    }
    
    private void createSlaveStoreFiles(ContentStoreSlaveStoreFile file, int fileNumber) {
        File path = createDataStoreFilePath(fileNumber);
        file.setSlaveStoreFilePath(path);
        
        try {
            file.setSlaveStoreFile(new RandomAccessFile(path, "rw"));
        } catch (FileNotFoundException ex) {
            throw new IllegalStateException("Can not open or create local file : " + path.getAbsolutePath() + ".", ex);
        }    
    }

    private File createDataStoreFilePath(int fileNumber) {
        return new File(storeDirectory, storeName + SLAVE_DATA_STORE_FILE_POSTFIX + fileNumber);
    }
    
    private ContentStoreSlave reuseStoreSlave(ContentStoreSlaveIndex slaveIndex, int indexInList) throws IOException {
        storeSlaveIndex2ManagementFile(slaveIndex, false, slaveIndex.getStoreSize(), slaveIndex.getFileNumber(), slaveIndex.getStoreStartAddress());
        removedSortedSlaves.remove(indexInList);
        
        int fileNumber = slaveIndex.getFileNumber();
        ContentStoreSlaveStoreFile file = slaveStoreFiles.get((long)fileNumber);
        if (file == null) {
            throw new IllegalStateException("Can not get file meta by file number: " + fileNumber);
        }
        
        file.incrementAndGetSlavesInThisFileNumber();
        FileBaseContentStoreSlave slave = slaves.get(slaveIndex.getSlaveNumber());
        slave.reopen();
        return slave;
    }
    
    private FileBaseContentStoreSlave createFileBaseStoreSlave(ManagedStoreFileMetaData slaveFileMeta,
                                                               ContentStoreSlaveIndex slaveIndex, boolean needIncreaseSlavesNumber) throws MallocContentStoreSpaceException {
        ContentStoreSlaveStoreFile slaveFile = slaveStoreFiles.get((long)slaveIndex.getFileNumber());
        if (slaveFile == null) {
            slaveFile = new ContentStoreSlaveStoreFile(slaveFileMeta);
            slaveStoreFiles.put((long)slaveIndex.getFileNumber(), slaveFile);
        }
        
        return loadFileBaseContentStoreSlaveAndAdd(slaveIndex, needIncreaseSlavesNumber);
    }
    
    private FileBaseContentStoreSlave createSlaveAndPut2Master(long mallocSize) throws IOException, MallocContentStoreSpaceException {
        if (slavesNumber.get() >= maxSlaveNumber) {
            throw new ExceededMaxAllowedContentStoreSlavesNumber("Exceeded max allowed slaves number: " + maxSlaveNumber);
        }
                
        ManagedStoreFileMetaData slaveFileMeta = getOrCreateManagedFileMeta(mallocSize, individualSlaveStoreFileSize);
        long newSlaveNumber = slavesNumber.get();
        ContentStoreSlaveIndex newSlaveIndex = createSlaveIndex(mallocSize, slaveFileMeta, newSlaveNumber);
        storeSlaveIndex2ManagementFile(newSlaveIndex, false, newSlaveIndex.getStoreSize(), newSlaveIndex.getFileNumber(), newSlaveIndex.getStoreStartAddress());
        return createFileBaseStoreSlave(slaveFileMeta, newSlaveIndex, true);
    }

    private ContentStoreSlaveIndex createSlaveIndex(long mallocSize, ManagedStoreFileMetaData slaveFileMeta, long newSlaveNumber) {
        ContentStoreSlaveIndex newSlaveIndex = null;
        newSlaveIndex = new ContentStoreSlaveIndex(false, mallocSize, newSlaveNumber, (int)slaveFileMeta.getFileNumber(), slaveFileMeta.getCurrentStartAddress() - mallocSize);
        return newSlaveIndex;
    }
    
    /* (non-Javadoc)
     */
    @Override
    public ContentStoreSlave malloc(long mallocSize) throws MallocContentStoreSpaceException {
        Asserts.longIsPositive("mallocSize", mallocSize);
        masterLock.writeLock().lock();
        try {
            if (closed) {
                throw new MallocContentStoreSpaceException("Store master had been closed.");
            }
            
            if (removedSortedSlaves.size() <= 0) {
                // No one to be reused : 
                return createSlaveAndPut2Master(mallocSize);
            } else {
                if (needSortRemovedSlaves) {
                    Collections.sort(removedSortedSlaves);
                    needSortRemovedSlaves = false;
                }
                
                int lastIndexInList = removedSortedSlaves.size() - 1;
                ContentStoreSlaveIndex lastSlaveIndex = removedSortedSlaves.get(lastIndexInList);
                ContentStoreSlaveIndex firstSlaveIndex = removedSortedSlaves.get(0);
                if (lastSlaveIndex.getStoreSize() < mallocSize) {
                    // no one can be reused. create a new one. but may be the slave number can be reused.
                    if (firstSlaveIndex.getStoreSize() < 0) {
                        //reuse the slave number and the slave index
                        ManagedStoreFileMetaData slaveFileMeta = getOrCreateManagedFileMeta(mallocSize, individualSlaveStoreFileSize);
                        storeSlaveIndex2ManagementFile(firstSlaveIndex, false, mallocSize, (int)slaveFileMeta.getFileNumber(), slaveFileMeta.getCurrentStartAddress() - mallocSize);
                        removedSortedSlaves.remove(0);
                        return createFileBaseStoreSlave(slaveFileMeta, firstSlaveIndex, false);
                    } else {
                        return createSlaveAndPut2Master(mallocSize);
                    }
                } else if (lastSlaveIndex.getStoreSize() == mallocSize)  {
                    // use the last
                    return reuseStoreSlave(lastSlaveIndex, lastIndexInList);
                } else if (firstSlaveIndex.getStoreSize() >= mallocSize) {
                    // use the first 
                    return reuseStoreSlave(firstSlaveIndex, 0);
                } else {
                    // search one to be used
                    ContentStoreSlaveIndex searchKey = new ContentStoreSlaveIndex(false, mallocSize, 0, 0, 0);
                    int searchedIndex = Collections.binarySearch(removedSortedSlaves, searchKey);
                    if (searchedIndex < 0) {
                        // uses the one after the insert position
                        searchedIndex = -searchedIndex - 1;
                    }
                    // uses the searched
                    
                    ContentStoreSlaveIndex searchedSlaveIndex = removedSortedSlaves.get(searchedIndex);
                    return reuseStoreSlave(searchedSlaveIndex, searchedIndex);
                }
            }
        } catch (IOException ex) {
            throw new MallocContentStoreSpaceException("Store slave index bytes to slaves management file failed. File Path: " + slavesManagementFilePath.getAbsolutePath(), ex);
        } finally {
            masterLock.writeLock().unlock();
        }
    }
    
    void removeSlave(long slaveNumber) throws ContentStoreMasterException {
        if (slaveNumber < 0 || slaveNumber > maxSlaveNumber) {
            throw new ContentStoreMasterException("slaveNumber must be this scope [0, " + maxSlaveNumber + "]");
        }
        
        masterLock.writeLock().lock();
        try {
            if (closed) {
                throw new ContentStoreMasterException("Store master had been closed.");
            }
            
            FileBaseContentStoreSlave slave = slaves.get(slaveNumber);
            if (slave == null || slave.isClosed()) {
                return;
            }
            
            ContentStoreSlaveIndex slaveIndex = slave.getSlaveIndex();
            if (slaveIndex.isRemoved()) {
                return;
            }
            
            int fileNumber = slave.getFileNumber();
            ContentStoreSlaveStoreFile file = slaveStoreFiles.get((long)fileNumber);
            if ((file.getSlavesInThisFileNumber() - 1) <= 0) {
                for (ContentStoreSlaveIndex removedIndex : file.getSlavesInThisFile()) {
                    storeSlaveIndex2ManagementFile(removedIndex, true, ContentStoreSlaveIndex.TOTALLY_REMOVED_STORE_SIZE,
                            ContentStoreSlaveIndex.NULL_FILE_NUMBER, ContentStoreSlaveIndex.NULL_STORE_START_ADDRESS);
                    FileBaseContentStoreSlave slave4Remove = slaves.get(removedIndex.getSlaveNumber());
                    if (slave4Remove != null) {
                        try {
                            slave4Remove.close();
                        } catch (Throwable ex) {
                            if (LOGGER.isDebugEnabled()) {
                                LOGGER.debug("Close FileBaseContentStoreSlave failed. File number with slave is " + slave4Remove.getFileNumber() + ", store start address is " + slave4Remove.getStoreStartAddress(), ex);
                            }
                        }
                    }
                    
                    slaves.remove(removedIndex.getSlaveNumber());
                }

                removedSortedSlaves.add(slaveIndex);
                needSortRemovedSlaves = true;

                storeManagedFileMeta2ManagementFile(file.getManagedFileMeta(), ManagedStoreFileMetaData.TOTALLY_REMOVED_FILE_SIZE, true, 0);

                file.clearSlavesInThisFile();
                file.resetSlavesInThisFileNumber();
                file.closeFile();

                slaveStoreFiles.remove((long)fileNumber);
            } else {
                storeSlaveIndex2ManagementFile(slaveIndex, true, slaveIndex.getStoreSize(), 
                        slaveIndex.getFileNumber(), slaveIndex.getStoreStartAddress());
                
                try {
                    slave.close();
                } catch (Throwable ex) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Close FileBaseContentStoreSlave failed. File number with slave is " + slave.getFileNumber() + ", store start address is " + slave.getStoreStartAddress(), ex);
                    }
                }
                
                removedSortedSlaves.add(slaveIndex);
                needSortRemovedSlaves = true;
                file.decrementAndGetSlavesInThisFileNumber();
            }
        } catch (IOException ex) {
            throw new ContentStoreMasterException("Store data to management file failed.", ex);
        } finally {
            masterLock.writeLock().unlock();
        }
    }

    private void storeManagedFileMeta2ManagementFile(ManagedStoreFileMetaData fileMeta, long fileTotalSize, boolean resetUsedSize, long deltaUsedSize) throws IOException {
        long previousFileTotalSize = fileMeta.getFileTotalSize();
        long previousUsedSize = fileMeta.getFileUsedSize();

        fileMeta.setFileTotalSize(fileTotalSize);
        if (resetUsedSize) {
            fileMeta.resetFileUsedSize();
        } else {
            fileMeta.increaseUsedSize(deltaUsedSize);
        }        

        try {
            long storeAddress = STORE_FILES_MANAGEMENT_FILE_START_ADDRESS + (fileMeta.getFileNumber() * ManagedStoreFileMetaData.MANAGED_FILE_META_BYTES_LENGTH);
            byte[] storeBytes = ManagedStoreFileMetaData.toBytes(fileMeta);
            storeFilesManagementFileChannel.write(ByteBuffer.wrap(storeBytes), storeAddress);
            if (forceWriteMetaData) {
                storeFilesManagementFileChannel.force(true);
            }
        } catch (IOException ex) {
            fileMeta.setFileTotalSize(previousFileTotalSize);
            if (resetUsedSize) {
                fileMeta.increaseUsedSize(previousUsedSize);
            } else {
                fileMeta.increaseUsedSize(-deltaUsedSize);
            }
            
            throw ex;
        }
    }
    
    private void storeSlaveIndex2ManagementFile(ContentStoreSlaveIndex index,
                                                boolean removed, long storeSize, int fileNumber, long storeStartAddress) throws IOException {
        boolean previousRemoved = index.isRemoved();
        long previousStoreSize = index.getStoreSize();
        int previousFileNumber = index.getFileNumber();
        long previousStoreStartAddress = index.getStoreStartAddress();
        
        index.setRemoved(removed);
        index.setStoreSize(storeSize);
        index.setFileNumber(fileNumber);
        index.setStoreStartAddress(storeStartAddress);

        try {
            long indexStoreAddress = SLAVES_MANAGEMENT_FILE_STORE_START_ADDRESS + (index.getSlaveNumber() * ContentStoreSlaveIndex.STORE_SLAVE_INDEX_BYTES_LENGTH);
            byte[] indexBytes = ContentStoreSlaveIndex.toBytes(index);
            slavesManagementFileChannel.write(ByteBuffer.wrap(indexBytes), indexStoreAddress);
            if (forceWriteMetaData) {
                slavesManagementFileChannel.force(true);
            }
        } catch (IOException ex) {
            index.setRemoved(previousRemoved);
            index.setStoreSize(previousStoreSize);
            index.setFileNumber(previousFileNumber);
            index.setStoreStartAddress(previousStoreStartAddress);
            throw ex;
        }
    }
    
    /* (non-Javadoc)
     */
    @Override
    public ContentStoreSlave retrieveSlave(long slaveNumber) throws ContentStoreMasterException {
        masterLock.readLock().lock();
        try {
            if (closed) {
                throw new IllegalStateException("Store master had been closed.");
            }
            
            FileBaseContentStoreSlave slave = slaves.get(slaveNumber);
            if (slave == null || slave.isClosed()) {
                return null;
            }
            
            ContentStoreSlaveIndex slaveIndex = slave.getSlaveIndex();
            if (slaveIndex.isRemoved()) {
                return null;
            }

            return slave;
        } finally {
            masterLock.readLock().unlock();
        }
    }

    @Override
    public void close() throws ContentStoreMasterException {
        masterLock.writeLock().lock();
        try {
            if (closed) {
                return;
            }
            
            closeAllSlaves();
            closeAllSlaveStoreFiles();
            closeSlaveManagementFile();
            closeSlaveStoreManagementFile();
            
            closed = true;
            masterClosed.signalAll();
        } finally {
            masterLock.writeLock().unlock();
        }
    }

    private void closeSlaveStoreManagementFile() {
        if (storeFilesManagementFileChannel != null) {
            try {
                storeFilesManagementFileChannel.force(true);
            } catch (Throwable ex) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Force write content to persistence file failed. File path:  " + storeFilesManagementFilePath.getAbsolutePath() + ".", ex);
                }
            }
            
            try {
                storeFilesManagementFileChannel.close();
            } catch (Throwable ex) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Close persistence file channel failed. File path:  " + storeFilesManagementFilePath.getAbsolutePath() + ".", ex);
                } 
            }
        }
        
        if (storeFilesManagementFile != null) {
            try {
                storeFilesManagementFile.close();
            } catch (Throwable ex) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Close persistence file failed. File path:  " + storeFilesManagementFilePath.getAbsolutePath() + ".", ex);
                }
            }
        }
    }

    private void closeSlaveManagementFile() {
        if (slavesManagementFileChannel != null) {
            try {
                slavesManagementFileChannel.force(true);
            } catch (Throwable ex) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Force write content to persistence file failed. File path:  " + slavesManagementFilePath.getAbsolutePath() + ".", ex);
                } 
            }
            
            try {
                slavesManagementFileChannel.close();
            } catch (Throwable ex) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Close persistence file channel failed. File path:  " + slavesManagementFilePath.getAbsolutePath() + ".", ex);
                } 
            }
        }
        
        if (slavesManagementFile != null) {
            try {
                slavesManagementFile.close();
            } catch (Throwable ex) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Close persistence file failed. File path:  " + slavesManagementFilePath.getAbsolutePath() + ".", ex);
                }
            }
        }
    }

    private void closeAllSlaveStoreFiles() {
        for (ContentStoreSlaveStoreFile file : slaveStoreFiles.values()) {
            file.clearSlavesInThisFile();
            file.resetSlavesInThisFileNumber();
            file.closeFile();
        }
        
        slaveStoreFiles.clear();
        sortedManagedFiles.clear();
    }

    private void closeAllSlaves() {
        for (FileBaseContentStoreSlave slave : slaves.values()) {
            try {
                slave.close();
            } catch (Throwable ex) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Close FileBaseContentStoreSlave failed. File number with slave is " + slave.getFileNumber() + ", store start address is " + slave.getStoreStartAddress(), ex);
                }
            }
        }
        
        slaves.clear();
        slavesNumber.set(0);
        removedSortedSlaves.clear();
    }

    public boolean isForceWriteData() {
        return forceWriteData;
    }

}
