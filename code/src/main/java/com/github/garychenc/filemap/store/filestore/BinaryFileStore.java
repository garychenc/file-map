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

import java.io.Externalizable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.nio.channels.FileChannel;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.github.garychenc.filemap.store.*;
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
public class BinaryFileStore implements StoreReader, StoreWriter {
    
    private final static Logger LOGGER = LoggerFactory.getLogger(BinaryFileStore.class);
    private final static String MAP_STORE_FILE_POSTFIX = "-meta.db.map";
    private final static long FILE_MAGIC_NUMBER = 0x12E95D2;
    private final static int CLASS_VERSION = 1;
    private final static long GRID_DATA_STORE_OFFSET = 20;
    private final static int LEAST_GRIDS_NUMBER = 1;
    
    private final File storeDirectory;
    private final File metaFilePath;    
    private RandomAccessFile metaFile;
    private FileChannel metaFileChannel;
    private final boolean forceWriteMetaData;
    private final ContentStoreMaster storeMaster;
    private final BinaryFileStoreConvertor defaultStoreConvertor;
    private final float mallocSizeExtendFactor;
    
    private final ReadWriteLock fileLock;
    
    private volatile IndexGrid[] indexGrids;
    private volatile int indexGridsNumber;
    private final LongAdder size;
    private final String storeName;
    
    public BinaryFileStore(String storeDirPath, String storeName, boolean forceWriteMetaData, 
            int indexGridsNumber, int maxGridBucketsNumber, float mallocSizeExtendFactor, ContentStoreMaster filesMaster,
            boolean ignoreErrorEntry) {
        Asserts.stringNotEmpty("storeDirPath", storeDirPath);
        Asserts.stringNotEmpty("storeName", storeName);
        Asserts.notNull("StoreFilesMaster", filesMaster);
        Asserts.isTrue(mallocSizeExtendFactor > 1.0F, "mallocSizeExtendFactor must be greater than 1.0");

        this.defaultStoreConvertor = new FileBaseBinaryFileStoreConvertorImpl();
        this.storeDirectory = Utils.getOrCreateDir(storeDirPath, storeName);
        this.metaFilePath = new File(storeDirectory, storeName + MAP_STORE_FILE_POSTFIX);
        this.mallocSizeExtendFactor = mallocSizeExtendFactor;
        this.storeName = storeName;
        
        try {
            this.metaFile = new RandomAccessFile(metaFilePath, "rw");
        } catch (FileNotFoundException ex) {
            throw new IllegalStateException("Can not open or create local file : " + metaFilePath.getAbsolutePath() + ".", ex);
        }

        if (indexGridsNumber < LEAST_GRIDS_NUMBER) {
            indexGridsNumber = LEAST_GRIDS_NUMBER;
        }
        
        this.metaFileChannel = metaFile.getChannel();
        this.forceWriteMetaData = forceWriteMetaData;
        this.storeMaster = filesMaster;
        this.fileLock = new ReentrantReadWriteLock();
        this.size = new LongAdder();
        checkOrCreateStoreFileFormat(ignoreErrorEntry, indexGridsNumber, maxGridBucketsNumber);
    }
    
    private void checkOrCreateStoreFileFormat(boolean ignoreErrorEntry, int indexGridsNumber, int maxGridBucketsNumber) {
        try {
            if (metaFile.length() <= 0) {
                createNewMetaFile(indexGridsNumber, maxGridBucketsNumber);
            } else {
                restoreFromMetaFile(ignoreErrorEntry, indexGridsNumber, maxGridBucketsNumber);
            }
        } catch (IOException ex) {
            throw new IllegalStateException("Local file store format error. File : " + metaFilePath.getAbsolutePath(), ex);
        }
    }

    private void restoreFromMetaFile(boolean ignoreErrorEntry, int indexGridsNumberFromPara, int maxGridBucketsNumberFromPara) throws IOException {
        if (ignoreErrorEntry) {
            try {
                metaFile.seek(0);
                long magicNumber = metaFile.readLong();
                if (magicNumber != FILE_MAGIC_NUMBER) {
                    throw new IllegalStateException("Local file store meta file magic number error.");
                }                
            } catch (Throwable ex) {
                LOGGER.error("Restore meta file failed, create an new one. File : " + metaFilePath.getAbsolutePath(), ex);
                createNewMetaFile(indexGridsNumberFromPara, maxGridBucketsNumberFromPara);
                return;
            }
            
            try {
                int classVersion = metaFile.readInt();
                if (classVersion != CLASS_VERSION) {
                    throw new IllegalStateException("Local file store meta file class version error.");
                }                
            } catch (Throwable ex) {
                LOGGER.error("Restore meta file failed, create an new one. File : " + metaFilePath.getAbsolutePath(), ex);
                createNewMetaFile(indexGridsNumberFromPara, maxGridBucketsNumberFromPara);
                return;
            }

            int indexGridsNumber = indexGridsNumberFromPara;
            int maxGridBucketsNumber = maxGridBucketsNumberFromPara;
            try {
                indexGridsNumber = metaFile.readInt();
                maxGridBucketsNumber = metaFile.readInt();
            } catch (Throwable ex) {
                LOGGER.error("Restore meta file failed, create an new one. File : " + metaFilePath.getAbsolutePath(), ex);
                createNewMetaFile(indexGridsNumberFromPara, maxGridBucketsNumberFromPara);
                return;
            }
            
            int gridBytesLength = IndexBucket.BUCKET_BYTES_LENGTH * maxGridBucketsNumber;            
            this.indexGrids = new IndexGrid[indexGridsNumber];
            
            byte[] gridBytes = new byte[gridBytesLength];
            for (int i = 0; i < indexGridsNumber; ++i) {                
                try {                    
                    IOHelper.readFully(metaFile, gridBytes);
                } catch (Throwable ex) {
                    LOGGER.error("Read IndexGrid failed.", ex);
                    gridBytes = null;
                }
                
                IndexGrid grid = IndexGrid.fromBytes(gridBytes, GRID_DATA_STORE_OFFSET + (i * gridBytesLength), metaFileChannel, 
                        forceWriteMetaData, maxGridBucketsNumber, mallocSizeExtendFactor, ignoreErrorEntry);
                this.indexGrids[i] = grid;
            }
            
            for (int i = 0; i < indexGridsNumber; ++i) {
                int currGridSize = this.indexGrids[i].indexBucketSize() - this.indexGrids[i].removedIndexBucketSize();
                if (currGridSize < 0) {
                    currGridSize = 0;
                }
                
                size.add(currGridSize);
            }
            
            this.indexGridsNumber = indexGridsNumber;
        } else {
            metaFile.seek(0);
            long magicNumber = metaFile.readLong();
            if (magicNumber != FILE_MAGIC_NUMBER) {
                throw new IllegalStateException("Local file store meta file magic number error.");
            }
            
            int classVersion = metaFile.readInt();
            if (classVersion != CLASS_VERSION) {
                throw new IllegalStateException("Local file store meta file class version error.");
            }

            int indexGridsNumber = metaFile.readInt();
            int maxGridBucketsNumber = metaFile.readInt();
            
            int gridBytesLength = IndexBucket.BUCKET_BYTES_LENGTH * maxGridBucketsNumber;
            this.indexGrids = new IndexGrid[indexGridsNumber];

            byte[] gridBytes = new byte[gridBytesLength];
            for (int i = 0; i < indexGridsNumber; ++i) {
                IOHelper.readFully(metaFile, gridBytes);                
                IndexGrid grid = IndexGrid.fromBytes(gridBytes, GRID_DATA_STORE_OFFSET + (i * gridBytesLength), metaFileChannel, 
                        forceWriteMetaData, maxGridBucketsNumber, mallocSizeExtendFactor, ignoreErrorEntry);
                this.indexGrids[i] = grid;
            }
            
            for (int i = 0; i < indexGridsNumber; ++i) {
                int currGridSize = this.indexGrids[i].indexBucketSize() - this.indexGrids[i].removedIndexBucketSize();
                if (currGridSize < 0) {
                    currGridSize = 0;
                }

                size.add(currGridSize);
            }
            
            this.indexGridsNumber = indexGridsNumber;
        }
    }

    private void createNewMetaFile(int indexGridsNumber, int maxGridBucketsNumber) throws IOException {
        for (int i = 1; i < Integer.MAX_VALUE; ++i) {
            int indexGridsUpperNumber = (int) Math.pow(2, i);
            if (indexGridsNumber <= indexGridsUpperNumber) {
                indexGridsNumber = indexGridsUpperNumber;
                break;
            }
        }
        
        metaFile.seek(0);
        metaFile.writeLong(FILE_MAGIC_NUMBER);
        metaFile.writeInt(CLASS_VERSION);
        
        metaFile.writeInt(indexGridsNumber);
        metaFile.writeInt(maxGridBucketsNumber);

        int gridBytesLength = IndexBucket.BUCKET_BYTES_LENGTH * maxGridBucketsNumber;
        byte[] nullGridBytes = new byte[gridBytesLength];
        for (int i = 0; i < maxGridBucketsNumber; ++i) {
            for (int j = 0; j < IndexBucket.BUCKET_BYTES_LENGTH; ++j) {
                nullGridBytes[i * IndexBucket.BUCKET_BYTES_LENGTH + j] = IndexBucket.NULL_BUCKET_BYTES[j];
            }
        }
        
        for (int i = 0; i < indexGridsNumber; ++i) {
            metaFile.write(nullGridBytes);
        }
        
        this.indexGrids = new IndexGrid[indexGridsNumber];
        for (int i = 0; i < indexGridsNumber; ++i) {
            this.indexGrids[i] = new IndexGrid(GRID_DATA_STORE_OFFSET + (i * gridBytesLength), metaFileChannel, forceWriteMetaData, 
                    maxGridBucketsNumber, mallocSizeExtendFactor);
        }
        
        this.indexGridsNumber = indexGridsNumber;
    }
    
    public void delete() throws StoreException {
        this.close();
        fileLock.writeLock().lock();
        try {
            Utils.deleteTheWholeDirectory(storeDirectory, null);
            size.reset();
        } catch (Throwable ex) {
            throw new StoreException("Delete store directory failed. Store Directory: " + storeDirectory.getAbsolutePath(), ex);
        } finally {
            fileLock.writeLock().unlock();
        }
    }
    
    public void close() throws StoreException {
        fileLock.writeLock().lock();
        try {
            if (metaFileChannel != null) {
                try {
                    metaFileChannel.force(true);
                } catch (Throwable ex) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Force write content to persistence file failed. File path:  " + metaFilePath.getAbsolutePath() + ".", ex);
                    }
                }
                
                try {
                    metaFileChannel.close();
                } catch (Throwable ex) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Close persistence file channel failed. File path:  " + metaFilePath.getAbsolutePath() + ".", ex);
                    } 
                }

                metaFileChannel = null;
            }
            
            if (metaFile != null) {
                try {
                    metaFile.close();
                } catch (Throwable ex) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Close persistence file failed. File path:  " + metaFilePath.getAbsolutePath() + ".", ex);
                    } 
                }

                metaFile = null;
            }
        } finally {
            fileLock.writeLock().unlock();
        }
    }

    private StoreValue internalRead(StoreKey key, boolean removeWhenEntryCorrupt) throws StoreReaderException {
        Asserts.notNull("StoreKey", key);
        if (!(key instanceof BinaryFileStoreKey)) {
            throw new IllegalArgumentException("Use the StoreKey to get info form LocalBinaryFileStore must be the type of " + BinaryFileStoreKey.class.getName());
        }

        int hash = hash(key.hashCode());
        
        fileLock.readLock().lock();
        try {
            checkFileIsClosed();

            int index = indexFor(hash, indexGridsNumber);
            int gridIndex = index;

            do {
                try {
                    IndexGrid grig = this.indexGrids[gridIndex];
                    return grig.get((BinaryFileStoreKey)key, hash, storeMaster, defaultStoreConvertor, removeWhenEntryCorrupt, new ErrorEntryRemovedListener() {                    
                        @Override
                        public void entryRemoved() {
                            size.decrement();
                        }
                    });
                } catch (SearchReachedMaxBucketsNumberException e) {
                    ++gridIndex;
                    if (gridIndex >= indexGridsNumber) {
                        gridIndex = 0;
                    }
                }
            } while (gridIndex != index);

            return null;
        } finally {
            fileLock.readLock().unlock();
        }
    }

    private void checkFileIsClosed() {
        if (metaFileChannel == null) {
            throw new IllegalStateException("Binary file store had been closed.");
        }
    }

    /**
     * Applies a supplemental hash function to a given hashCode, which
     * defends against poor quality hash functions.  This is critical
     * because HashMap uses power-of-two length hash tables, that
     * otherwise encounter collisions for hashCodes that do not differ
     * in lower bits. Note: Null keys always map to hash 0, thus index 0.
     */
    private static final int hash(int h) {
        // This function ensures that hashCodes that differ only by
        // constant multiples at each bit position have a bounded
        // number of collisions (approximately 8 at default load factor).
        h ^= (h >>> 20) ^ (h >>> 12);
        return h ^ (h >>> 7) ^ (h >>> 4);
    }
    
    /**
     * Returns index for hash code h.
     */
    private static int indexFor(int h, int length) {
        return h & (length-1);
    }
    
    private static final ContentStoreSlave store2Slave(BinaryFileStoreKey key, BinaryFileStoreValue value,
                                                       int hash, ContentStoreMaster storeMaster, BinaryFileStoreConvertor convertor, float mallocSizeExtendFactor)
                    throws ContentStoreBlockException, MallocContentStoreSpaceException, ContentStoreSlaveException, ContentStoreBlockLengthExceedException {
        key.setHashValue(hash);
        value.setOriginalStoreKey(key);
        ContentStoreBlock valueBlock = convertor.toStoreBlock(value);
        if (valueBlock == null) {
            throw new IllegalStateException("Can not get binary store block from binary store value.");
        }
        
        ContentStoreSlave slave = storeMaster.malloc((long)(valueBlock.getContentLength() * mallocSizeExtendFactor));
        slave.store(valueBlock);
        return slave;
    }
    
    /**
     * For performance consideration, allow multiple keys which are totally the same to be added to this store. 
     */
    private boolean internalAdd(StoreKey key, StoreValue value) throws StoreIsFullException, StoreWriterException {
        Asserts.notNull("StoreKey", key);
        Asserts.notNull("StoreValue", value);
        
        if (!(key instanceof BinaryFileStoreKey)) {
            throw new IllegalArgumentException("The StoreKey added to LocalBinaryFileStore must be the type of " + BinaryFileStoreKey.class.getName());
        }
        
        if (!(value instanceof BinaryFileStoreValue)) {
            throw new IllegalArgumentException("The StoreValue added to LocalBinaryFileStore must be the type of " + BinaryFileStoreValue.class.getName());
        }
        
        BinaryFileStoreValue binaryFileStoreValue = (BinaryFileStoreValue) value;
        binaryFileStoreValue.setVersionNumber(0L);

        int hash = hash(key.hashCode());
        
        fileLock.readLock().lock();
        try {
            checkFileIsClosed();

            ContentStoreSlave slave = store2Slave((BinaryFileStoreKey)key, binaryFileStoreValue, hash, storeMaster, defaultStoreConvertor, mallocSizeExtendFactor);
            int index = indexFor(hash, indexGridsNumber);
            int gridIndex = index;
            
            do {
                try {
                    IndexGrid grig = this.indexGrids[gridIndex];
                    if (grig.add(slave, hash)) {
                        size.increment();
                        return true;
                    } else {
                        return false;
                    }
                } catch (AddExceededMaxBucketsNumberException e) {
                    ++gridIndex;
                    if (gridIndex >= indexGridsNumber) {
                        gridIndex = 0;
                    }
                }
            } while (gridIndex != index);
            
            throw new StoreIsFullException("Can not add this store value to binary store file because of all grids are full. " +
                    "Store Directory: " + storeDirectory.getAbsolutePath() + ", Store Key ID: " + key.getUuid());
            
        } catch (ContentStoreBlockException ex) {
            throw new StoreWriterException("Convert binary store value to block failed.", ex);
        } catch (ExceededMaxAllowedContentStoreSlavesNumber ex) {
            throw new StoreIsFullException("Malloc store space failed, exceeded max allowed slaves number.", ex);
        } catch (MallocContentStoreSpaceException ex) {
            throw new StoreWriterException("Malloc store space failed.", ex);
        } catch (ContentStoreSlaveException ex) {
            throw new StoreWriterException("Store block to store slave failed.", ex);
        } catch (ContentStoreBlockLengthExceedException ex) {
            throw new StoreWriterException("Store block to store slave failed.", ex);
        } finally {
            fileLock.readLock().unlock();
        }
    }

    private boolean internalRemove(StoreKey key, long versionNumber) throws StoreWriterException, VersionConflictedException {
        Asserts.notNull("StoreKey", key);
        if (!(key instanceof BinaryFileStoreKey)) {
            throw new IllegalArgumentException("Use the StoreKey to remove data form LocalBinaryFileStore must be the type of " + BinaryFileStoreKey.class.getName());
        }

        int hash = hash(key.hashCode());
        
        fileLock.readLock().lock();
        try {
            checkFileIsClosed();

            int index = indexFor(hash, indexGridsNumber);
            int gridIndex = index;

            do {
                try {
                    IndexGrid grig = this.indexGrids[gridIndex];
                    if (grig.remove((BinaryFileStoreKey)key, hash, storeMaster, defaultStoreConvertor, versionNumber, new ErrorEntryRemovedListener() {                    
                        @Override
                        public void entryRemoved() {
                            size.decrement();
                        } 
                    })) {
                        size.decrement();
                        return true;
                    } else {
                        return false;
                    }
                } catch (SearchReachedMaxBucketsNumberException e) {
                    ++gridIndex;
                    if (gridIndex >= indexGridsNumber) {
                        gridIndex = 0;
                    }
                }
            } while (gridIndex != index);

            return false;
        } finally {
            fileLock.readLock().unlock();
        }
    }

    private boolean internalUpdate(StoreKey key, StoreValue value) throws StoreWriterException, VersionConflictedException {
        Asserts.notNull("StoreKey", key);
        Asserts.notNull("StoreValue", value);
        
        if (!(key instanceof BinaryFileStoreKey)) {
            throw new IllegalArgumentException("The StoreKey to update the LocalBinaryFileStore must be the type of " + BinaryFileStoreKey.class.getName());
        }
        
        if (!(value instanceof BinaryFileStoreValue)) {
            throw new IllegalArgumentException("The StoreValue to update the LocalBinaryFileStore must be the type of " + BinaryFileStoreValue.class.getName());
        }

        int hash = hash(key.hashCode());
        
        fileLock.readLock().lock();
        try {
            checkFileIsClosed();

            int index = indexFor(hash, indexGridsNumber);
            int gridIndex = index;
            
            do {
                try {
                    IndexGrid grig = this.indexGrids[gridIndex];
                    return grig.update((BinaryFileStoreKey)key, (BinaryFileStoreValue)value, hash, storeMaster, defaultStoreConvertor);
                } catch (SearchReachedMaxBucketsNumberException e) {
                    ++gridIndex;
                    if (gridIndex >= indexGridsNumber) {
                        gridIndex = 0;
                    }
                }
            } while (gridIndex != index);
            
            return false;
        } finally {
            fileLock.readLock().unlock();
        }
    }

    private Set<StoreKey> internalKeySet(boolean ignoreErrorEntry) throws StoreReaderException {
        fileLock.readLock().lock();
        try {
            checkFileIsClosed();

            Set<StoreKey> keys = new LinkedHashSet<StoreKey>();
            for (IndexGrid grid : indexGrids) {
                keys.addAll(grid.getAllStoreKeys(storeMaster, defaultStoreConvertor, ignoreErrorEntry, new ErrorEntryRemovedListener() {                    
                    @Override
                    public void entryRemoved() {
                        size.decrement();
                    }
                }));
            }
            
            return Collections.unmodifiableSet(keys);
        } finally {
            fileLock.readLock().unlock();
        }
    }
    
    @Override
    public List<StoreEntry> values() throws StoreReaderException {
        fileLock.readLock().lock();
        try {
            checkFileIsClosed();

            List<StoreEntry> values = new LinkedList<StoreEntry>();
            for (IndexGrid grid : indexGrids) {
                values.addAll(grid.getAllStoreValues(storeMaster, defaultStoreConvertor, false, new ErrorEntryRemovedListener() {                    
                    @Override
                    public void entryRemoved() {
                        size.decrement();
                    }
                }));
            }
            
            return Collections.unmodifiableList(values);
        } catch (Throwable ex) {
            throw new StoreReaderException(ex);
        } finally {
            fileLock.readLock().unlock();
        }
    }

    @Override
    public List<StoreEntry> values(boolean ignoreErrorEntry) throws StoreReaderException {
        fileLock.readLock().lock();
        try {
            checkFileIsClosed();

            List<StoreEntry> values = new LinkedList<StoreEntry>();
            for (IndexGrid grid : indexGrids) {
                values.addAll(grid.getAllStoreValues(storeMaster, defaultStoreConvertor, ignoreErrorEntry, new ErrorEntryRemovedListener() {                    
                    @Override
                    public void entryRemoved() {
                        size.decrement();
                    }
                }));
            }

            return Collections.unmodifiableList(values);
        } catch (Throwable ex) {
            throw new StoreReaderException(ex);
        } finally {
            fileLock.readLock().unlock();
        }
    }
    
    private int getSize() throws StoreReaderException {
        fileLock.readLock().lock();
        try {
            checkFileIsClosed();

            return size.intValue();
        } catch (Throwable ex) {
            throw new StoreReaderException(ex);
        } finally {
            fileLock.readLock().unlock();
        }
    }

    @Override
    public long size() throws StoreReaderException {
        return getSize();
    }

    @Override
    public boolean add(String key, byte[] value) throws StoreIsFullException, StoreWriterException {
        try {
            StringStoreKey stringStoreKey = new StringStoreKey(key, key);
            FileBaseBinaryFileStoreKeyImpl storeKey = new FileBaseBinaryFileStoreKeyImpl(stringStoreKey);
            FileBaseBinaryFileStoreValueImpl storeValue = new FileBaseBinaryFileStoreValueImpl(value);
            return this.internalAdd(storeKey, storeValue);            
        } catch (StoreIsFullException ex) {
            throw ex;
        } catch (Throwable ex) {
            throw new StoreWriterException(ex);
        }
    }

    @Override
    public boolean add(long key, byte[] value) throws StoreIsFullException, StoreWriterException {
        try {
            LongStoreKey longStoreKey = new LongStoreKey(String.valueOf(key), key);
            FileBaseBinaryFileStoreKeyImpl storeKey = new FileBaseBinaryFileStoreKeyImpl(longStoreKey);
            FileBaseBinaryFileStoreValueImpl storeValue = new FileBaseBinaryFileStoreValueImpl(value);
            return this.internalAdd(storeKey, storeValue);
        } catch (StoreIsFullException ex) {
            throw ex;
        } catch (Throwable ex) {
            throw new StoreWriterException(ex);
        }
    }

    @Override
    public boolean add(String key, String value) throws StoreIsFullException, StoreWriterException {
        try {
            StringStoreKey stringStoreKey = new StringStoreKey(key, key);
            FileBaseBinaryFileStoreKeyImpl storeKey = new FileBaseBinaryFileStoreKeyImpl(stringStoreKey);
            FileBaseBinaryFileStoreValueImpl storeValue = new FileBaseBinaryFileStoreValueImpl(value);
            return this.internalAdd(storeKey, storeValue);
        } catch (StoreIsFullException ex) {
            throw ex;
        } catch (Throwable ex) {
            throw new StoreWriterException(ex);
        }
    }

    @Override
    public boolean add(long key, String value) throws StoreIsFullException, StoreWriterException {
        try {
            LongStoreKey longStoreKey = new LongStoreKey(String.valueOf(key), key);
            FileBaseBinaryFileStoreKeyImpl storeKey = new FileBaseBinaryFileStoreKeyImpl(longStoreKey);
            FileBaseBinaryFileStoreValueImpl storeValue = new FileBaseBinaryFileStoreValueImpl(value);
            return this.internalAdd(storeKey, storeValue);
        } catch (StoreIsFullException ex) {
            throw ex;
        } catch (Throwable ex) {
            throw new StoreWriterException(ex);
        }
    }

    @Override
    public boolean add(String key, Externalizable value) throws StoreIsFullException, StoreWriterException {
        try {
            StringStoreKey stringStoreKey = new StringStoreKey(key, key);
            FileBaseBinaryFileStoreKeyImpl storeKey = new FileBaseBinaryFileStoreKeyImpl(stringStoreKey);
            FileBaseBinaryFileStoreValueImpl storeValue = new FileBaseBinaryFileStoreValueImpl(value);
            return this.internalAdd(storeKey, storeValue);            
        } catch (StoreIsFullException ex) {
            throw ex;
        } catch (Throwable ex) {
            throw new StoreWriterException(ex);
        }
    }

    @Override
    public boolean add(long key, Externalizable value) throws StoreIsFullException, StoreWriterException {
        try {
            LongStoreKey longStoreKey = new LongStoreKey(String.valueOf(key), key);
            FileBaseBinaryFileStoreKeyImpl storeKey = new FileBaseBinaryFileStoreKeyImpl(longStoreKey);
            FileBaseBinaryFileStoreValueImpl storeValue = new FileBaseBinaryFileStoreValueImpl(value);
            return this.internalAdd(storeKey, storeValue);
        } catch (StoreIsFullException ex) {
            throw ex;
        } catch (Throwable ex) {
            throw new StoreWriterException(ex);
        }
    }

    @Override
    public boolean add(String key, Serializable value) throws StoreIsFullException, StoreWriterException {
        try {
            StringStoreKey stringStoreKey = new StringStoreKey(key, key);
            FileBaseBinaryFileStoreKeyImpl storeKey = new FileBaseBinaryFileStoreKeyImpl(stringStoreKey);
            FileBaseBinaryFileStoreValueImpl storeValue = new FileBaseBinaryFileStoreValueImpl(value);
            return this.internalAdd(storeKey, storeValue);
        } catch (StoreIsFullException ex) {
            throw ex;
        } catch (Throwable ex) {
            throw new StoreWriterException(ex);
        }
    }

    @Override
    public boolean add(long key, Serializable value) throws StoreIsFullException, StoreWriterException {
        try {
            LongStoreKey longStoreKey = new LongStoreKey(String.valueOf(key), key);
            FileBaseBinaryFileStoreKeyImpl storeKey = new FileBaseBinaryFileStoreKeyImpl(longStoreKey);
            FileBaseBinaryFileStoreValueImpl storeValue = new FileBaseBinaryFileStoreValueImpl(value);
            return this.internalAdd(storeKey, storeValue);            
        } catch (StoreIsFullException ex) {
            throw ex;
        } catch (Throwable ex) {
            throw new StoreWriterException(ex);
        }
    }

    @Override
    public boolean remove(String key, long versionNumber) throws StoreWriterException, VersionConflictedException {
        try {
            StringStoreKey stringStoreKey = new StringStoreKey(key, key);
            FileBaseBinaryFileStoreKeyImpl storeKey = new FileBaseBinaryFileStoreKeyImpl(stringStoreKey);
            return this.internalRemove(storeKey, versionNumber);            
        } catch (VersionConflictedException ex) {
            throw ex;
        } catch (Throwable ex) {
            throw new StoreWriterException(ex);
        }
    }

    @Override
    public boolean remove(long key, long versionNumber) throws StoreWriterException, VersionConflictedException {
        try {
            LongStoreKey longStoreKey = new LongStoreKey(String.valueOf(key), key);
            FileBaseBinaryFileStoreKeyImpl storeKey = new FileBaseBinaryFileStoreKeyImpl(longStoreKey);
            return this.internalRemove(storeKey, versionNumber);            
        } catch (VersionConflictedException ex) {
            throw ex;
        } catch (Throwable ex) {
            throw new StoreWriterException(ex);
        }
    }

    @Override
    public boolean update(String key, byte[] value, long versionNumber) throws StoreWriterException, VersionConflictedException {
        try {
            StringStoreKey stringStoreKey = new StringStoreKey(key, key);
            FileBaseBinaryFileStoreKeyImpl storeKey = new FileBaseBinaryFileStoreKeyImpl(stringStoreKey);
            FileBaseBinaryFileStoreValueImpl storeValue = new FileBaseBinaryFileStoreValueImpl(value);
            storeValue.setVersionNumber(versionNumber);
            return internalUpdate(storeKey, storeValue);            
        } catch (VersionConflictedException ex) {
            throw ex;
        } catch (Throwable ex) {
            throw new StoreWriterException(ex);
        }
    }

    @Override
    public boolean update(long key, byte[] value, long versionNumber) throws StoreWriterException, VersionConflictedException {
        try {
            LongStoreKey longStoreKey = new LongStoreKey(String.valueOf(key), key);
            FileBaseBinaryFileStoreKeyImpl storeKey = new FileBaseBinaryFileStoreKeyImpl(longStoreKey);
            FileBaseBinaryFileStoreValueImpl storeValue = new FileBaseBinaryFileStoreValueImpl(value);
            storeValue.setVersionNumber(versionNumber);
            return internalUpdate(storeKey, storeValue);            
        } catch (VersionConflictedException ex) {
            throw ex;
        } catch (Throwable ex) {
            throw new StoreWriterException(ex);
        }
    }

    @Override
    public boolean update(String key, String value, long versionNumber) throws StoreWriterException, VersionConflictedException {
        try {
            StringStoreKey stringStoreKey = new StringStoreKey(key, key);
            FileBaseBinaryFileStoreKeyImpl storeKey = new FileBaseBinaryFileStoreKeyImpl(stringStoreKey);
            FileBaseBinaryFileStoreValueImpl storeValue = new FileBaseBinaryFileStoreValueImpl(value);
            storeValue.setVersionNumber(versionNumber);
            return internalUpdate(storeKey, storeValue);            
        } catch (VersionConflictedException ex) {
            throw ex;
        } catch (Throwable ex) {
            throw new StoreWriterException(ex);
        }
    }

    @Override
    public boolean update(long key, String value, long versionNumber) throws StoreWriterException, VersionConflictedException {
        try {
            LongStoreKey longStoreKey = new LongStoreKey(String.valueOf(key), key);
            FileBaseBinaryFileStoreKeyImpl storeKey = new FileBaseBinaryFileStoreKeyImpl(longStoreKey);
            FileBaseBinaryFileStoreValueImpl storeValue = new FileBaseBinaryFileStoreValueImpl(value);
            storeValue.setVersionNumber(versionNumber);
            return internalUpdate(storeKey, storeValue);            
        } catch (VersionConflictedException ex) {
            throw ex;
        } catch (Throwable ex) {
            throw new StoreWriterException(ex);
        }
    }

    @Override
    public boolean update(String key, Externalizable value, long versionNumber) throws StoreWriterException, VersionConflictedException {
        try {
            StringStoreKey stringStoreKey = new StringStoreKey(key, key);
            FileBaseBinaryFileStoreKeyImpl storeKey = new FileBaseBinaryFileStoreKeyImpl(stringStoreKey);
            FileBaseBinaryFileStoreValueImpl storeValue = new FileBaseBinaryFileStoreValueImpl(value);
            storeValue.setVersionNumber(versionNumber);
            return internalUpdate(storeKey, storeValue);            
        } catch (VersionConflictedException ex) {
            throw ex;
        } catch (Throwable ex) {
            throw new StoreWriterException(ex);
        }
    }

    @Override
    public boolean update(long key, Externalizable value, long versionNumber) throws StoreWriterException, VersionConflictedException {
        try {
            LongStoreKey longStoreKey = new LongStoreKey(String.valueOf(key), key);
            FileBaseBinaryFileStoreKeyImpl storeKey = new FileBaseBinaryFileStoreKeyImpl(longStoreKey);
            FileBaseBinaryFileStoreValueImpl storeValue = new FileBaseBinaryFileStoreValueImpl(value);
            storeValue.setVersionNumber(versionNumber);
            return internalUpdate(storeKey, storeValue);            
        } catch (VersionConflictedException ex) {
            throw ex;
        } catch (Throwable ex) {
            throw new StoreWriterException(ex);
        }
    }

    @Override
    public boolean update(String key, Serializable value, long versionNumber) throws StoreWriterException, VersionConflictedException {
        try {
            StringStoreKey stringStoreKey = new StringStoreKey(key, key);
            FileBaseBinaryFileStoreKeyImpl storeKey = new FileBaseBinaryFileStoreKeyImpl(stringStoreKey);
            FileBaseBinaryFileStoreValueImpl storeValue = new FileBaseBinaryFileStoreValueImpl(value);
            storeValue.setVersionNumber(versionNumber);
            return internalUpdate(storeKey, storeValue);            
        } catch (VersionConflictedException ex) {
            throw ex;
        } catch (Throwable ex) {
            throw new StoreWriterException(ex);
        }
    }

    @Override
    public boolean update(long key, Serializable value, long versionNumber) throws StoreWriterException, VersionConflictedException {
        try {
            LongStoreKey longStoreKey = new LongStoreKey(String.valueOf(key), key);
            FileBaseBinaryFileStoreKeyImpl storeKey = new FileBaseBinaryFileStoreKeyImpl(longStoreKey);
            FileBaseBinaryFileStoreValueImpl storeValue = new FileBaseBinaryFileStoreValueImpl(value);
            storeValue.setVersionNumber(versionNumber);
            return internalUpdate(storeKey, storeValue);            
        } catch (VersionConflictedException ex) {
            throw ex;
        } catch (Throwable ex) {
            throw new StoreWriterException(ex);
        }
    }

    @Override
    public StoreEntry read(String key) throws StoreReaderException {
        try {
            StringStoreKey stringStoreKey = new StringStoreKey(key, key);
            FileBaseBinaryFileStoreKeyImpl storeKey = new FileBaseBinaryFileStoreKeyImpl(stringStoreKey);
            StoreValue storeValue = this.internalRead(storeKey, false);
            if (storeValue == null) {
                return null;
            }
            
            FileBaseBinaryFileStoreValueImpl binStoreValue = (FileBaseBinaryFileStoreValueImpl) storeValue;
            StoreEntry entry = new StoreEntry(binStoreValue.getVersionNumber(), binStoreValue.getRealValue(), IndexGrid.getKeyForStoreEntry(binStoreValue));
            return entry;            
        } catch (Throwable ex) {
            throw new StoreReaderException(ex);
        }
    }

    @Override
    public StoreEntry read(long key) throws StoreReaderException {
        try {
            LongStoreKey longStoreKey = new LongStoreKey(String.valueOf(key), key);
            FileBaseBinaryFileStoreKeyImpl storeKey = new FileBaseBinaryFileStoreKeyImpl(longStoreKey);
            StoreValue storeValue = this.internalRead(storeKey, false);
            if (storeValue == null) {
                return null;
            }
            
            FileBaseBinaryFileStoreValueImpl binStoreValue = (FileBaseBinaryFileStoreValueImpl) storeValue;
            StoreEntry entry = new StoreEntry(binStoreValue.getVersionNumber(), binStoreValue.getRealValue(), IndexGrid.getKeyForStoreEntry(binStoreValue));
            return entry;            
        } catch (Throwable ex) {
            throw new StoreReaderException(ex);
        }
    }
    
    @Override
    public StoreEntry read(String key, boolean removeWhenEntryCorrupt) throws StoreReaderException {
        try {
            StringStoreKey stringStoreKey = new StringStoreKey(key, key);
            FileBaseBinaryFileStoreKeyImpl storeKey = new FileBaseBinaryFileStoreKeyImpl(stringStoreKey);
            StoreValue storeValue = this.internalRead(storeKey, removeWhenEntryCorrupt);
            if (storeValue == null) {
                return null;
            }
            
            FileBaseBinaryFileStoreValueImpl binStoreValue = (FileBaseBinaryFileStoreValueImpl) storeValue;
            StoreEntry entry = new StoreEntry(binStoreValue.getVersionNumber(), binStoreValue.getRealValue(), IndexGrid.getKeyForStoreEntry(binStoreValue));
            return entry;            
        } catch (Throwable ex) {
            throw new StoreReaderException(ex);
        }
    }

    @Override
    public StoreEntry read(long key, boolean removeWhenEntryCorrupt) throws StoreReaderException {
        try {
            LongStoreKey longStoreKey = new LongStoreKey(String.valueOf(key), key);
            FileBaseBinaryFileStoreKeyImpl storeKey = new FileBaseBinaryFileStoreKeyImpl(longStoreKey);
            StoreValue storeValue = this.internalRead(storeKey, removeWhenEntryCorrupt);
            if (storeValue == null) {
                return null;
            }
            
            FileBaseBinaryFileStoreValueImpl binStoreValue = (FileBaseBinaryFileStoreValueImpl) storeValue;
            StoreEntry entry = new StoreEntry(binStoreValue.getVersionNumber(), binStoreValue.getRealValue(), IndexGrid.getKeyForStoreEntry(binStoreValue));
            return entry;            
        } catch (Throwable ex) {
            throw new StoreReaderException(ex);
        }
    }

    @Override
    public Set<Object> keySet() throws StoreReaderException {
        try {
            Set<Object> newKeys = new LinkedHashSet<Object>();
            for (StoreKey key : this.internalKeySet(false)) {
                FileBaseBinaryFileStoreKeyImpl binKey = (FileBaseBinaryFileStoreKeyImpl) key;
                StoreKey realKey = binKey.getRealKey();
                
                if (realKey instanceof StringStoreKey) {
                    StringStoreKey stringStoreKey = (StringStoreKey) realKey;
                    newKeys.add(stringStoreKey.getKey());
                } else if (realKey instanceof LongStoreKey) {
                    LongStoreKey longStoreKey = (LongStoreKey) realKey;
                    newKeys.add(longStoreKey.getKey());
                }
            }

            return Collections.unmodifiableSet(newKeys);            
        } catch (Throwable ex) {
            throw new StoreReaderException(ex);
        }
    }
    
    @Override
    public Set<Object> keySet(boolean ignoreErrorEntry) throws StoreReaderException {
        try {
            Set<Object> newKeys = new LinkedHashSet<Object>();
            for (StoreKey key : this.internalKeySet(ignoreErrorEntry)) {
                FileBaseBinaryFileStoreKeyImpl binKey = (FileBaseBinaryFileStoreKeyImpl) key;
                StoreKey realKey = binKey.getRealKey();
                
                if (realKey instanceof StringStoreKey) {
                    StringStoreKey stringStoreKey = (StringStoreKey) realKey;
                    newKeys.add(stringStoreKey.getKey());
                } else if (realKey instanceof LongStoreKey) {
                    LongStoreKey longStoreKey = (LongStoreKey) realKey;
                    newKeys.add(longStoreKey.getKey());
                }
            }

            return Collections.unmodifiableSet(newKeys);            
        } catch (Throwable ex) {
            throw new StoreReaderException(ex);
        }
    }

    public String getStoreName() {
        return storeName;
    }

}
