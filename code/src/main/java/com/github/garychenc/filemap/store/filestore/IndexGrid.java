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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.github.garychenc.filemap.store.StoreEntry;
import com.github.garychenc.filemap.store.StoreWriterException;
import com.github.garychenc.filemap.store.VersionConflictedException;
import com.github.garychenc.filemap.util.Asserts;
import com.github.garychenc.filemap.store.StoreReaderException;
import com.github.garychenc.filemap.store.filestore.IndexBucket.BucketFromBytesResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Gary CHEN
 *
 */
class IndexGrid {

    private final static Logger LOGGER = LoggerFactory.getLogger(IndexGrid.class);

    //cache line padding 1
    private long p1, p2, p3, p4, p5, p6, p7, p8;

    private final ReadWriteLock gridLock;
    private final LongAdder bucketSize;
    private final Map<Integer, LinkedList<IndexBucket>> buckets;
    private final LinkedList<IndexBucket> removedBuckets;
    private final long gridStartAddress;

    private final FileChannel metaFileChannel;
    private final boolean forceWriteMetaData;

    private final int maxGridBucketsNumber;
    private final float mallocSizeExtendFactor;

    //cache line padding 2
    private long p9, p10, p11, p12, p13, p14, p15, p16;

    IndexGrid(long gridStartAddress, FileChannel metaFileChannel, boolean forceWriteMetaData,
              int maxGridBucketsNumber, float mallocSizeExtendFactor) {
        this.gridLock = new ReentrantReadWriteLock();
        this.bucketSize = new LongAdder();
        this.buckets = new HashMap<Integer, LinkedList<IndexBucket>>(8 * 1024);
        this.removedBuckets = new LinkedList<IndexBucket>();
        this.gridStartAddress = gridStartAddress;
        this.metaFileChannel = metaFileChannel;
        this.forceWriteMetaData = forceWriteMetaData;
        this.maxGridBucketsNumber = maxGridBucketsNumber;
        this.mallocSizeExtendFactor = mallocSizeExtendFactor;
    }

    private void storeBucket2MetaFile(IndexBucket bucket, long slaveNumber, boolean removed, int hashValue) throws IOException {
        long previousSlaveNumber = bucket.getSlaveNumber();
        bucket.setSlaveNumber(slaveNumber);

        try {
            long bucketStoreAddress = gridStartAddress + (bucket.getBucketIndex() * IndexBucket.BUCKET_BYTES_LENGTH);
            byte[] bucketBytes = IndexBucket.toBytes(bucket, removed, hashValue);
            metaFileChannel.write(ByteBuffer.wrap(bucketBytes), bucketStoreAddress);
            if (forceWriteMetaData) {
                metaFileChannel.force(true);
            }
        } catch (IOException ex) {
            bucket.setSlaveNumber(previousSlaveNumber);
            throw ex;
        }
    }

    boolean add(ContentStoreSlave slave, int hash) throws AddExceededMaxBucketsNumberException, StoreWriterException {
        gridLock.writeLock().lock();
        try {
            if (removedBuckets.size() <= 0) {
                int currentBucketSize = bucketSize.intValue();
                if (currentBucketSize >= maxGridBucketsNumber) {
                    throw new AddExceededMaxBucketsNumberException("Exceeded max allowed buckets number: " + maxGridBucketsNumber);
                } else {
                    IndexBucket bucket = new IndexBucket(slave.getSlaveNumber(), currentBucketSize);
                    storeBucket2MetaFile(bucket, slave.getSlaveNumber(), false, hash);
                    LinkedList<IndexBucket> bucketsByHash = buckets.get(hash);
                    if (bucketsByHash == null) {
                        bucketsByHash = new LinkedList<IndexBucket>();
                        buckets.put(hash, bucketsByHash);
                    }

                    bucketsByHash.add(bucket);
                    bucketSize.increment();
                    return true;
                }
            } else {
                IndexBucket bucket = removedBuckets.get(0);
                storeBucket2MetaFile(bucket, slave.getSlaveNumber(), false, hash);
                removedBuckets.remove(0);
                LinkedList<IndexBucket> bucketsByHash = buckets.get(hash);
                if (bucketsByHash == null) {
                    bucketsByHash = new LinkedList<IndexBucket>();
                    buckets.put(hash, bucketsByHash);
                }

                bucketsByHash.add(bucket);
                return true;
            }
        } catch (IOException ex) {
            throw new StoreWriterException("Store index bucket bytes to grid meta file failed. Grid Start Address: " + gridStartAddress, ex);
        } finally {
            gridLock.writeLock().unlock();
        }
    }

    private static final class InternalGetResult {
        private BinaryFileStoreValue storeValue;
        private IndexBucket bucket;
        private ContentStoreSlave slave;
    }

    private InternalGetResult internalGet(BinaryFileStoreKey key, int hash, ContentStoreMaster storeMaster, BinaryFileStoreConvertor convertor,
                                          boolean removeWhenEntryCorrupt, ErrorEntryRemovedListener entryRemovedListener) throws SearchReachedMaxBucketsNumberException, StoreReaderException {
        Asserts.notNull("BinaryFileStoreKey", key);

        if (!removeWhenEntryCorrupt) {
            try {
                LinkedList<IndexBucket> forRemovingIndexBuckets = new LinkedList<IndexBucket>();
                LinkedList<IndexBucket> bucketsByHash = null;
                boolean targetBucketFound = false;
                InternalGetResult result = new InternalGetResult();
                int currentBucketSize = 0;

                gridLock.readLock().lock();
                try {
                    currentBucketSize = bucketSize.intValue();
                    if (removedBuckets.size() >= currentBucketSize) {
                        if (currentBucketSize >= maxGridBucketsNumber) {
                            throw new SearchReachedMaxBucketsNumberException("Reach max buckets number: " + maxGridBucketsNumber);
                        } else {
                            return null;
                        }
                    }

                    bucketsByHash = buckets.get(hash);
                    if (bucketsByHash == null || bucketsByHash.size() == 0) {
                        if (currentBucketSize >= maxGridBucketsNumber) {
                            throw new SearchReachedMaxBucketsNumberException("Reach max buckets number: " + maxGridBucketsNumber);
                        } else {
                            return null;
                        }
                    } else {
                        for (IndexBucket bucket : bucketsByHash) {
                            ContentStoreSlave slave = storeMaster.retrieveSlave(bucket.getSlaveNumber());
                            if (slave == null) {
                                forRemovingIndexBuckets.add(bucket);
                            } else {
                                ContentStoreBlock valueBlock = slave.read();
                                if (valueBlock == null) {
                                    forRemovingIndexBuckets.add(bucket);
                                } else {
                                    BinaryFileStoreValue value = convertor.fromStoreBlock(valueBlock);
                                    BinaryFileStoreKey anotherKey = (BinaryFileStoreKey) value.getOriginalStoreKey();
                                    if (anotherKey.getHashValue() == hash && (anotherKey == key || key.equals(anotherKey))) {
                                        result.storeValue = value;
                                        result.bucket = bucket;
                                        result.slave = slave;
                                        targetBucketFound = true;
                                        break;
                                    }
                                }
                            }
                        }

                        if (forRemovingIndexBuckets.isEmpty()) {
                            if (targetBucketFound) {
                                return result;
                            } else {
                                if (currentBucketSize >= maxGridBucketsNumber) {
                                    throw new SearchReachedMaxBucketsNumberException("Reach max buckets number: " + maxGridBucketsNumber);
                                } else {
                                    return null;
                                }
                            }
                        }
                    }
                } finally {
                    gridLock.readLock().unlock();
                }

                gridLock.writeLock().lock();
                try {
                    for (IndexBucket removingBucket : forRemovingIndexBuckets) {
                        try {
                            storeBucket2MetaFile(removingBucket, IndexBucket.REMOVED_SLAVE_NUMBER, true, IndexBucket.NULL_HASH_VALUE);
                            if (bucketsByHash.contains(removingBucket)) {
                                bucketsByHash.remove(removingBucket);
                                if (bucketsByHash.isEmpty()) {
                                    buckets.remove(hash);
                                }

                                removedBuckets.add(removingBucket);

                                if (entryRemovedListener != null) {
                                    entryRemovedListener.entryRemoved();
                                }
                            }
                        } catch (IOException exce) {
                            LOGGER.error("Store deleted bucket meta data to file failed.", exce);
                        }
                    }

                    if (targetBucketFound) {
                        return result;
                    } else {
                        if (currentBucketSize >= maxGridBucketsNumber) {
                            throw new SearchReachedMaxBucketsNumberException("Reach max buckets number: " + maxGridBucketsNumber);
                        } else {
                            return null;
                        }
                    }
                } finally {
                    gridLock.writeLock().unlock();
                }
            } catch (ContentStoreBlockException ex) {
                throw new StoreReaderException("Convert block to binary store value failed.", ex);
            } catch (ContentStoreSlaveException ex) {
                throw new StoreReaderException("Read block from store slave failed.", ex);
            } catch (ContentStoreMasterException ex) {
                throw new StoreReaderException("Retrieve store slave failed.", ex);
            }
        } else {
            ContentStoreSlave slave = null;
            IndexBucket currentBucket = null;
            LinkedList<IndexBucket> forRemovingIndexBuckets = new LinkedList<IndexBucket>();
            LinkedList<IndexBucket> bucketsByHash = null;
            boolean targetBucketFound = false;
            InternalGetResult result = new InternalGetResult();
            int currentBucketSize = 0;

            try {
                gridLock.readLock().lock();
                try {
                    currentBucketSize = bucketSize.intValue();
                    if (removedBuckets.size() >= currentBucketSize) {
                        if (currentBucketSize >= maxGridBucketsNumber) {
                            throw new SearchReachedMaxBucketsNumberException("Reach max buckets number: " + maxGridBucketsNumber);
                        } else {
                            return null;
                        }
                    }

                    bucketsByHash = buckets.get(hash);
                    if (bucketsByHash == null || bucketsByHash.size() == 0) {
                        if (currentBucketSize >= maxGridBucketsNumber) {
                            throw new SearchReachedMaxBucketsNumberException("Reach max buckets number: " + maxGridBucketsNumber);
                        } else {
                            return null;
                        }
                    } else {
                        for (IndexBucket bucket : bucketsByHash) {
                            currentBucket = bucket;
                            slave = storeMaster.retrieveSlave(bucket.getSlaveNumber());
                            if (slave == null) {
                                forRemovingIndexBuckets.add(bucket);
                            } else {
                                ContentStoreBlock valueBlock = slave.read();
                                if (valueBlock == null) {
                                    forRemovingIndexBuckets.add(bucket);
                                } else {
                                    BinaryFileStoreValue value = convertor.fromStoreBlock(valueBlock);
                                    BinaryFileStoreKey anotherKey = (BinaryFileStoreKey) value.getOriginalStoreKey();
                                    if (anotherKey.getHashValue() == hash && (anotherKey == key || key.equals(anotherKey))) {
                                        result.storeValue = value;
                                        result.bucket = bucket;
                                        result.slave = slave;
                                        targetBucketFound = true;
                                        break;
                                    }
                                }
                            }
                        }

                        if (forRemovingIndexBuckets.isEmpty()) {
                            if (targetBucketFound) {
                                return result;
                            } else {
                                if (currentBucketSize >= maxGridBucketsNumber) {
                                    throw new SearchReachedMaxBucketsNumberException("Reach max buckets number: " + maxGridBucketsNumber);
                                } else {
                                    return null;
                                }
                            }
                        }
                    }
                } finally {
                    gridLock.readLock().unlock();
                }

                gridLock.writeLock().lock();
                try {
                    for (IndexBucket removingBucket : forRemovingIndexBuckets) {
                        try {
                            storeBucket2MetaFile(removingBucket, IndexBucket.REMOVED_SLAVE_NUMBER, true, IndexBucket.NULL_HASH_VALUE);
                            if (bucketsByHash.contains(removingBucket)) {
                                bucketsByHash.remove(removingBucket);
                                if (bucketsByHash.size() == 0) {
                                    buckets.remove(hash);
                                }

                                removedBuckets.add(removingBucket);

                                if (entryRemovedListener != null) {
                                    entryRemovedListener.entryRemoved();
                                }
                            }
                        } catch (IOException exce) {
                            LOGGER.error("Store deleted bucket meta data to file failed.", exce);
                        }
                    }

                    if (targetBucketFound) {
                        return result;
                    } else {
                        if (currentBucketSize >= maxGridBucketsNumber) {
                            throw new SearchReachedMaxBucketsNumberException("Reach max buckets number: " + maxGridBucketsNumber);
                        } else {
                            return null;
                        }
                    }
                } finally {
                    gridLock.writeLock().unlock();
                }
            } catch (ContentStoreBlockException ex) {
                LOGGER.error("Convert block to binary store value failed.", ex);
                removeErrorEntryOnGet(hash, entryRemovedListener, slave, currentBucket, forRemovingIndexBuckets);
                return null;
            } catch (ContentStoreSlaveException ex) {
                LOGGER.error("Read block from store slave failed.", ex);
                removeErrorEntryOnGet(hash, entryRemovedListener, slave, currentBucket, forRemovingIndexBuckets);
                return null;
            } catch (ContentStoreMasterException ex) {
                LOGGER.error("Retrieve store slave failed.", ex);
                removeErrorEntryOnGet(hash, entryRemovedListener, slave, currentBucket, forRemovingIndexBuckets);
                return null;
            }
        }
    }

    private void removeErrorEntryOnGet(int hash, ErrorEntryRemovedListener entryRemovedListener,
                                       ContentStoreSlave slave, IndexBucket currentBucket, LinkedList<IndexBucket> forRemovingIndexBuckets) {
        gridLock.writeLock().lock();
        try {
            LinkedList<IndexBucket> bucketsByHash = buckets.get(hash);

            for (IndexBucket removingBucket : forRemovingIndexBuckets) {
                try {
                    storeBucket2MetaFile(removingBucket, IndexBucket.REMOVED_SLAVE_NUMBER, true, IndexBucket.NULL_HASH_VALUE);
                    if (bucketsByHash != null && bucketsByHash.contains(removingBucket)) {
                        bucketsByHash.remove(removingBucket);
                        if (bucketsByHash.size() == 0) {
                            buckets.remove(hash);
                        }

                        removedBuckets.add(removingBucket);

                        if (entryRemovedListener != null) {
                            entryRemovedListener.entryRemoved();
                        }
                    }
                } catch (IOException exce) {
                    LOGGER.error("Store deleted bucket meta data to file failed.", exce);
                }
            }

            if (slave != null) {
                try {
                    slave.delete();
                } catch (ContentStoreSlaveException exce) {
                    LOGGER.error("Delete error store slave failed.", exce);
                }
            }

            if (currentBucket != null) {
                try {
                    storeBucket2MetaFile(currentBucket, IndexBucket.REMOVED_SLAVE_NUMBER, true, IndexBucket.NULL_HASH_VALUE);
                    if (bucketsByHash != null && bucketsByHash.contains(currentBucket)) {
                        bucketsByHash.remove(currentBucket);
                        if (bucketsByHash.size() == 0) {
                            buckets.remove(hash);
                        }

                        removedBuckets.add(currentBucket);

                        if (entryRemovedListener != null) {
                            entryRemovedListener.entryRemoved();
                        }
                    }
                } catch (IOException exce) {
                    LOGGER.error("Store deleted bucket meta data to file failed.", exce);
                }
            }
        } finally {
            gridLock.writeLock().unlock();
        }
    }

    BinaryFileStoreValue get(BinaryFileStoreKey key, int hash, ContentStoreMaster storeMaster, BinaryFileStoreConvertor convertor,
                             boolean removeWhenEntryCorrupt, ErrorEntryRemovedListener entryRemovedListener) throws SearchReachedMaxBucketsNumberException, StoreReaderException {
        InternalGetResult result = internalGet(key, hash, storeMaster, convertor, removeWhenEntryCorrupt, entryRemovedListener);
        if (result != null) {
            return result.storeValue;
        } else {
            return null;
        }
    }

    boolean remove(BinaryFileStoreKey key, int hash, ContentStoreMaster storeMaster, BinaryFileStoreConvertor convertor,
                   long versionNumber, ErrorEntryRemovedListener entryRemovedListener) throws SearchReachedMaxBucketsNumberException, StoreWriterException, VersionConflictedException {
        //Not really remove, just mark as remove, so the length of the list is still the same.
        //Then event after removing, read can get a SearchReachedMaxBucketsNumberException

        InternalGetResult result = null;
        try {
            result = internalGet(key, hash, storeMaster, convertor, true, entryRemovedListener);
        } catch (StoreReaderException ex) {
            throw new StoreWriterException(ex);
        }

        if (result == null) {
            return false;
        }

        gridLock.writeLock().lock();
        try {
            if (result.bucket.getSlaveNumber() != IndexBucket.REMOVED_SLAVE_NUMBER) {
                long expectedVersionNumber = result.storeValue.getVersionNumber();
                if (expectedVersionNumber != versionNumber) {
                    throw new VersionConflictedException("Version conflicted when remove value from grid of file store. ProvidedVersionNumber: " +
                            versionNumber + ", ExpectedVersionNumber: " +
                            expectedVersionNumber + ".", versionNumber, expectedVersionNumber);
                }

                result.slave.delete();
                storeBucket2MetaFile(result.bucket, IndexBucket.REMOVED_SLAVE_NUMBER, true, IndexBucket.NULL_HASH_VALUE);
                LinkedList<IndexBucket> bucketsByHash = buckets.get(hash);
                if (bucketsByHash != null && bucketsByHash.contains(result.bucket)) {
                    bucketsByHash.remove(result.bucket);
                    if (bucketsByHash.size() == 0) {
                        buckets.remove(hash);
                    }

                    removedBuckets.add(result.bucket);
                }

                return true;
            } else {
                return false;
            }
        } catch (ContentStoreSlaveException ex) {
            throw new StoreWriterException("Delete block from store slave failed.", ex);
        } catch (IOException ex) {
            throw new StoreWriterException("Store index bucket bytes to grid meta file failed. Grid Start Address: " + gridStartAddress, ex);
        } finally {
            gridLock.writeLock().unlock();
        }
    }

    boolean update(BinaryFileStoreKey key, BinaryFileStoreValue value, int hash, ContentStoreMaster storeMaster, BinaryFileStoreConvertor convertor)
            throws SearchReachedMaxBucketsNumberException, StoreWriterException, VersionConflictedException {
        InternalGetResult result = null;
        try {
            result = internalGet(key, hash, storeMaster, convertor, false, null);
        } catch (StoreReaderException ex) {
            throw new StoreWriterException(ex);
        }

        if (result == null) {
            return false;
        }

        gridLock.writeLock().lock();
        try {
            if (result.bucket.getSlaveNumber() != IndexBucket.REMOVED_SLAVE_NUMBER) {
                long expectedVersionNumber = result.storeValue.getVersionNumber();
                long providedVersionNumber = value.getVersionNumber();
                if (expectedVersionNumber != providedVersionNumber) {
                    throw new VersionConflictedException("Version conflicted when update value for grid of file store. ProvidedVersionNumber: " +
                            providedVersionNumber + ", ExpectedVersionNumber: " +
                            expectedVersionNumber + ".", providedVersionNumber, expectedVersionNumber);
                }

                value.setVersionNumber(providedVersionNumber + 1L);

                updateSlave(key, value, hash, result.slave, storeMaster, result.bucket, convertor);
                storeBucket2MetaFile(result.bucket, result.bucket.getSlaveNumber(), false, hash);
                return true;
            } else {
                return false;
            }
        } catch (ContentStoreSlaveException ex) {
            throw new StoreWriterException("Update block value to store slave failed.", ex);
        } catch (IOException ex) {
            throw new StoreWriterException("Store index bucket bytes to grid meta file failed. Grid Start Address: " + gridStartAddress, ex);
        } catch (ContentStoreBlockException ex) {
            throw new StoreWriterException("Convert block to binary store value failed.", ex);
        } catch (ExceededMaxAllowedContentStoreSlavesNumber ex) {
            throw new StoreWriterException("Malloc store space failed, exceeded max allowed slaves number.", ex);
        } catch (MallocContentStoreSpaceException ex) {
            throw new StoreWriterException("Malloc store space failed.", ex);
        } catch (ContentStoreBlockLengthExceedException ex) {
            throw new StoreWriterException("Store block to store slave failed.", ex);
        } finally {
            gridLock.writeLock().unlock();
        }
    }

    private void updateSlave(BinaryFileStoreKey key, BinaryFileStoreValue value, int hash, ContentStoreSlave slave,
                             ContentStoreMaster storeMaster, IndexBucket bucket, BinaryFileStoreConvertor convertor) throws ContentStoreBlockException, ContentStoreSlaveException,
            MallocContentStoreSpaceException, ContentStoreBlockLengthExceedException {
        key.setHashValue(hash);
        value.setOriginalStoreKey(key);
        ContentStoreBlock updateValueBlock = convertor.toStoreBlock(value);
        if (updateValueBlock == null) {
            throw new IllegalStateException("Can not get binary store block from binary store value.");
        }

        try {
            slave.store(updateValueBlock);
        } catch (ContentStoreBlockLengthExceedException ex) {
            slave.delete();
            ContentStoreSlave slaveNew = storeMaster.malloc((long) (updateValueBlock.getContentLength() * mallocSizeExtendFactor));
            slaveNew.store(updateValueBlock);
            bucket.setSlaveNumber(slaveNew.getSlaveNumber());
        }
    }

    private final class RemovingIndexBucketItem {
        private final IndexBucket forRemovingIndexBucket;
        private final int forRemovingIndexBucketHash;

        private RemovingIndexBucketItem(IndexBucket forRemovingIndexBucket, int forRemovingIndexBucketHash) {
            this.forRemovingIndexBucket = forRemovingIndexBucket;
            this.forRemovingIndexBucketHash = forRemovingIndexBucketHash;
        }
    }

    Set<StoreKey> getAllStoreKeys(ContentStoreMaster storeMaster, BinaryFileStoreConvertor convertor, boolean ignoreErrorEntry,
                                  ErrorEntryRemovedListener entryRemovedListener) throws StoreReaderException {
        if (!ignoreErrorEntry) {
            try {
                Set<StoreKey> keys = new LinkedHashSet<StoreKey>();
                LinkedList<RemovingIndexBucketItem> forRemovingIndexBuckets = new LinkedList<>();

                gridLock.readLock().lock();
                try {
                    if (removedBuckets.size() >= bucketSize.intValue()) {
                        return Collections.unmodifiableSet(keys);
                    }

                    for (Entry<Integer, LinkedList<IndexBucket>> bucketsByHashEntry : buckets.entrySet()) {
                        int hash = bucketsByHashEntry.getKey();
                        LinkedList<IndexBucket> bucketsByHash = bucketsByHashEntry.getValue();

                        for (IndexBucket bucket : bucketsByHash) {
                            ContentStoreSlave slave = storeMaster.retrieveSlave(bucket.getSlaveNumber());
                            if (slave == null) {
                                forRemovingIndexBuckets.add(new RemovingIndexBucketItem(bucket, hash));
                            } else {
                                ContentStoreBlock valueBlock = slave.read();
                                if (valueBlock == null) {
                                    forRemovingIndexBuckets.add(new RemovingIndexBucketItem(bucket, hash));
                                } else {
                                    BinaryFileStoreValue value = convertor.fromStoreBlock(valueBlock);
                                    keys.add(value.getOriginalStoreKey());
                                }
                            }
                        }
                    }

                    if (forRemovingIndexBuckets.isEmpty()) {
                        return Collections.unmodifiableSet(keys);
                    }

                } finally {
                    gridLock.readLock().unlock();
                }

                gridLock.writeLock().lock();
                try {
                    for (RemovingIndexBucketItem removingBucketItem : forRemovingIndexBuckets) {
                        try {
                            IndexBucket removingBucket = removingBucketItem.forRemovingIndexBucket;
                            int hash = removingBucketItem.forRemovingIndexBucketHash;
                            LinkedList<IndexBucket> bucketsByHash = buckets.get(hash);

                            storeBucket2MetaFile(removingBucket, IndexBucket.REMOVED_SLAVE_NUMBER, true, IndexBucket.NULL_HASH_VALUE);
                            if (bucketsByHash != null && bucketsByHash.contains(removingBucket)) {
                                bucketsByHash.remove(removingBucket);
                                if (bucketsByHash.size() == 0) {
                                    buckets.remove(hash);
                                }

                                removedBuckets.add(removingBucket);

                                if (entryRemovedListener != null) {
                                    entryRemovedListener.entryRemoved();
                                }
                            }
                        } catch (IOException exce) {
                            LOGGER.error("Store deleted bucket meta data to file failed.", exce);
                        }
                    }

                    return Collections.unmodifiableSet(keys);
                } finally {
                    gridLock.writeLock().unlock();
                }
            } catch (ContentStoreBlockException ex) {
                throw new StoreReaderException("Convert block to binary store value failed.", ex);
            } catch (ContentStoreSlaveException ex) {
                throw new StoreReaderException("Read block from store slave failed.", ex);
            } catch (ContentStoreMasterException ex) {
                throw new StoreReaderException("Retrieve store slave failed.", ex);
            } catch (Throwable ex) {
                throw new StoreReaderException(ex.getMessage(), ex);
            }
        } else {
            Set<StoreKey> keys = new LinkedHashSet<StoreKey>();
            LinkedList<RemovingIndexBucketItem> forRemovingIndexBuckets = new LinkedList<>();

            gridLock.readLock().lock();
            try {
                if (removedBuckets.size() >= bucketSize.intValue()) {
                    return Collections.unmodifiableSet(keys);
                }

                for (Entry<Integer, LinkedList<IndexBucket>> bucketsByHashEntry : buckets.entrySet()) {
                    int hash = bucketsByHashEntry.getKey();
                    LinkedList<IndexBucket> bucketsByHash = bucketsByHashEntry.getValue();

                    for (IndexBucket bucket : bucketsByHash) {
                        ContentStoreSlave slave = null;

                        try {
                            slave = storeMaster.retrieveSlave(bucket.getSlaveNumber());
                            if (slave == null) {
                                forRemovingIndexBuckets.add(new RemovingIndexBucketItem(bucket, hash));
                            } else {
                                ContentStoreBlock valueBlock = slave.read();
                                if (valueBlock == null) {
                                    forRemovingIndexBuckets.add(new RemovingIndexBucketItem(bucket, hash));
                                } else {
                                    BinaryFileStoreValue value = convertor.fromStoreBlock(valueBlock);
                                    keys.add(value.getOriginalStoreKey());
                                }
                            }
                        } catch (ContentStoreBlockException ex) {
                            LOGGER.error("Convert block to binary store value failed.", ex);

                            if (slave != null && bucket.getSlaveNumber() != IndexBucket.REMOVED_SLAVE_NUMBER) {
                                try {
                                    slave.delete();
                                    forRemovingIndexBuckets.add(new RemovingIndexBucketItem(bucket, hash));
                                } catch (ContentStoreSlaveException exce) {
                                    LOGGER.error("Delete error store slave failed.", exce);
                                }
                            }

                        } catch (ContentStoreSlaveException ex) {
                            LOGGER.error("Read block from store slave failed.", ex);

                            if (slave != null && bucket.getSlaveNumber() != IndexBucket.REMOVED_SLAVE_NUMBER) {
                                try {
                                    slave.delete();
                                    forRemovingIndexBuckets.add(new RemovingIndexBucketItem(bucket, hash));
                                } catch (ContentStoreSlaveException exce) {
                                    LOGGER.error("Delete error store slave failed.", exce);
                                }
                            }

                        } catch (ContentStoreMasterException ex) {
                            LOGGER.error("Retrieve store slave failed.", ex);

                            if (slave != null && bucket.getSlaveNumber() != IndexBucket.REMOVED_SLAVE_NUMBER) {
                                try {
                                    slave.delete();
                                    forRemovingIndexBuckets.add(new RemovingIndexBucketItem(bucket, hash));
                                } catch (ContentStoreSlaveException exce) {
                                    LOGGER.error("Delete error store slave failed.", exce);
                                }
                            }

                        } catch (Throwable ex) {
                            LOGGER.error(ex.getMessage(), ex);

                            if (slave != null && bucket.getSlaveNumber() != IndexBucket.REMOVED_SLAVE_NUMBER) {
                                try {
                                    slave.delete();
                                    forRemovingIndexBuckets.add(new RemovingIndexBucketItem(bucket, hash));
                                } catch (ContentStoreSlaveException exce) {
                                    LOGGER.error("Delete error store slave failed.", exce);
                                }
                            }
                        }
                    }
                }

                if (forRemovingIndexBuckets.isEmpty()) {
                    return Collections.unmodifiableSet(keys);
                }
            } finally {
                gridLock.readLock().unlock();
            }

            gridLock.writeLock().lock();
            try {
                for (RemovingIndexBucketItem removingBucketItem : forRemovingIndexBuckets) {
                    try {
                        IndexBucket removingBucket = removingBucketItem.forRemovingIndexBucket;
                        int hash = removingBucketItem.forRemovingIndexBucketHash;
                        LinkedList<IndexBucket> bucketsByHash = buckets.get(hash);

                        storeBucket2MetaFile(removingBucket, IndexBucket.REMOVED_SLAVE_NUMBER, true, IndexBucket.NULL_HASH_VALUE);
                        if (bucketsByHash != null && bucketsByHash.contains(removingBucket)) {
                            bucketsByHash.remove(removingBucket);
                            if (bucketsByHash.size() == 0) {
                                buckets.remove(hash);
                            }

                            removedBuckets.add(removingBucket);

                            if (entryRemovedListener != null) {
                                entryRemovedListener.entryRemoved();
                            }
                        }
                    } catch (IOException exce) {
                        LOGGER.error("Store deleted bucket meta data to file failed.", exce);
                    }
                }

                return Collections.unmodifiableSet(keys);
            } finally {
                gridLock.writeLock().unlock();
            }
        }
    }

    List<StoreEntry> getAllStoreValues(ContentStoreMaster storeMaster, BinaryFileStoreConvertor convertor, boolean ignoreErrorEntry,
                                       ErrorEntryRemovedListener entryRemovedListener) throws StoreReaderException {
        if (!ignoreErrorEntry) {
            try {
                List<StoreEntry> values = new LinkedList<StoreEntry>();
                LinkedList<RemovingIndexBucketItem> forRemovingIndexBuckets = new LinkedList<RemovingIndexBucketItem>();

                gridLock.readLock().lock();
                try {
                    if (removedBuckets.size() >= bucketSize.intValue()) {
                        return values;
                    }

                    for (Entry<Integer, LinkedList<IndexBucket>> bucketsByHashEntry : buckets.entrySet()) {
                        int hash = bucketsByHashEntry.getKey();
                        LinkedList<IndexBucket> bucketsByHash = bucketsByHashEntry.getValue();

                        for (IndexBucket bucket : bucketsByHash) {
                            ContentStoreSlave slave = storeMaster.retrieveSlave(bucket.getSlaveNumber());
                            if (slave == null) {
                                forRemovingIndexBuckets.add(new RemovingIndexBucketItem(bucket, hash));
                            } else {
                                ContentStoreBlock valueBlock = slave.read();
                                if (valueBlock == null) {
                                    forRemovingIndexBuckets.add(new RemovingIndexBucketItem(bucket, hash));
                                } else {
                                    BinaryFileStoreValue value = convertor.fromStoreBlock(valueBlock);
                                    FileBaseBinaryFileStoreValueImpl binStoreValue = (FileBaseBinaryFileStoreValueImpl) value;
                                    StoreEntry entry = new StoreEntry(binStoreValue.getVersionNumber(), binStoreValue.getRealValue(), getKeyForStoreEntry(value));
                                    values.add(entry);
                                }
                            }
                        }
                    }

                    if (forRemovingIndexBuckets.isEmpty()) {
                        return values;
                    }
                } finally {
                    gridLock.readLock().unlock();
                }

                gridLock.writeLock().lock();
                try {
                    for (RemovingIndexBucketItem removingBucketItem : forRemovingIndexBuckets) {
                        try {
                            IndexBucket removingBucket = removingBucketItem.forRemovingIndexBucket;
                            int hash = removingBucketItem.forRemovingIndexBucketHash;
                            LinkedList<IndexBucket> bucketsByHash = buckets.get(hash);

                            storeBucket2MetaFile(removingBucket, IndexBucket.REMOVED_SLAVE_NUMBER, true, IndexBucket.NULL_HASH_VALUE);
                            if (bucketsByHash != null && bucketsByHash.contains(removingBucket)) {
                                bucketsByHash.remove(removingBucket);
                                if (bucketsByHash.size() == 0) {
                                    buckets.remove(hash);
                                }

                                removedBuckets.add(removingBucket);

                                if (entryRemovedListener != null) {
                                    entryRemovedListener.entryRemoved();
                                }
                            }
                        } catch (IOException exce) {
                            LOGGER.error("Store deleted bucket meta data to file failed.", exce);
                        }
                    }

                    return values;
                } finally {
                    gridLock.writeLock().unlock();
                }
            } catch (ContentStoreBlockException ex) {
                throw new StoreReaderException("Convert block to binary store value failed.", ex);
            } catch (ContentStoreSlaveException ex) {
                throw new StoreReaderException("Read block from store slave failed.", ex);
            } catch (ContentStoreMasterException ex) {
                throw new StoreReaderException("Retrieve store slave failed.", ex);
            } catch (Throwable ex) {
                throw new StoreReaderException(ex.getMessage(), ex);
            }
        } else {
            List<StoreEntry> values = new LinkedList<StoreEntry>();
            LinkedList<RemovingIndexBucketItem> forRemovingIndexBuckets = new LinkedList<RemovingIndexBucketItem>();

            gridLock.readLock().lock();
            try {
                if (removedBuckets.size() >= bucketSize.intValue()) {
                    return values;
                }

                for (Entry<Integer, LinkedList<IndexBucket>> bucketsByHashEntry : buckets.entrySet()) {
                    int hash = bucketsByHashEntry.getKey();
                    LinkedList<IndexBucket> bucketsByHash = bucketsByHashEntry.getValue();

                    for (IndexBucket bucket : bucketsByHash) {
                        ContentStoreSlave slave = null;

                        try {
                            slave = storeMaster.retrieveSlave(bucket.getSlaveNumber());
                            if (slave == null) {
                                forRemovingIndexBuckets.add(new RemovingIndexBucketItem(bucket, hash));
                            } else {
                                ContentStoreBlock valueBlock = slave.read();
                                if (valueBlock == null) {
                                    forRemovingIndexBuckets.add(new RemovingIndexBucketItem(bucket, hash));
                                } else {
                                    BinaryFileStoreValue value = convertor.fromStoreBlock(valueBlock);
                                    FileBaseBinaryFileStoreValueImpl binStoreValue = (FileBaseBinaryFileStoreValueImpl) value;
                                    StoreEntry entry = new StoreEntry(binStoreValue.getVersionNumber(), binStoreValue.getRealValue(), getKeyForStoreEntry(value));
                                    values.add(entry);
                                }
                            }
                        } catch (ContentStoreBlockException ex) {
                            LOGGER.error("Convert block to binary store value failed.", ex);

                            if (slave != null && bucket.getSlaveNumber() != IndexBucket.REMOVED_SLAVE_NUMBER) {
                                try {
                                    slave.delete();
                                    forRemovingIndexBuckets.add(new RemovingIndexBucketItem(bucket, hash));
                                } catch (ContentStoreSlaveException exce) {
                                    LOGGER.error("Delete error store slave failed.", exce);
                                }
                            }

                        } catch (ContentStoreSlaveException ex) {
                            LOGGER.error("Read block from store slave failed.", ex);

                            if (slave != null && bucket.getSlaveNumber() != IndexBucket.REMOVED_SLAVE_NUMBER) {
                                try {
                                    slave.delete();
                                    forRemovingIndexBuckets.add(new RemovingIndexBucketItem(bucket, hash));
                                } catch (ContentStoreSlaveException exce) {
                                    LOGGER.error("Delete error store slave failed.", exce);
                                }
                            }

                        } catch (ContentStoreMasterException ex) {
                            LOGGER.error("Retrieve store slave failed.", ex);

                            if (slave != null && bucket.getSlaveNumber() != IndexBucket.REMOVED_SLAVE_NUMBER) {
                                try {
                                    slave.delete();
                                    forRemovingIndexBuckets.add(new RemovingIndexBucketItem(bucket, hash));
                                } catch (ContentStoreSlaveException exce) {
                                    LOGGER.error("Delete error store slave failed.", exce);
                                }
                            }

                        } catch (Throwable ex) {
                            LOGGER.error(ex.getMessage(), ex);

                            if (slave != null && bucket.getSlaveNumber() != IndexBucket.REMOVED_SLAVE_NUMBER) {
                                try {
                                    slave.delete();
                                    forRemovingIndexBuckets.add(new RemovingIndexBucketItem(bucket, hash));
                                } catch (ContentStoreSlaveException exce) {
                                    LOGGER.error("Delete error store slave failed.", exce);
                                }
                            }

                        }
                    }
                }

                if (forRemovingIndexBuckets.isEmpty()) {
                    return values;
                }
            } finally {
                gridLock.readLock().unlock();
            }

            gridLock.writeLock().lock();
            try {
                for (RemovingIndexBucketItem removingBucketItem : forRemovingIndexBuckets) {
                    try {
                        IndexBucket removingBucket = removingBucketItem.forRemovingIndexBucket;
                        int hash = removingBucketItem.forRemovingIndexBucketHash;
                        LinkedList<IndexBucket> bucketsByHash = buckets.get(hash);

                        storeBucket2MetaFile(removingBucket, IndexBucket.REMOVED_SLAVE_NUMBER, true, IndexBucket.NULL_HASH_VALUE);
                        if (bucketsByHash != null && bucketsByHash.contains(removingBucket)) {
                            bucketsByHash.remove(removingBucket);
                            if (bucketsByHash.size() == 0) {
                                buckets.remove(hash);
                            }

                            removedBuckets.add(removingBucket);

                            if (entryRemovedListener != null) {
                                entryRemovedListener.entryRemoved();
                            }
                        }
                    } catch (IOException exce) {
                        LOGGER.error("Store deleted bucket meta data to file failed.", exce);
                    }
                }

                return values;
            } finally {
                gridLock.writeLock().unlock();
            }
        }
    }

    static final Object getKeyForStoreEntry(BinaryFileStoreValue value) {
        Object keyForStoreEntry = null;
        FileBaseBinaryFileStoreKeyImpl binKey = (FileBaseBinaryFileStoreKeyImpl) value.getOriginalStoreKey();
        StoreKey realKey = binKey.getRealKey();
        if (realKey instanceof StringStoreKey) {
            StringStoreKey stringStoreKey = (StringStoreKey) realKey;
            keyForStoreEntry = stringStoreKey.getKey();
        } else if (realKey instanceof LongStoreKey) {
            LongStoreKey longStoreKey = (LongStoreKey) realKey;
            keyForStoreEntry = longStoreKey.getKey();
        }

        return keyForStoreEntry;
    }

    int indexBucketSize() {
        gridLock.readLock().lock();
        try {
            return bucketSize.intValue();
        } finally {
            gridLock.readLock().unlock();
        }
    }

    int removedIndexBucketSize() {
        gridLock.readLock().lock();
        try {
            return removedBuckets.size();
        } finally {
            gridLock.readLock().unlock();
        }
    }

    static IndexGrid fromBytes(byte[] bytes, long gridStartAddress, FileChannel metaFileChannel,
                               boolean forceWrite, int maxGridBucketsNumber, float mallocSizeExtendFactor,
                               boolean ignoreErrorEntry) throws IOException {
        if (ignoreErrorEntry) {
            int gridBytesLength = IndexBucket.BUCKET_BYTES_LENGTH * maxGridBucketsNumber;
            if (bytes == null || bytes.length != gridBytesLength) {
                return new IndexGrid(gridStartAddress, metaFileChannel, forceWrite, maxGridBucketsNumber, mallocSizeExtendFactor);
            }

            IndexGrid grid = new IndexGrid(gridStartAddress, metaFileChannel, forceWrite, maxGridBucketsNumber, mallocSizeExtendFactor);
            grid.gridLock.writeLock().lock();
            try {
                byte[] bucketBytes = new byte[IndexBucket.BUCKET_BYTES_LENGTH];
                for (int i = 0; i < maxGridBucketsNumber; ++i) {
                    try {
                        System.arraycopy(bytes, i * IndexBucket.BUCKET_BYTES_LENGTH, bucketBytes, 0, IndexBucket.BUCKET_BYTES_LENGTH);
                        BucketFromBytesResult result = IndexBucket.fromBytes(bucketBytes, i, ignoreErrorEntry);
                        if (result == null) {
                            break;
                        } else {
                            if (result.isRemoved()) {
                                grid.removedBuckets.add(result.getBucket());
                            } else {
                                LinkedList<IndexBucket> bucketsByHash = grid.buckets.get(result.getHashValue());
                                if (bucketsByHash == null) {
                                    bucketsByHash = new LinkedList<IndexBucket>();
                                    grid.buckets.put(result.getHashValue(), bucketsByHash);
                                }

                                bucketsByHash.add(result.getBucket());
                            }

                            grid.bucketSize.increment();
                        }
                    } catch (Throwable ex) {
                        LOGGER.error("Read IndexGrid failed.", ex);
                        break;
                    }
                }

                return grid;
            } finally {
                grid.gridLock.writeLock().unlock();
            }
        } else {
            int gridBytesLength = IndexBucket.BUCKET_BYTES_LENGTH * maxGridBucketsNumber;
            if (bytes == null || bytes.length != gridBytesLength) {
                throw new IllegalArgumentException("Bytes length error. Expect length : " + gridBytesLength);
            }

            IndexGrid grid = new IndexGrid(gridStartAddress, metaFileChannel, forceWrite, maxGridBucketsNumber, mallocSizeExtendFactor);
            grid.gridLock.writeLock().lock();
            try {
                byte[] bucketBytes = new byte[IndexBucket.BUCKET_BYTES_LENGTH];
                for (int i = 0; i < maxGridBucketsNumber; ++i) {
                    System.arraycopy(bytes, i * IndexBucket.BUCKET_BYTES_LENGTH, bucketBytes, 0, IndexBucket.BUCKET_BYTES_LENGTH);
                    BucketFromBytesResult result = IndexBucket.fromBytes(bucketBytes, i, ignoreErrorEntry);
                    if (result == null) {
                        break;
                    } else {
                        if (result.isRemoved()) {
                            grid.removedBuckets.add(result.getBucket());
                        } else {
                            LinkedList<IndexBucket> bucketsByHash = grid.buckets.get(result.getHashValue());
                            if (bucketsByHash == null) {
                                bucketsByHash = new LinkedList<IndexBucket>();
                                grid.buckets.put(result.getHashValue(), bucketsByHash);
                            }

                            bucketsByHash.add(result.getBucket());
                        }

                        grid.bucketSize.increment();
                    }
                }

                return grid;
            } finally {
                grid.gridLock.writeLock().unlock();
            }
        }
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
}











