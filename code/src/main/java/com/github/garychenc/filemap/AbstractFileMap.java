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
package com.github.garychenc.filemap;

import com.github.garychenc.filemap.store.*;
import com.github.garychenc.filemap.store.filestore.BinaryFileStore;
import com.github.garychenc.filemap.store.filestore.FileBaseContentStoreMaster;
import com.github.garychenc.filemap.util.Asserts;
import com.github.garychenc.filemap.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Externalizable;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 *
 * @author Gary CHEN
 *
 */
abstract class AbstractFileMap implements FileMap {

    private final static Logger LOGGER = LoggerFactory.getLogger(AbstractFileMap.class);
    private final static String STORE_NAME_SEPARATOR = "-";
    private final static int A_BINARY_FILE_STORE_DEFAULT_MAX_SIZE = 500000;
    private final static float EXTEND_FACTOR_FOR_GRID_BUCKETS = 1.5F;
    private final static float EXTEND_FACTOR_FOR_CONTENT_STORE_SLAVE = 1.8F;
    private final static long INDIVIDUAL_CONTENT_SLAVE_STORE_FILE_SIZE = 64 * 1024 * 1024; // 64M , refer from GFS design
    private final static long DATA_STORE_FILE_GC_INTERVAL_IN_MS = 3 * 60 * 1000; // 3 min
    private final static float CONTENT_BLOCK_ALLOC_SIZE_EXTEND_FACTOR = 2.0F;

    private final static long NEW_FILE_MAP_STORE_EXTEND_INTERVAL_IN_MS = 2 * 60 * 1000; // 2 min
    private final static float EXTEND_FILE_MAP_STORE_WHEN_SPACE_LEFT = 0.45F; // 45%

    private final String storeDirPath;
    private final String storeName;
    private final boolean isFlushFileCacheEveryOperation;
    private final int maxGridBucketNumber;
    protected final boolean isStrictStorage;
    protected final int aFileMapStoreMaxSize;
    protected final CopyOnWriteArrayList<BinaryFileStore> fileMapStores;
    private volatile int lastStoreIndex = 0;
    private volatile boolean isFileMapClosed = false;
    private volatile Thread extendNewFileMapStoreThread = null;

    protected AbstractFileMap(String storeDirPath, String storeName) {
        this(storeDirPath, storeName, false, false, A_BINARY_FILE_STORE_DEFAULT_MAX_SIZE);
    }

    protected AbstractFileMap(String storeDirPath, String storeName, boolean isFlushFileCacheEveryOperation) {
        this(storeDirPath, storeName, isFlushFileCacheEveryOperation, false, A_BINARY_FILE_STORE_DEFAULT_MAX_SIZE);
    }

    protected AbstractFileMap(String storeDirPath, String storeName, boolean isFlushFileCacheEveryOperation, boolean isStrictStorage) {
        this(storeDirPath, storeName, isFlushFileCacheEveryOperation, isStrictStorage, A_BINARY_FILE_STORE_DEFAULT_MAX_SIZE);
    }

    protected AbstractFileMap(String storeDirPath, String storeName, boolean isFlushFileCacheEveryOperation, boolean isStrictStorage, int aFileMapStoreMaxSize) {
        Asserts.stringNotEmpty("storeDirPath", storeDirPath);
        Asserts.stringNotEmpty("storeName", storeName);
        Asserts.integerGtZero("aFileMapStoreMaxSize", aFileMapStoreMaxSize);

        int maxGridBucketNumber = (int) Math.sqrt(aFileMapStoreMaxSize * EXTEND_FACTOR_FOR_GRID_BUCKETS) + 1;
        if (maxGridBucketNumber < 16) {
            maxGridBucketNumber = 16;
        }

        ArrayList<BinaryFileStore> loadedFileMapStores = new ArrayList<>(1024);
        File storeDir = Utils.getOrCreateDir(storeDirPath);
        int lastStoreIndex = 0;
        while (true) {
            String fileMapStoreName = storeName + STORE_NAME_SEPARATOR + String.valueOf(lastStoreIndex);
            File mapStoreFile = new File(storeDir, fileMapStoreName);
            if (mapStoreFile.exists()) {
                if (!mapStoreFile.isDirectory()) {
                    if (!mapStoreFile.delete()) {
                        throw new IllegalStateException("Delete a path [" + mapStoreFile.getAbsolutePath() + "] that is not a directory failed when loading BinaryFileStore.");
                    }
                }

                FileBaseContentStoreMaster contentStoreMaster = new FileBaseContentStoreMaster(storeDirPath, fileMapStoreName, isFlushFileCacheEveryOperation,
                        (long) (aFileMapStoreMaxSize * EXTEND_FACTOR_FOR_CONTENT_STORE_SLAVE), INDIVIDUAL_CONTENT_SLAVE_STORE_FILE_SIZE,
                        isFlushFileCacheEveryOperation, DATA_STORE_FILE_GC_INTERVAL_IN_MS, !isStrictStorage);
                BinaryFileStore fileMapStore = new BinaryFileStore(storeDirPath, fileMapStoreName, isFlushFileCacheEveryOperation,
                        (int) ((int) (aFileMapStoreMaxSize * EXTEND_FACTOR_FOR_GRID_BUCKETS) / maxGridBucketNumber) + 1, maxGridBucketNumber, CONTENT_BLOCK_ALLOC_SIZE_EXTEND_FACTOR,
                        contentStoreMaster, !isStrictStorage);

                loadedFileMapStores.add(fileMapStore);
                ++lastStoreIndex;
            } else {
                break;
            }
        }

        this.storeDirPath = storeDirPath;
        this.storeName = storeName;
        this.isFlushFileCacheEveryOperation = isFlushFileCacheEveryOperation;
        this.maxGridBucketNumber = maxGridBucketNumber;
        this.isStrictStorage = isStrictStorage;
        this.aFileMapStoreMaxSize = aFileMapStoreMaxSize;
        this.fileMapStores = new CopyOnWriteArrayList(loadedFileMapStores);
        this.lastStoreIndex = lastStoreIndex;

        if (this.fileMapStores.isEmpty()) {
            try {
                extendNewFileMapStore();
            } catch (StoreWriterException ex) {
                throw new IllegalStateException(ex);
            }
        }

        this.extendNewFileMapStoreThread = new Thread(new ExtendNewFileMapStoreTask(), "ExtendNewFileMapStoreTask");
        this.extendNewFileMapStoreThread.setDaemon(true);
        this.extendNewFileMapStoreThread.start();
    }

    protected synchronized BinaryFileStore extendNewFileMapStore() throws StoreWriterException {
        try {
            BinaryFileStore spaceAvailableMapStore = null;
            for (BinaryFileStore fileMapStore : this.fileMapStores) {
                try {
                    if (spaceAvailableMapStore == null && fileMapStore.size() < aFileMapStoreMaxSize) {
                        spaceAvailableMapStore = fileMapStore;
                        break;
                    }
                } catch (StoreReaderException ex) {
                    throw new StoreWriterException(ex);
                }
            }

            if (spaceAvailableMapStore != null) {
                return spaceAvailableMapStore;
            } else {
                File storeDir = Utils.getOrCreateDir(storeDirPath);
                String fileMapStoreName = storeName + STORE_NAME_SEPARATOR + String.valueOf(lastStoreIndex);
                File mapStoreFile = new File(storeDir, fileMapStoreName);
                if (mapStoreFile.exists()) {
                    if (!mapStoreFile.isDirectory()) {
                        if (!mapStoreFile.delete()) {
                            throw new IllegalStateException("Delete a path [" + mapStoreFile.getAbsolutePath() + "] that is not a directory failed when loading BinaryFileStore.");
                        }
                    }
                }

                FileBaseContentStoreMaster contentStoreMaster = new FileBaseContentStoreMaster(storeDirPath, fileMapStoreName, isFlushFileCacheEveryOperation,
                        (long) (aFileMapStoreMaxSize * EXTEND_FACTOR_FOR_CONTENT_STORE_SLAVE), INDIVIDUAL_CONTENT_SLAVE_STORE_FILE_SIZE,
                        isFlushFileCacheEveryOperation, DATA_STORE_FILE_GC_INTERVAL_IN_MS, !isStrictStorage);
                BinaryFileStore fileMapStore = new BinaryFileStore(storeDirPath, fileMapStoreName, isFlushFileCacheEveryOperation,
                        (int) ((int) (aFileMapStoreMaxSize * EXTEND_FACTOR_FOR_GRID_BUCKETS) / maxGridBucketNumber), maxGridBucketNumber, CONTENT_BLOCK_ALLOC_SIZE_EXTEND_FACTOR,
                        contentStoreMaster, !isStrictStorage);

                this.fileMapStores.add(fileMapStore);
                ++lastStoreIndex;

                return fileMapStore;
            }
        } catch (Throwable ex) {
            throw new StoreWriterException("Extend new file map store failed.", ex);
        }
    }

    private final class ExtendNewFileMapStoreTask implements Runnable {

        @Override
        public void run() {
            while (true) {
                boolean fileMapClosed = false;
                int fileMapStoreMaxSize = 0;
                synchronized (AbstractFileMap.this) {
                    fileMapClosed = AbstractFileMap.this.isFileMapClosed;
                    fileMapStoreMaxSize = AbstractFileMap.this.aFileMapStoreMaxSize;
                }

                if (fileMapClosed) {
                    LOGGER.info("File map is closed, ExtendNewFileMapStoreTask finished.");
                    break;
                }

                try {
                    Thread.sleep(NEW_FILE_MAP_STORE_EXTEND_INTERVAL_IN_MS);
                } catch (InterruptedException e) {
                    LOGGER.info("File map is closed, ExtendNewFileMapStoreTask finished.");
                    break;
                }

                BinaryFileStore lastFileMapStore = null;
                for (BinaryFileStore fileMapStore : AbstractFileMap.this.fileMapStores) {
                    lastFileMapStore = fileMapStore;
                }

                if (lastFileMapStore != null) {
                    try {
                        float lastFileMapStoreOccupiedSpace = (float) lastFileMapStore.size() / (float) fileMapStoreMaxSize;
                        if ((1.0F - lastFileMapStoreOccupiedSpace) <= EXTEND_FILE_MAP_STORE_WHEN_SPACE_LEFT) {
                            synchronized (AbstractFileMap.this) {
                                File storeDir = Utils.getOrCreateDir(AbstractFileMap.this.storeDirPath);
                                String fileMapStoreName = AbstractFileMap.this.storeName + STORE_NAME_SEPARATOR + String.valueOf(AbstractFileMap.this.lastStoreIndex);
                                File mapStoreFile = new File(storeDir, fileMapStoreName);
                                if (mapStoreFile.exists()) {
                                    if (!mapStoreFile.isDirectory()) {
                                        if (!mapStoreFile.delete()) {
                                            throw new IllegalStateException("Delete a path [" + mapStoreFile.getAbsolutePath() + "] that is not a directory failed when loading BinaryFileStore.");
                                        }
                                    }
                                }

                                FileBaseContentStoreMaster contentStoreMaster = new FileBaseContentStoreMaster(AbstractFileMap.this.storeDirPath, fileMapStoreName, AbstractFileMap.this.isFlushFileCacheEveryOperation,
                                        (long) (AbstractFileMap.this.aFileMapStoreMaxSize * EXTEND_FACTOR_FOR_CONTENT_STORE_SLAVE), INDIVIDUAL_CONTENT_SLAVE_STORE_FILE_SIZE,
                                        AbstractFileMap.this.isFlushFileCacheEveryOperation, DATA_STORE_FILE_GC_INTERVAL_IN_MS, !AbstractFileMap.this.isStrictStorage);
                                BinaryFileStore fileMapStore = new BinaryFileStore(AbstractFileMap.this.storeDirPath, fileMapStoreName, AbstractFileMap.this.isFlushFileCacheEveryOperation,
                                        (int) ((int) (AbstractFileMap.this.aFileMapStoreMaxSize * EXTEND_FACTOR_FOR_GRID_BUCKETS) / AbstractFileMap.this.maxGridBucketNumber), AbstractFileMap.this.maxGridBucketNumber, CONTENT_BLOCK_ALLOC_SIZE_EXTEND_FACTOR,
                                        contentStoreMaster, !AbstractFileMap.this.isStrictStorage);

                                AbstractFileMap.this.fileMapStores.add(fileMapStore);
                                ++AbstractFileMap.this.lastStoreIndex;
                            }
                        }
                    } catch (Throwable ex) {
                        LOGGER.error("Extend new file map store failed.", ex);
                    }
                }
            }
        }
    }

    @Override
    public StoreEntry read(String key) throws StoreReaderException {
        for (BinaryFileStore fileMapStore : this.fileMapStores) {
            StoreEntry v = fileMapStore.read(key, !isStrictStorage);
            if (v != null) {
                return v;
            }
        }

        return null;
    }

    @Override
    public StoreEntry read(long key) throws StoreReaderException {
        for (BinaryFileStore fileMapStore : this.fileMapStores) {
            StoreEntry v = fileMapStore.read(key, !isStrictStorage);
            if (v != null) {
                return v;
            }
        }

        return null;
    }

    @Override
    public Set<Object> keySet() throws StoreReaderException {
        Set<Object> keys = new LinkedHashSet<>();
        for (BinaryFileStore fileMapStore : this.fileMapStores) {
            keys.addAll(fileMapStore.keySet(!isStrictStorage));
        }

        return Collections.unmodifiableSet(keys);
    }

    @Override
    public List<StoreEntry> values() throws StoreReaderException {
        List<StoreEntry> values = new LinkedList<>();
        for (BinaryFileStore fileMapStore : this.fileMapStores) {
            values.addAll(fileMapStore.values(!isStrictStorage));
        }

        return Collections.unmodifiableList(values);
    }

    @Override
    public long size() throws StoreReaderException {
        long size = 0;
        for (BinaryFileStore fileMapStore : this.fileMapStores) {
            size += fileMapStore.size();
        }

        return size;
    }

    @Override
    public boolean remove(String key, long versionNumber) throws StoreWriterException, VersionConflictedException {
        for (BinaryFileStore fileMapStore : this.fileMapStores) {
            if (fileMapStore.remove(key, versionNumber)) {
                return true;
            }
        }

        return false;
    }

    @Override
    public boolean remove(long key, long versionNumber) throws StoreWriterException, VersionConflictedException {
        for (BinaryFileStore fileMapStore : this.fileMapStores) {
            if (fileMapStore.remove(key, versionNumber)) {
                return true;
            }
        }

        return false;
    }

    @Override
    public boolean update(String key, byte[] value, long versionNumber) throws StoreWriterException, VersionConflictedException {
        for (BinaryFileStore fileMapStore : this.fileMapStores) {
            if (fileMapStore.update(key, value, versionNumber)) {
                return true;
            }
        }

        return false;
    }

    @Override
    public boolean update(long key, byte[] value, long versionNumber) throws StoreWriterException, VersionConflictedException {
        for (BinaryFileStore fileMapStore : this.fileMapStores) {
            if (fileMapStore.update(key, value, versionNumber)) {
                return true;
            }
        }

        return false;
    }

    @Override
    public boolean update(String key, String value, long versionNumber) throws StoreWriterException, VersionConflictedException {
        for (BinaryFileStore fileMapStore : this.fileMapStores) {
            if (fileMapStore.update(key, value, versionNumber)) {
                return true;
            }
        }

        return false;
    }

    @Override
    public boolean update(long key, String value, long versionNumber) throws StoreWriterException, VersionConflictedException {
        for (BinaryFileStore fileMapStore : this.fileMapStores) {
            if (fileMapStore.update(key, value, versionNumber)) {
                return true;
            }
        }

        return false;
    }

    @Override
    public boolean update(String key, Externalizable value, long versionNumber) throws StoreWriterException, VersionConflictedException {
        for (BinaryFileStore fileMapStore : this.fileMapStores) {
            if (fileMapStore.update(key, value, versionNumber)) {
                return true;
            }
        }

        return false;
    }

    @Override
    public boolean update(long key, Externalizable value, long versionNumber) throws StoreWriterException, VersionConflictedException {
        for (BinaryFileStore fileMapStore : this.fileMapStores) {
            if (fileMapStore.update(key, value, versionNumber)) {
                return true;
            }
        }

        return false;
    }

    @Override
    public boolean update(String key, Serializable value, long versionNumber) throws StoreWriterException, VersionConflictedException {
        for (BinaryFileStore fileMapStore : this.fileMapStores) {
            if (fileMapStore.update(key, value, versionNumber)) {
                return true;
            }
        }

        return false;
    }

    @Override
    public boolean update(long key, Serializable value, long versionNumber) throws StoreWriterException, VersionConflictedException {
        for (BinaryFileStore fileMapStore : this.fileMapStores) {
            if (fileMapStore.update(key, value, versionNumber)) {
                return true;
            }
        }

        return false;
    }

    @Override
    public void close() throws IOException {
        synchronized (this) {
            for (BinaryFileStore fileMapStore : this.fileMapStores) {
                try { fileMapStore.close(); } catch (Throwable ex) { }
            }

            this.fileMapStores.clear();
            this.lastStoreIndex = 0;
            this.isFileMapClosed = true;

            if (this.extendNewFileMapStoreThread != null) {
                this.extendNewFileMapStoreThread.interrupt();
                this.extendNewFileMapStoreThread = null;
            }
        }
    }

}
