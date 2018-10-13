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

import java.io.Externalizable;
import java.io.Serializable;

/**
 * UniqueKeyFileMap 类为 FileMap 的一个实现，该类的 add 方法确保添加新 Key - Value 的时候，新的 Key 与目前 Map 中所有的 Key 都不重复才能添加成功。
 *
 * @author Gary CHEN
 *
 */
public class UniqueKeyFileMap extends AbstractFileMap {

    /**
     * 创建一个 UniqueKeyFileMap 对象。创建 UniqueKeyFileMap 对象失败时抛出运行时异常。
     *
     * @param storeDirPath FileMap 数据存储在文件上时的存储目录。
     * @param storeName FileMap 的存储名，存储名用于在同个存储目录中有多个 FileMap 存储时，区分不同的 FileMap 。
     *                  假如存储目录相同，则每个 FileMap 实例的存储名都必须不一样。两个不同的 FileMap 实例绝对不能使用相同的存储目录和存储名。
     *                  否则存储的数据可能会互相干扰，出现不可预知的错误。
     */
    public UniqueKeyFileMap(String storeDirPath, String storeName) {
        super(storeDirPath, storeName);
    }

    /**
     * 创建一个 UniqueKeyFileMap 对象。创建 UniqueKeyFileMap 对象失败时抛出运行时异常。
     *
     * @param storeDirPath FileMap 数据存储在文件上时的存储目录。
     * @param storeName FileMap 的存储名，存储名用于在同个存储目录中有多个 FileMap 存储时，区分不同的 FileMap 。
     *                  假如存储目录相同，则每个 FileMap 实例的存储名都必须不一样。两个不同的 FileMap 实例绝对不能使用相同的存储目录和存储名。
     *                  否则存储的数据可能会互相干扰，出现不可预知的错误。
     * @param isFlushFileCacheEveryOperation FileMap 写数据的时候默认先写在操作系统的文件缓存中，然后再刷新文件缓存的数据到磁盘上，该参数
     *                                       控制是否每次对 FileMap 进行 add，update，remove 操作之后都刷新文件缓存的数据到磁盘上。
     *                                       true 为每次都刷新，false 则等待 FileMap close 的时候再刷新。
     */
    public UniqueKeyFileMap(String storeDirPath, String storeName, boolean isFlushFileCacheEveryOperation) {
        super(storeDirPath, storeName, isFlushFileCacheEveryOperation);
    }

    /**
     * 创建一个 UniqueKeyFileMap 对象。创建 UniqueKeyFileMap 对象失败时抛出运行时异常。
     *
     * @param storeDirPath FileMap 数据存储在文件上时的存储目录。
     * @param storeName FileMap 的存储名，存储名用于在同个存储目录中有多个 FileMap 存储时，区分不同的 FileMap 。
     *                  假如存储目录相同，则每个 FileMap 实例的存储名都必须不一样。两个不同的 FileMap 实例绝对不能使用相同的存储目录和存储名。
     *                  否则存储的数据可能会互相干扰，出现不可预知的错误。
     * @param isFlushFileCacheEveryOperation FileMap 写数据的时候默认先写在操作系统的文件缓存中，然后再刷新文件缓存的数据到磁盘上，该参数
     *                                       控制是否每次对 FileMap 进行 add，update，remove 操作之后都刷新文件缓存的数据到磁盘上。
     *                                       true 为每次都刷新，false 则等待 FileMap close 的时候再刷新。
     * @param isStrictStorage 由于上述写数据的特性，所以，当 FileMap 被不正常关闭的时候，有可能出现某些数据丢失、损坏，而出现无法再次读取的情况。
     *                        该参数控制当出现数据无法读取的时候，是抛出异常，还是忽略该数据，返回 null，并且将该数据从 FileMap 中移除。
     *                        ture 抛出异常，false 忽略该数据。
     */
    public UniqueKeyFileMap(String storeDirPath, String storeName, boolean isFlushFileCacheEveryOperation, boolean isStrictStorage) {
        super(storeDirPath, storeName, isFlushFileCacheEveryOperation, isStrictStorage);
    }

    /**
     * 创建一个 UniqueKeyFileMap 对象。创建 UniqueKeyFileMap 对象失败时抛出运行时异常。
     *
     * @param storeDirPath FileMap 数据存储在文件上时的存储目录。
     * @param storeName FileMap 的存储名，存储名用于在同个存储目录中有多个 FileMap 存储时，区分不同的 FileMap 。
     *                  假如存储目录相同，则每个 FileMap 实例的存储名都必须不一样。两个不同的 FileMap 实例绝对不能使用相同的存储目录和存储名。
     *                  否则存储的数据可能会互相干扰，出现不可预知的错误。
     * @param isFlushFileCacheEveryOperation FileMap 写数据的时候默认先写在操作系统的文件缓存中，然后再刷新文件缓存的数据到磁盘上，该参数
     *                                       控制是否每次对 FileMap 进行 add，update，remove 操作之后都刷新文件缓存的数据到磁盘上。
     *                                       true 为每次都刷新，false 则等待 FileMap close 的时候再刷新。
     * @param isStrictStorage 由于上述写数据的特性，所以，当 FileMap 被不正常关闭的时候，有可能出现某些数据丢失、损坏，而出现无法再次读取的情况。
     *                        该参数控制当出现数据无法读取的时候，是抛出异常，还是忽略该数据，返回 null，并且将该数据从 FileMap 中移除。
     *                        ture 抛出异常，false 忽略该数据。
     * @param aFileMapStoreMaxSize FileMap 内部采用多个文件存储数据，该参数控制当一个文件存储的记录条数到达多少时创建新的文件存储新的数据。
     */
    public UniqueKeyFileMap(String storeDirPath, String storeName, boolean isFlushFileCacheEveryOperation, boolean isStrictStorage, int aFileMapStoreMaxSize) {
        super(storeDirPath, storeName, isFlushFileCacheEveryOperation, isStrictStorage, aFileMapStoreMaxSize);
    }

    @Override
    public boolean add(String key, byte[] value) throws StoreWriterException {
        synchronized (this) {
            while (true) {
                BinaryFileStore spaceAvailableMapStore = null;
                for (BinaryFileStore fileMapStore : this.fileMapStores) {
                    try {
                        StoreEntry v = fileMapStore.read(key, true);
                        if (v != null) {
                            return false;
                        }

                        if (spaceAvailableMapStore == null && fileMapStore.size() < aFileMapStoreMaxSize) {
                            spaceAvailableMapStore = fileMapStore;
                        }
                    } catch (StoreReaderException ex) {
                        throw new StoreWriterException("Check is key exist failed. Key : " + key, ex);
                    }
                }

                if (spaceAvailableMapStore != null) {
                    try {
                        spaceAvailableMapStore.add(key, value);
                        return true;
                    } catch (StoreIsFullException e) {
                        // continue;
                    }
                } else {
                    spaceAvailableMapStore = extendNewFileMapStore();

                    try {
                        spaceAvailableMapStore.add(key, value);
                        return true;
                    } catch (StoreIsFullException e) {
                        // continue;
                    }
                }
            }
        }
    }

    @Override
    public boolean add(long key, byte[] value) throws StoreWriterException {
        synchronized (this) {
            while (true) {
                BinaryFileStore spaceAvailableMapStore = null;
                for (BinaryFileStore fileMapStore : this.fileMapStores) {
                    try {
                        StoreEntry v = fileMapStore.read(key, true);
                        if (v != null) {
                            return false;
                        }

                        if (spaceAvailableMapStore == null && fileMapStore.size() < aFileMapStoreMaxSize) {
                            spaceAvailableMapStore = fileMapStore;
                        }
                    } catch (StoreReaderException ex) {
                        throw new StoreWriterException("Check is key exist failed. Key : " + key, ex);
                    }
                }

                if (spaceAvailableMapStore != null) {
                    try {
                        spaceAvailableMapStore.add(key, value);
                        return true;
                    } catch (StoreIsFullException e) {
                        // continue;
                    }
                } else {
                    spaceAvailableMapStore = extendNewFileMapStore();

                    try {
                        spaceAvailableMapStore.add(key, value);
                        return true;
                    } catch (StoreIsFullException e) {
                        // continue;
                    }
                }
            }
        }
    }

    @Override
    public boolean add(String key, String value) throws StoreWriterException {
        synchronized (this) {
            while (true) {
                BinaryFileStore spaceAvailableMapStore = null;
                for (BinaryFileStore fileMapStore : this.fileMapStores) {
                    try {
                        StoreEntry v = fileMapStore.read(key, true);
                        if (v != null) {
                            return false;
                        }

                        if (spaceAvailableMapStore == null && fileMapStore.size() < aFileMapStoreMaxSize) {
                            spaceAvailableMapStore = fileMapStore;
                        }
                    } catch (StoreReaderException ex) {
                        throw new StoreWriterException("Check is key exist failed. Key : " + key, ex);
                    }
                }

                if (spaceAvailableMapStore != null) {
                    try {
                        spaceAvailableMapStore.add(key, value);
                        return true;
                    } catch (StoreIsFullException e) {
                        // continue;
                    }
                } else {
                    spaceAvailableMapStore = extendNewFileMapStore();

                    try {
                        spaceAvailableMapStore.add(key, value);
                        return true;
                    } catch (StoreIsFullException e) {
                        // continue;
                    }
                }
            }
        }
    }

    @Override
    public boolean add(long key, String value) throws StoreWriterException {
        synchronized (this) {
            while (true) {
                BinaryFileStore spaceAvailableMapStore = null;
                for (BinaryFileStore fileMapStore : this.fileMapStores) {
                    try {
                        StoreEntry v = fileMapStore.read(key, true);
                        if (v != null) {
                            return false;
                        }

                        if (spaceAvailableMapStore == null && fileMapStore.size() < aFileMapStoreMaxSize) {
                            spaceAvailableMapStore = fileMapStore;
                        }
                    } catch (StoreReaderException ex) {
                        throw new StoreWriterException("Check is key exist failed. Key : " + key, ex);
                    }
                }

                if (spaceAvailableMapStore != null) {
                    try {
                        spaceAvailableMapStore.add(key, value);
                        return true;
                    } catch (StoreIsFullException e) {
                        // continue;
                    }
                } else {
                    spaceAvailableMapStore = extendNewFileMapStore();

                    try {
                        spaceAvailableMapStore.add(key, value);
                        return true;
                    } catch (StoreIsFullException e) {
                        // continue;
                    }
                }
            }
        }
    }

    @Override
    public boolean add(String key, Externalizable value) throws StoreWriterException {
        synchronized (this) {
            while (true) {
                BinaryFileStore spaceAvailableMapStore = null;
                for (BinaryFileStore fileMapStore : this.fileMapStores) {
                    try {
                        StoreEntry v = fileMapStore.read(key, true);
                        if (v != null) {
                            return false;
                        }

                        if (spaceAvailableMapStore == null && fileMapStore.size() < aFileMapStoreMaxSize) {
                            spaceAvailableMapStore = fileMapStore;
                        }
                    } catch (StoreReaderException ex) {
                        throw new StoreWriterException("Check is key exist failed. Key : " + key, ex);
                    }
                }

                if (spaceAvailableMapStore != null) {
                    try {
                        spaceAvailableMapStore.add(key, value);
                        return true;
                    } catch (StoreIsFullException e) {
                        // continue;
                    }
                } else {
                    spaceAvailableMapStore = extendNewFileMapStore();

                    try {
                        spaceAvailableMapStore.add(key, value);
                        return true;
                    } catch (StoreIsFullException e) {
                        // continue;
                    }
                }
            }
        }
    }

    @Override
    public boolean add(long key, Externalizable value) throws StoreWriterException {
        synchronized (this) {
            while (true) {
                BinaryFileStore spaceAvailableMapStore = null;
                for (BinaryFileStore fileMapStore : this.fileMapStores) {
                    try {
                        StoreEntry v = fileMapStore.read(key, true);
                        if (v != null) {
                            return false;
                        }

                        if (spaceAvailableMapStore == null && fileMapStore.size() < aFileMapStoreMaxSize) {
                            spaceAvailableMapStore = fileMapStore;
                        }
                    } catch (StoreReaderException ex) {
                        throw new StoreWriterException("Check is key exist failed. Key : " + key, ex);
                    }
                }

                if (spaceAvailableMapStore != null) {
                    try {
                        spaceAvailableMapStore.add(key, value);
                        return true;
                    } catch (StoreIsFullException e) {
                        // continue;
                    }
                } else {
                    spaceAvailableMapStore = extendNewFileMapStore();

                    try {
                        spaceAvailableMapStore.add(key, value);
                        return true;
                    } catch (StoreIsFullException e) {
                        // continue;
                    }
                }
            }
        }
    }

    @Override
    public boolean add(String key, Serializable value) throws StoreWriterException {
        synchronized (this) {
            while (true) {
                BinaryFileStore spaceAvailableMapStore = null;
                for (BinaryFileStore fileMapStore : this.fileMapStores) {
                    try {
                        StoreEntry v = fileMapStore.read(key, true);
                        if (v != null) {
                            return false;
                        }

                        if (spaceAvailableMapStore == null && fileMapStore.size() < aFileMapStoreMaxSize) {
                            spaceAvailableMapStore = fileMapStore;
                        }
                    } catch (StoreReaderException ex) {
                        throw new StoreWriterException("Check is key exist failed. Key : " + key, ex);
                    }
                }

                if (spaceAvailableMapStore != null) {
                    try {
                        spaceAvailableMapStore.add(key, value);
                        return true;
                    } catch (StoreIsFullException e) {
                        // continue;
                    }
                } else {
                    spaceAvailableMapStore = extendNewFileMapStore();

                    try {
                        spaceAvailableMapStore.add(key, value);
                        return true;
                    } catch (StoreIsFullException e) {
                        // continue;
                    }
                }
            }
        }
    }

    @Override
    public boolean add(long key, Serializable value) throws StoreWriterException {
        synchronized (this) {
            while (true) {
                BinaryFileStore spaceAvailableMapStore = null;
                for (BinaryFileStore fileMapStore : this.fileMapStores) {
                    try {
                        StoreEntry v = fileMapStore.read(key, true);
                        if (v != null) {
                            return false;
                        }

                        if (spaceAvailableMapStore == null && fileMapStore.size() < aFileMapStoreMaxSize) {
                            spaceAvailableMapStore = fileMapStore;
                        }
                    } catch (StoreReaderException ex) {
                        throw new StoreWriterException("Check is key exist failed. Key : " + key, ex);
                    }
                }

                if (spaceAvailableMapStore != null) {
                    try {
                        spaceAvailableMapStore.add(key, value);
                        return true;
                    } catch (StoreIsFullException e) {
                        // continue;
                    }
                } else {
                    spaceAvailableMapStore = extendNewFileMapStore();

                    try {
                        spaceAvailableMapStore.add(key, value);
                        return true;
                    } catch (StoreIsFullException e) {
                        // continue;
                    }
                }
            }
        }
    }

}
