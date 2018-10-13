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

import com.github.garychenc.filemap.store.StoreEntry;
import com.github.garychenc.filemap.store.StoreReaderException;
import com.github.garychenc.filemap.store.StoreWriterException;
import com.github.garychenc.filemap.store.VersionConflictedException;

import java.io.Closeable;
import java.io.Externalizable;
import java.io.Serializable;
import java.util.List;
import java.util.Set;

/**
 * FileMap 是一个基于文件的高性能本地 Key - Value 存储，可以理解为一个将数据存储在文件上的 Map，因此，其可存储巨大的数据量，而且具有持久化的效果。<br/>
 * <br/>
 * FileMap 使用非常简单，创建一个 com.github.garychenc.filemap.RepeatableKeyFileMap 对象或 com.github.garychenc.filemap.UniqueKeyFileMap 对象
 * 即可开始使用。创建对象的时候指定数据存储的本地目录即可。<br/>
 * <br/>
 * UniqueKeyFileMap 类确保添加新 Key - Value 的时候，新的 Key 与目前 Map 中所有的 Key 都不重复才能添加成功。<br/>
 * <br/>
 * RepeatableKeyFileMap 类允许添加新 Key - Value 的时候，新的 Key 可与原来 Map 中已经存在的 Key 重复。假如需要 Key 的唯一性，由客户端自己保证。<br/>
 * <br/>
 * UniqueKeyFileMap 类由于每次 add 操作都需要搜索 FileMap，确保即将添加的 key 不存在，所以性能会比 RepeatableKeyFileMap 差一些。而 RepeatableKeyFileMap
 * 则每次直接将记录添加到 FileMap 中，不进行判断，性能比 UniqueKeyFileMap 好一些，假如应用能够确保产生的 key 的唯一性，并且即使 key 重复也不造成大的影响，
 * 则使用 RepeatableKeyFileMap 性能好一些。<br/>
 * <br/>
 * FileMap 采用乐观离线锁对记录进行并发更新管理，所以，每次更新或者移除记录的时候需要提供预期的版本号，假如版本号不正确，说明在此之前记录已经被修改了，
 * 更新或移除此记录会抛出 VersionConflictedException，需要重新读出记录，拿到最新的版本号之后再进行更新或移除操作。一般操作模式为 ：<br/>
 * <br/>
 * <pre>
 *                        do {
 *                             try {
 *                                 StoreEntry testValue = fileMap.read("TEST-KEY-1");
 *                                 fileMap.update((String) testValue.getKey(), "TEST-VALUE-111100-1", testValue.getVersionNumber());
 *                                 break;
 *                             } catch (VersionConflictedException e) {
 *                                 continue;
 *                             } catch (StoreReaderException | StoreWriterException e) {
 *                                 e.printStackTrace();
 *                                 break;
 *                             }
 *                         } while (true);
 * </pre>
 * <br/>
 * FileMap 使用完毕之后，必须调用其 close() 方法关闭 FileMap，否则可能造成资源泄露。<br/>
 * <br/>
 * 使用示例 ：<br/>
 * <br/>
 * <pre>
 *         try (FileMap fileMap = new UniqueKeyFileMap(storeDir.getAbsolutePath(), "Test-1")) {
 *
 *             fileMap.add("TEST-KEY-1", "TEST-VALUE-1");
 *             fileMap.add("TEST-KEY-2", "TEST-VALUE-2");
 *
 *             System.out.println(fileMap.size()); // Size = 2
 *
 *             StoreEntry testValue1 = fileMap.read("TEST-KEY-1");
 *             StoreEntry testValue2 = fileMap.read("TEST-KEY-2");
 *
 *             System.out.println(testValue1.getValue()); // Value = "TEST-VALUE-1"
 *             System.out.println(testValue1.getVersionNumber()); // Version = 0
 *
 *             System.out.println(testValue2.getValue()); // Value = "TEST-VALUE-2"
 *             System.out.println(testValue2.getVersionNumber()); // Version = 0
 *
 *             fileMap.update("TEST-KEY-2", "TEST-VALUE-2-001", testValue2.getVersionNumber());
 *
 *             testValue2 = fileMap.read("TEST-KEY-2");
 *             System.out.println(testValue2.getValue()); // Value = "TEST-VALUE-2-001"
 *             System.out.println(testValue2.getVersionNumber()); // Version = 1
 *
 *             fileMap.remove("TEST-KEY-1", testValue1.getVersionNumber());
 *             fileMap.remove("TEST-KEY-2", testValue2.getVersionNumber());
 *         }
 * </pre>
 * <br/>
 * 该接口的实现类是线程安全的。<br/>
 * <br/>
 * @see com.github.garychenc.filemap.RepeatableKeyFileMap
 * @see com.github.garychenc.filemap.UniqueKeyFileMap
 * @author Gary CHEN
 *
 */
public interface FileMap extends Closeable {

    /**
     * 根据 key 读取 key 相对应的值，假如找到 key，则返回封装了 key 的值的 StoreEntry 对象，否则返回 null。
     * StoreEntry 包含 key 对应的值和该值对应的版本号，版本号从 0 开始，每更新一次，版本号加一，版本号用于更新，删除时使用。
     *
     * @param key 用于搜索值的 key
     * @return 假如找到，返回封装了值、版本号和 key 的 StoreEntry 对象，否则返回 null
     * @throws StoreReaderException 在文件存储中搜索 key 和读取其对应的值失败时抛出该异常
     */
    StoreEntry read(String key) throws StoreReaderException;

    /**
     * 根据 key 读取 key 相对应的值，假如找到 key，则返回封装了 key 的值的 StoreEntry 对象，否则返回 null。
     * StoreEntry 包含 key 对应的值和该值对应的版本号，版本号从 0 开始，每更新一次，版本号加一，版本号用于更新，删除时使用。
     *
     * @param key 用于搜索值的 key
     * @return 假如找到，返回封装了值、版本号和 key 的 StoreEntry 对象，否则返回 null
     * @throws StoreReaderException 在文件存储中搜索 key 和读取其对应的值失败时抛出该异常
     */
    StoreEntry read(long key) throws StoreReaderException;

    /**
     * 获取 FileMap 中键值对的键集合
     *
     * @return FileMap 中键值对的键集合，假如 FileMap 是空的返回空的集合，不会返回 null
     * @throws StoreReaderException 获取 Key 集合失败时抛出该异常
     */
    Set<Object> keySet() throws StoreReaderException;

    /**
     * 获取 FileMap 中键值对的值集合
     *
     * @return FileMap 中键值对的值集合，假如 FileMap 是空的返回空的集合，不会返回 null
     * @throws StoreReaderException 获取值集合失败时抛出该异常
     */
    List<StoreEntry> values() throws StoreReaderException;

    /**
     * 获取 FileMap 中键值对的数量
     *
     * @return FileMap 中键值对的数量
     * @throws StoreReaderException
     */
    long size() throws StoreReaderException;

    /**
     * 往 FileMap 中添加一个键值对，key 是否可重复由 FileMap 的实现决定。
     *
     * @param key 新添加记录的键，不能为空或者 null
     * @param value 新添加记录的值，不能为 null
     * @return 添加成功返回 ture，假如 key 不允许重复并且 key 已经存在，则返回 false
     * @throws StoreWriterException 添加失败时抛出该异常
     */
    boolean add(String key, byte[] value) throws StoreWriterException;

    /**
     * 往 FileMap 中添加一个键值对，key 是否可重复由 FileMap 的实现决定。
     *
     * @param key 新添加记录的键，不能为空或者 null
     * @param value 新添加记录的值，不能为 null
     * @return 添加成功返回 ture，假如 key 不允许重复并且 key 已经存在，则返回 false
     * @throws StoreWriterException 添加失败时抛出该异常
     */
    boolean add(long key, byte[] value) throws StoreWriterException;

    /**
     * 往 FileMap 中添加一个键值对，key 是否可重复由 FileMap 的实现决定。
     *
     * @param key 新添加记录的键，不能为空或者 null
     * @param value 新添加记录的值，不能为 null
     * @return 添加成功返回 ture，假如 key 不允许重复并且 key 已经存在，则返回 false
     * @throws StoreWriterException 添加失败时抛出该异常
     */
    boolean add(String key, String value) throws StoreWriterException;

    /**
     * 往 FileMap 中添加一个键值对，key 是否可重复由 FileMap 的实现决定。
     *
     * @param key 新添加记录的键，不能为空或者 null
     * @param value 新添加记录的值，不能为 null
     * @return 添加成功返回 ture，假如 key 不允许重复并且 key 已经存在，则返回 false
     * @throws StoreWriterException 添加失败时抛出该异常
     */
    boolean add(long key, String value) throws StoreWriterException;

    /**
     * 往 FileMap 中添加一个键值对，key 是否可重复由 FileMap 的实现决定。
     *
     * @param key 新添加记录的键，不能为空或者 null
     * @param value 新添加记录的值，不能为 null
     * @return 添加成功返回 ture，假如 key 不允许重复并且 key 已经存在，则返回 false
     * @throws StoreWriterException 添加失败时抛出该异常
     */
    boolean add(String key, Externalizable value) throws StoreWriterException;

    /**
     * 往 FileMap 中添加一个键值对，key 是否可重复由 FileMap 的实现决定。
     *
     * @param key 新添加记录的键，不能为空或者 null
     * @param value 新添加记录的值，不能为 null
     * @return 添加成功返回 ture，假如 key 不允许重复并且 key 已经存在，则返回 false
     * @throws StoreWriterException 添加失败时抛出该异常
     */
    boolean add(long key, Externalizable value) throws StoreWriterException;

    /**
     * 往 FileMap 中添加一个键值对，key 是否可重复由 FileMap 的实现决定。
     *
     * @param key 新添加记录的键，不能为空或者 null
     * @param value 新添加记录的值，不能为 null
     * @return 添加成功返回 ture，假如 key 不允许重复并且 key 已经存在，则返回 false
     * @throws StoreWriterException 添加失败时抛出该异常
     */
    boolean add(String key, Serializable value) throws StoreWriterException;

    /**
     * 往 FileMap 中添加一个键值对，key 是否可重复由 FileMap 的实现决定。
     *
     * @param key 新添加记录的键，不能为空或者 null
     * @param value 新添加记录的值，不能为 null
     * @return 添加成功返回 ture，假如 key 不允许重复并且 key 已经存在，则返回 false
     * @throws StoreWriterException 添加失败时抛出该异常
     */
    boolean add(long key, Serializable value) throws StoreWriterException;

    /**
     * 从 FileMap 中移除一个键值对。FileMap 采用乐观离线锁对记录进行并发更新管理，所以，每次更新或者移除记录的时候需要提供预期的版本号，
     * 假如版本号不正确，说明在此之前记录已经被修改了，更新或移除此记录会抛出 VersionConflictedException，需要重新读出记录，拿到最新的
     * 版本号之后再进行更新或移除操作。一般操作模式为 ：<br/>
     * <br/>
     * <pre>
     *                        do {
     *                             try {
     *                                 StoreEntry testValue = fileMap.read("TEST-KEY-1");
     *                                 fileMap.remove((String) testValue.getKey(), testValue.getVersionNumber());
     *                                 break;
     *                             } catch (VersionConflictedException e) {
     *                                 continue;
     *                             } catch (StoreReaderException | StoreWriterException e) {
     *                                 e.printStackTrace();
     *                                 break;
     *                             }
     *                         } while (true);
     * </pre>
     *
     * @param key 需移除记录的键，不能为空或者 null
     * @param versionNumber 需移除记录的版本号，一般在移除之前需要读出记录，获取最新的版本号
     * @return 移除成功返回 true，假如要移除的记录不存在，则返回 false 。
     * @throws StoreWriterException 移除失败时抛出该异常
     * @throws VersionConflictedException 版本号冲突时抛出该移除
     */
    boolean remove(String key, long versionNumber) throws StoreWriterException, VersionConflictedException;

    /**
     * 从 FileMap 中移除一个键值对。FileMap 采用乐观离线锁对记录进行并发更新管理，所以，每次更新或者移除记录的时候需要提供预期的版本号，
     * 假如版本号不正确，说明在此之前记录已经被修改了，更新或移除此记录会抛出 VersionConflictedException，需要重新读出记录，拿到最新的
     * 版本号之后再进行更新或移除操作。一般操作模式为 ：<br/>
     * <br/>
     * <pre>
     *                        do {
     *                             try {
     *                                 StoreEntry testValue = fileMap.read("TEST-KEY-1");
     *                                 fileMap.remove((String) testValue.getKey(), testValue.getVersionNumber());
     *                                 break;
     *                             } catch (VersionConflictedException e) {
     *                                 continue;
     *                             } catch (StoreReaderException | StoreWriterException e) {
     *                                 e.printStackTrace();
     *                                 break;
     *                             }
     *                         } while (true);
     * </pre>
     *
     * @param key 需移除记录的键，不能为空或者 null
     * @param versionNumber 需移除记录的版本号，一般在移除之前需要读出记录，获取最新的版本号
     * @return 移除成功返回 true，假如要移除的记录不存在，则返回 false 。
     * @throws StoreWriterException 移除失败时抛出该异常
     * @throws VersionConflictedException 版本号冲突时抛出该移除
     */
    boolean remove(long key, long versionNumber) throws StoreWriterException, VersionConflictedException;

    /**
     * 更新 key 对应的记录的值。FileMap 采用乐观离线锁对记录进行并发更新管理，所以，每次更新或者移除记录的时候需要提供预期的版本号，
     * 假如版本号不正确，说明在此之前记录已经被修改了，更新或移除此记录会抛出 VersionConflictedException，需要重新读出记录，拿到最新的
     * 版本号之后再进行更新或移除操作。一般操作模式为 ：<br/>
     * <br/>
     * <pre>
     *                        do {
     *                             try {
     *                                 StoreEntry testValue = fileMap.read("TEST-KEY-1");
     *                                 fileMap.update((String) testValue.getKey(), "TEST-VALUE-111100-1", testValue.getVersionNumber());
     *                                 break;
     *                             } catch (VersionConflictedException e) {
     *                                 continue;
     *                             } catch (StoreReaderException | StoreWriterException e) {
     *                                 e.printStackTrace();
     *                                 break;
     *                             }
     *                         } while (true);
     * </pre>
     *
     * @param key 需更新记录的键，不能为空或者 null
     * @param value 新的值
     * @param versionNumber 需更新记录的版本号，一般在更新之前需要读出记录，获取最新的版本号
     * @return 更新成功返回 true，假如要更新的记录不存在，则返回 false 。
     * @throws StoreWriterException 更新失败时抛出该异常
     * @throws VersionConflictedException 版本号冲突时抛出该移除
     */
    boolean update(String key, byte[] value, long versionNumber) throws StoreWriterException, VersionConflictedException;

    /**
     * 更新 key 对应的记录的值。FileMap 采用乐观离线锁对记录进行并发更新管理，所以，每次更新或者移除记录的时候需要提供预期的版本号，
     * 假如版本号不正确，说明在此之前记录已经被修改了，更新或移除此记录会抛出 VersionConflictedException，需要重新读出记录，拿到最新的
     * 版本号之后再进行更新或移除操作。一般操作模式为 ：<br/>
     * <br/>
     * <pre>
     *                        do {
     *                             try {
     *                                 StoreEntry testValue = fileMap.read("TEST-KEY-1");
     *                                 fileMap.update((String) testValue.getKey(), "TEST-VALUE-111100-1", testValue.getVersionNumber());
     *                                 break;
     *                             } catch (VersionConflictedException e) {
     *                                 continue;
     *                             } catch (StoreReaderException | StoreWriterException e) {
     *                                 e.printStackTrace();
     *                                 break;
     *                             }
     *                         } while (true);
     * </pre>
     *
     * @param key 需更新记录的键，不能为空或者 null
     * @param value 新的值
     * @param versionNumber 需更新记录的版本号，一般在更新之前需要读出记录，获取最新的版本号
     * @return 更新成功返回 true，假如要更新的记录不存在，则返回 false 。
     * @throws StoreWriterException 更新失败时抛出该异常
     * @throws VersionConflictedException 版本号冲突时抛出该移除
     */
    boolean update(long key, byte[] value, long versionNumber) throws StoreWriterException, VersionConflictedException;

    /**
     * 更新 key 对应的记录的值。FileMap 采用乐观离线锁对记录进行并发更新管理，所以，每次更新或者移除记录的时候需要提供预期的版本号，
     * 假如版本号不正确，说明在此之前记录已经被修改了，更新或移除此记录会抛出 VersionConflictedException，需要重新读出记录，拿到最新的
     * 版本号之后再进行更新或移除操作。一般操作模式为 ：<br/>
     * <br/>
     * <pre>
     *                        do {
     *                             try {
     *                                 StoreEntry testValue = fileMap.read("TEST-KEY-1");
     *                                 fileMap.update((String) testValue.getKey(), "TEST-VALUE-111100-1", testValue.getVersionNumber());
     *                                 break;
     *                             } catch (VersionConflictedException e) {
     *                                 continue;
     *                             } catch (StoreReaderException | StoreWriterException e) {
     *                                 e.printStackTrace();
     *                                 break;
     *                             }
     *                         } while (true);
     * </pre>
     *
     * @param key 需更新记录的键，不能为空或者 null
     * @param value 新的值
     * @param versionNumber 需更新记录的版本号，一般在更新之前需要读出记录，获取最新的版本号
     * @return 更新成功返回 true，假如要更新的记录不存在，则返回 false 。
     * @throws StoreWriterException 更新失败时抛出该异常
     * @throws VersionConflictedException 版本号冲突时抛出该移除
     */
    boolean update(String key, String value, long versionNumber) throws StoreWriterException, VersionConflictedException;

    /**
     * 更新 key 对应的记录的值。FileMap 采用乐观离线锁对记录进行并发更新管理，所以，每次更新或者移除记录的时候需要提供预期的版本号，
     * 假如版本号不正确，说明在此之前记录已经被修改了，更新或移除此记录会抛出 VersionConflictedException，需要重新读出记录，拿到最新的
     * 版本号之后再进行更新或移除操作。一般操作模式为 ：<br/>
     * <br/>
     * <pre>
     *                        do {
     *                             try {
     *                                 StoreEntry testValue = fileMap.read("TEST-KEY-1");
     *                                 fileMap.update((String) testValue.getKey(), "TEST-VALUE-111100-1", testValue.getVersionNumber());
     *                                 break;
     *                             } catch (VersionConflictedException e) {
     *                                 continue;
     *                             } catch (StoreReaderException | StoreWriterException e) {
     *                                 e.printStackTrace();
     *                                 break;
     *                             }
     *                         } while (true);
     * </pre>
     *
     * @param key 需更新记录的键，不能为空或者 null
     * @param value 新的值
     * @param versionNumber 需更新记录的版本号，一般在更新之前需要读出记录，获取最新的版本号
     * @return 更新成功返回 true，假如要更新的记录不存在，则返回 false 。
     * @throws StoreWriterException 更新失败时抛出该异常
     * @throws VersionConflictedException 版本号冲突时抛出该移除
     */
    boolean update(long key, String value, long versionNumber) throws StoreWriterException, VersionConflictedException;

    /**
     * 更新 key 对应的记录的值。FileMap 采用乐观离线锁对记录进行并发更新管理，所以，每次更新或者移除记录的时候需要提供预期的版本号，
     * 假如版本号不正确，说明在此之前记录已经被修改了，更新或移除此记录会抛出 VersionConflictedException，需要重新读出记录，拿到最新的
     * 版本号之后再进行更新或移除操作。一般操作模式为 ：<br/>
     * <br/>
     * <pre>
     *                        do {
     *                             try {
     *                                 StoreEntry testValue = fileMap.read("TEST-KEY-1");
     *                                 fileMap.update((String) testValue.getKey(), "TEST-VALUE-111100-1", testValue.getVersionNumber());
     *                                 break;
     *                             } catch (VersionConflictedException e) {
     *                                 continue;
     *                             } catch (StoreReaderException | StoreWriterException e) {
     *                                 e.printStackTrace();
     *                                 break;
     *                             }
     *                         } while (true);
     * </pre>
     *
     * @param key 需更新记录的键，不能为空或者 null
     * @param value 新的值
     * @param versionNumber 需更新记录的版本号，一般在更新之前需要读出记录，获取最新的版本号
     * @return 更新成功返回 true，假如要更新的记录不存在，则返回 false 。
     * @throws StoreWriterException 更新失败时抛出该异常
     * @throws VersionConflictedException 版本号冲突时抛出该移除
     */
    boolean update(String key, Externalizable value, long versionNumber) throws StoreWriterException, VersionConflictedException;

    /**
     * 更新 key 对应的记录的值。FileMap 采用乐观离线锁对记录进行并发更新管理，所以，每次更新或者移除记录的时候需要提供预期的版本号，
     * 假如版本号不正确，说明在此之前记录已经被修改了，更新或移除此记录会抛出 VersionConflictedException，需要重新读出记录，拿到最新的
     * 版本号之后再进行更新或移除操作。一般操作模式为 ：<br/>
     * <br/>
     * <pre>
     *                        do {
     *                             try {
     *                                 StoreEntry testValue = fileMap.read("TEST-KEY-1");
     *                                 fileMap.update((String) testValue.getKey(), "TEST-VALUE-111100-1", testValue.getVersionNumber());
     *                                 break;
     *                             } catch (VersionConflictedException e) {
     *                                 continue;
     *                             } catch (StoreReaderException | StoreWriterException e) {
     *                                 e.printStackTrace();
     *                                 break;
     *                             }
     *                         } while (true);
     * </pre>
     *
     * @param key 需更新记录的键，不能为空或者 null
     * @param value 新的值
     * @param versionNumber 需更新记录的版本号，一般在更新之前需要读出记录，获取最新的版本号
     * @return 更新成功返回 true，假如要更新的记录不存在，则返回 false 。
     * @throws StoreWriterException 更新失败时抛出该异常
     * @throws VersionConflictedException 版本号冲突时抛出该移除
     */
    boolean update(long key, Externalizable value, long versionNumber) throws StoreWriterException, VersionConflictedException;

    /**
     * 更新 key 对应的记录的值。FileMap 采用乐观离线锁对记录进行并发更新管理，所以，每次更新或者移除记录的时候需要提供预期的版本号，
     * 假如版本号不正确，说明在此之前记录已经被修改了，更新或移除此记录会抛出 VersionConflictedException，需要重新读出记录，拿到最新的
     * 版本号之后再进行更新或移除操作。一般操作模式为 ：<br/>
     * <br/>
     * <pre>
     *                        do {
     *                             try {
     *                                 StoreEntry testValue = fileMap.read("TEST-KEY-1");
     *                                 fileMap.update((String) testValue.getKey(), "TEST-VALUE-111100-1", testValue.getVersionNumber());
     *                                 break;
     *                             } catch (VersionConflictedException e) {
     *                                 continue;
     *                             } catch (StoreReaderException | StoreWriterException e) {
     *                                 e.printStackTrace();
     *                                 break;
     *                             }
     *                         } while (true);
     * </pre>
     *
     * @param key 需更新记录的键，不能为空或者 null
     * @param value 新的值
     * @param versionNumber 需更新记录的版本号，一般在更新之前需要读出记录，获取最新的版本号
     * @return 更新成功返回 true，假如要更新的记录不存在，则返回 false 。
     * @throws StoreWriterException 更新失败时抛出该异常
     * @throws VersionConflictedException 版本号冲突时抛出该移除
     */
    boolean update(String key, Serializable value, long versionNumber) throws StoreWriterException, VersionConflictedException;

    /**
     * 更新 key 对应的记录的值。FileMap 采用乐观离线锁对记录进行并发更新管理，所以，每次更新或者移除记录的时候需要提供预期的版本号，
     * 假如版本号不正确，说明在此之前记录已经被修改了，更新或移除此记录会抛出 VersionConflictedException，需要重新读出记录，拿到最新的
     * 版本号之后再进行更新或移除操作。一般操作模式为 ：<br/>
     * <br/>
     * <pre>
     *                        do {
     *                             try {
     *                                 StoreEntry testValue = fileMap.read("TEST-KEY-1");
     *                                 fileMap.update((String) testValue.getKey(), "TEST-VALUE-111100-1", testValue.getVersionNumber());
     *                                 break;
     *                             } catch (VersionConflictedException e) {
     *                                 continue;
     *                             } catch (StoreReaderException | StoreWriterException e) {
     *                                 e.printStackTrace();
     *                                 break;
     *                             }
     *                         } while (true);
     * </pre>
     *
     * @param key 需更新记录的键，不能为空或者 null
     * @param value 新的值
     * @param versionNumber 需更新记录的版本号，一般在更新之前需要读出记录，获取最新的版本号
     * @return 更新成功返回 true，假如要更新的记录不存在，则返回 false 。
     * @throws StoreWriterException 更新失败时抛出该异常
     * @throws VersionConflictedException 版本号冲突时抛出该移除
     */
    boolean update(long key, Serializable value, long versionNumber) throws StoreWriterException, VersionConflictedException;

}
