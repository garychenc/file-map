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

import java.lang.ref.SoftReference;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 *
 * @author Gary CHEN
 *
 */
class SimpleObjectCacheImpl <K, V> implements ObjectCache <K, V> {

    private final int cacheSize;
    private final ConcurrentMap<K, SoftReference<V>> cache;

    SimpleObjectCacheImpl(int cacheSize) {
        assert cacheSize > 0;
        this.cacheSize = cacheSize;
        this.cache = new ConcurrentHashMap<K, SoftReference<V>>();
    }

    /* (non-Javadoc)
     * @see scnu.cs.spider.cache.ObjectCache#clear()
     */
    public void clear() {
        cache.clear();
    }

    private void clearCacheIfNecessary() {
        int size = cache.size();
        if (size >= cacheSize) {
            cache.clear();
        }
    }    
    
    /* (non-Javadoc)
     * @see scnu.cs.spider.cache.ObjectCache#get(java.lang.Object)
     */
    public V get(K key) {
        try {
            SoftReference<V> valueRef = cache.get(key);
            if (valueRef == null) {
                return null;
            }
            else {
                V value = valueRef.get();
                if (value == null && cache.containsKey(key)) {
                    cache.remove(key);
                }
                
                return value;
            }
        } finally {
            clearCacheIfNecessary();
        }
    }

    /* (non-Javadoc)
     * @see scnu.cs.spider.cache.ObjectCache#put(java.lang.Object, java.lang.Object)
     */
    public V put(K key, V value) {
        clearCacheIfNecessary();
        SoftReference<V> valueRef = cache.put(key, new SoftReference<V>(value));
        if (valueRef == null) {
            return null;
        }
        else {
            return valueRef.get();
        }
    }

    /* (non-Javadoc)
     * @see scnu.cs.spider.cache.ObjectCache#remove(java.lang.Object)
     */
    public V remove(K key) {
        SoftReference<V> valueRef = cache.remove(key);
        if (valueRef == null) {
            return null;
        }
        else {
            return valueRef.get();
        }
    }

}
