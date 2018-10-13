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
package com.github.garychenc.filemap.store;

/**
 * 封装了值、版本号和 key 的搜索结果对象，versionNumber 版本号，value 存储的值，key 用于读取值的关键字。
 *
 * @author Gary CHEN
 *
 */
public class StoreEntry {

    private final long versionNumber;
    private final Object value;
    private final Object key;
    
    public StoreEntry(long versionNumber, Object value, Object key) {
        this.versionNumber = versionNumber;
        this.value = value;
        this.key = key;
    }

    public long getVersionNumber() {
        return versionNumber;
    }

    public Object getValue() {
        return value;
    }

    public Object getKey() {
        return key;
    }

    @Override
    public String toString() {
        return "StoreEntry [versionNumber=" + versionNumber + ", value=" + value + ", key=" + key + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((value == null) ? 0 : value.hashCode());
        result = prime * result + (int) (versionNumber ^ (versionNumber >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof StoreEntry)) {
            return false;
        }
        StoreEntry other = (StoreEntry) obj;
        if (value == null) {
            if (other.value != null) {
                return false;
            }
        } else if (!value.equals(other.value)) {
            return false;
        }
        if (versionNumber != other.versionNumber) {
            return false;
        }
        return true;
    }
    
}
