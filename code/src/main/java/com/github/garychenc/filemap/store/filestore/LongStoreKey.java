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
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.github.garychenc.filemap.util.Asserts;

/**
 *
 * @author Gary CHEN
 *
 */
public class LongStoreKey implements StoreKey, Externalizable, Comparable <LongStoreKey> {
    
    private static final int CLASS_VERSION = 1;
    private String uuid;
    private long key;
    
    public LongStoreKey() {
    }
    
    public LongStoreKey(String uuid, long key) {
        Asserts.stringNotEmpty("uuid", uuid);
        this.uuid = uuid;
        this.key = key;
    }

    /* (non-Javadoc)
     */
    @Override
    public String getUuid() {
        return uuid;
    }

    public void setUUID(String uuid) {
        Asserts.stringNotEmpty("uuid", uuid);
        this.uuid = uuid;
    }

    /* (non-Javadoc)
     * @see java.io.Externalizable#readExternal(java.io.ObjectInput)
     */
    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        in.readInt();
        this.key = in.readLong();
        this.uuid = in.readUTF();
    }

    /* (non-Javadoc)
     * @see java.io.Externalizable#writeExternal(java.io.ObjectOutput)
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(CLASS_VERSION);
        out.writeLong(key);
        out.writeUTF(uuid);
    }

    public long getKey() {
        return key;
    }

    public void setKey(long key) {
        this.key = key;
    }

    public String toString() {
        StringBuilder description = new StringBuilder();
        description.append("LongStoreKey [uuid = ").append(uuid).append(", key = ").append(key).append("]");
        return description.toString();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (key ^ (key >>> 32));
        result = prime * result + ((uuid == null) ? 0 : uuid.hashCode());
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
        if (!(obj instanceof LongStoreKey)) {
            return false;
        }
        LongStoreKey other = (LongStoreKey) obj;
        if (key != other.key) {
            return false;
        }
        if (uuid == null) {
            if (other.uuid != null) {
                return false;
            }
        } else if (!uuid.equals(other.uuid)) {
            return false;
        }
        return true;
    }

    @Override
    public int compareTo(LongStoreKey another) {
        if (this.getKey() > another.getKey()) {
            return 1;
        } else if (this.getKey() < another.getKey()) {
            return -1;
        } else {
            return 0;
        }
    }

    @Override
    public long getVersionNumber() {
        return 0;
    }

}
