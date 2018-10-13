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
public class StringStoreKey implements StoreKey, Externalizable {
    
    private static final int CLASS_VERSION = 1;
    private String uuid;
    private String key;

    public StringStoreKey() {
    }
    
    public StringStoreKey(String uuid, String key) {
        Asserts.stringNotEmpty("uuid", uuid);
        Asserts.stringNotEmpty("key", key);
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
        this.key = in.readUTF();
        this.uuid = in.readUTF();
    }

    /* (non-Javadoc)
     * @see java.io.Externalizable#writeExternal(java.io.ObjectOutput)
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(CLASS_VERSION);
        out.writeUTF(key);
        out.writeUTF(uuid);
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        Asserts.stringNotEmpty("key", key);
        this.key = key;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((key == null) ? 0 : key.hashCode());
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
        if (!(obj instanceof StringStoreKey)) {
            return false;
        }
        StringStoreKey other = (StringStoreKey) obj;
        if (key == null) {
            if (other.key != null) {
                return false;
            }
        } else if (!key.equals(other.key)) {
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

    public String toString() {
        StringBuilder description = new StringBuilder();
        description.append("StringStoreKey [uuid = ").append(uuid).append(", key = ").append(key).append("]");
        return description.toString();
    }

    @Override
    public long getVersionNumber() {
        return 0;
    }

}
