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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Externalizable;
import java.io.IOException;

import com.github.garychenc.filemap.util.*;

/**
 *
 * @author Gary CHEN
 *
 */
public class FileBaseBinaryFileStoreKeyImpl implements BinaryFileStoreKey {

    private final static int CLASS_VERSION = 1;
    private final static String NULL_OBJECT = "null";
    private final static String UseExternalizable = "UseExternalizable";
    private final static String UseSerializable = "UseSerializable";
   
    private volatile int hash = -1;
    private final StoreKey realKey;

    public FileBaseBinaryFileStoreKeyImpl(StoreKey realKey) {
        Asserts.notNull("realKey", realKey);
        this.realKey = realKey;
    }

    /* (non-Javadoc)
     */
    @Override
    public int getHashValue() {
        return hash;
    }

    /* (non-Javadoc)
     */
    @Override
    public void setHashValue(int hash) {
        this.hash = hash;
    }

    /* (non-Javadoc)
     */
    @Override
    public String getUuid() {
        return realKey.getUuid();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((realKey == null) ? 0 : realKey.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        FileBaseBinaryFileStoreKeyImpl other = (FileBaseBinaryFileStoreKeyImpl) obj;
        if (realKey == null) {
            if (other.realKey != null)
                return false;
        } else if (!realKey.equals(other.realKey))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "FileBaseBinaryFileStoreKeyImpl [realKey=" + realKey.toString() + "]";
    }

    private final static byte[] ZERO_BYTES = new byte[0];
    
    public static final byte[] toBytes(FileBaseBinaryFileStoreKeyImpl key) throws IOException {
        if (key == null) {
            return ZERO_BYTES;
        }

        try (ByteArrayOutputStream outputstream = new ByteArrayOutputStream(IOHelper.BUFFER_DEFAULT_ALLOCATED_SIZE)) {
            IOHelper.writeClassVersion(outputstream, CLASS_VERSION);
            IOHelper.write32bit(outputstream, key.hash);
            
            if (key.realKey == null) {
                IOHelper.writeUTF32(outputstream, NULL_OBJECT);
            } else {
                if (key.realKey instanceof Externalizable) {
                    IOHelper.writeUTF32(outputstream, UseExternalizable);
                    Externalizable v = (Externalizable) key.realKey;

                    try (ByteArrayOutputStream outputstreamForExternalizable = new ByteArrayOutputStream(IOHelper.BUFFER_DEFAULT_ALLOCATED_SIZE)) {
                        IOHelper.writeUTF(outputstreamForExternalizable, v.getClass().getName());
                        ByteArrayObjectOutput out = new ByteArrayObjectOutput(outputstreamForExternalizable, true);
                        v.writeExternal(out);
                        outputstreamForExternalizable.flush();
                        IOHelper.writeByteArray32(outputstream, outputstreamForExternalizable.toByteArray());
                    }
                } else {
                    IOHelper.writeUTF32(outputstream, UseSerializable);

                    try (ByteArrayOutputStream outputstreamForSerializable = new ByteArrayOutputStream(IOHelper.BUFFER_DEFAULT_ALLOCATED_SIZE)) {
                        IOHelper.writeObject(outputstreamForSerializable, key.realKey);
                        outputstreamForSerializable.flush();
                        IOHelper.writeByteArray32(outputstream, outputstreamForSerializable.toByteArray());
                    }
                }
            }
            
            outputstream.flush();
            return outputstream.toByteArray();
        }
    }

    public static final FileBaseBinaryFileStoreKeyImpl fromBytes(byte[] bytes) throws Exception {
        if (bytes == null || bytes.length == 0) {
            return null;
        }

        ByteArrayInputStream inputstream = new ByteArrayInputStream(bytes);
        IOHelper.readClassVersion(inputstream);
        int hashValue = IOHelper.read32bit(inputstream);
        String realKeyFlag = IOHelper.readUTF32(inputstream);
        StoreKey key = null;
        if (NULL_OBJECT.equals(realKeyFlag)) {
            key = null;
        } else if (UseExternalizable.equals(realKeyFlag)) {
            byte[] realKeyBytes = IOHelper.readByteArray32(inputstream);
            ByteArrayInputStream externalizableInputstream = new ByteArrayInputStream(realKeyBytes);
            String className = IOHelper.readUTF(externalizableInputstream);
            Object obj = Utils.createObject(className);
            ByteArrayObjectInput in = new ByteArrayObjectInput(externalizableInputstream, true);
            ((Externalizable) obj).readExternal(in);
            key = (StoreKey)obj;
        } else if (UseSerializable.equals(realKeyFlag)) {
            byte[] realKeyBytes = IOHelper.readByteArray32(inputstream);
            ByteArrayInputStream serializableInputstream = new ByteArrayInputStream(realKeyBytes);
            key = (StoreKey)IOHelper.readObject(serializableInputstream);
        } else {
            throw new IOException("Real key flag broken. Flag value: " + realKeyFlag);
        }

        FileBaseBinaryFileStoreKeyImpl storeKeyImpl = new FileBaseBinaryFileStoreKeyImpl(key);
        storeKeyImpl.setHashValue(hashValue);
        return storeKeyImpl;
    }

    public StoreKey getRealKey() {
        return realKey;
    }

    @Override
    public long getVersionNumber() {
        return 0;
    }
}
