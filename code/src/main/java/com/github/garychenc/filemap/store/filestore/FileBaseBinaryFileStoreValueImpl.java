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
public class FileBaseBinaryFileStoreValueImpl implements BinaryFileStoreValue {

    private final static int CLASS_VERSION = 1;
    private final static String NULL_OBJECT = "null";
    private final static String NOT_NULL_OBJECT = "not_null";    
    private final static String UseExternalizable = "UseExternalizable";
    private final static String UseSerializable = "UseSerializable";
    private final static String UseByteArray = "UseByteArray";
    private final static String UseString = "UseString";
    
    private volatile FileBaseBinaryFileStoreKeyImpl originalStoreKey = null;
    private volatile long versionNumber = 0;
    private Object realValue;

    public FileBaseBinaryFileStoreValueImpl(Object realValue) {
        this.realValue = realValue;
    }

    /* (non-Javadoc)
     */
    @Override
    public StoreKey getOriginalStoreKey() {
        return originalStoreKey;
    }

    @Override
    public long getVersionNumber() {
        return versionNumber;
    }

    @Override
    public void setVersionNumber(long versionNumber) {
        this.versionNumber = versionNumber;
    }

    /* (non-Javadoc)
     */
    @Override
    public void setOriginalStoreKey(StoreKey key) {
        if (key == null) {
            this.originalStoreKey = null;
        } else {
            if (!(key instanceof FileBaseBinaryFileStoreKeyImpl)) {
                throw new IllegalArgumentException("StoreKey must be the type of " + FileBaseBinaryFileStoreKeyImpl.class.getName());
            }
            
            this.originalStoreKey = (FileBaseBinaryFileStoreKeyImpl)key;
        }
    }
    
    private final static byte[] ZERO_BYTES = new byte[0];
    
    public static final byte[] toBytes(FileBaseBinaryFileStoreValueImpl value) throws IOException {
        if (value == null) {
            return ZERO_BYTES;
        }

        try (ByteArrayOutputStream outputstream = new ByteArrayOutputStream(IOHelper.BUFFER_DEFAULT_ALLOCATED_SIZE)) {
            IOHelper.writeClassVersion(outputstream, CLASS_VERSION);
            
            IOHelper.write64bit(outputstream, value.versionNumber);
            
            if (value.originalStoreKey == null) {
                IOHelper.writeUTF32(outputstream, NULL_OBJECT);
            } else {
                IOHelper.writeUTF32(outputstream, NOT_NULL_OBJECT);
                byte[] keyBytes = FileBaseBinaryFileStoreKeyImpl.toBytes(value.originalStoreKey);
                if (keyBytes == null) {
                    throw new IllegalStateException("Bytes of originalStoreKey can not be null.");
                }
                
                IOHelper.writeByteArray32(outputstream, keyBytes);
            }
            
            if (value.realValue == null) {
                IOHelper.writeUTF32(outputstream, NULL_OBJECT);
            } else {
                if (value.realValue instanceof byte[]) {
                    IOHelper.writeUTF32(outputstream, UseByteArray);
                    byte[] v = (byte[]) value.realValue;
                    IOHelper.writeByteArray32(outputstream, v);
                } else if (value.realValue instanceof String) {
                    IOHelper.writeUTF32(outputstream, UseString);
                    IOHelper.writeUTF32(outputstream, (String) value.realValue);
                } else if (value.realValue instanceof Externalizable) {
                    IOHelper.writeUTF32(outputstream, UseExternalizable);
                    Externalizable v = (Externalizable) value.realValue;

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
                        IOHelper.writeObject(outputstreamForSerializable, value.realValue);
                        outputstreamForSerializable.flush();
                        IOHelper.writeByteArray32(outputstream, outputstreamForSerializable.toByteArray());
                    }
                }
            }
            
            outputstream.flush();
            return outputstream.toByteArray();
        }
    }

    public static final FileBaseBinaryFileStoreValueImpl fromBytes(byte[] bytes) throws Exception {
        if (bytes == null || bytes.length == 0) {
            return null;
        }

        ByteArrayInputStream inputstream = new ByteArrayInputStream(bytes);
        IOHelper.readClassVersion(inputstream);
        
        long versionNumber = IOHelper.read64bit(inputstream);
        
        String storeKeyFlag = IOHelper.readUTF32(inputstream);
        FileBaseBinaryFileStoreKeyImpl storeKey = null;
        if (NULL_OBJECT.equals(storeKeyFlag)) {
            storeKey = null;
        } else {
            byte[] keyBytes = IOHelper.readByteArray32(inputstream);
            storeKey = FileBaseBinaryFileStoreKeyImpl.fromBytes(keyBytes);
        }
        
        String realValueFlag = IOHelper.readUTF32(inputstream);
        Object v = null;
        if (NULL_OBJECT.equals(realValueFlag)) {
            v = null;
        } else if (UseByteArray.equals(realValueFlag)) {
            v = IOHelper.readByteArray32(inputstream);
        } else if (UseString.equals(realValueFlag)) {
            v = IOHelper.readUTF32(inputstream);
        } else if (UseExternalizable.equals(realValueFlag)) {
            byte[] valueBytes = IOHelper.readByteArray32(inputstream);
            ByteArrayInputStream externalizableInputstream = new ByteArrayInputStream(valueBytes);
            String className = IOHelper.readUTF(externalizableInputstream);
            Object obj = Utils.createObject(className);
            ByteArrayObjectInput in = new ByteArrayObjectInput(externalizableInputstream, true);
            ((Externalizable) obj).readExternal(in);
            v = obj;
        } else if (UseSerializable.equals(realValueFlag)) {
            byte[] valueBytes = IOHelper.readByteArray32(inputstream);
            ByteArrayInputStream serializableInputstream = new ByteArrayInputStream(valueBytes);
            v = IOHelper.readObject(serializableInputstream);
        } else {
            throw new IOException("Real value flag broken. Flag value: " + realValueFlag);
        }
        
        FileBaseBinaryFileStoreValueImpl storeValue = new FileBaseBinaryFileStoreValueImpl(v);
        storeValue.setOriginalStoreKey(storeKey);
        storeValue.setVersionNumber(versionNumber);
        return storeValue;
    }

    /**
     * @return the realValue
     */
    public Object getRealValue() {
        return realValue;
    }

    public void setRealValue(Object realValue) {
        this.realValue = realValue;
    }
}

































