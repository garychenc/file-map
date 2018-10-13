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
import java.io.IOException;

import com.github.garychenc.filemap.util.IOHelper;

/**
 *
 * @author Gary CHEN
 *
 */
public class FileBaseContentStoreBlockImpl implements ContentStoreBlock {

    private static final int CLASS_VERSION = 1;
    private static final String NULL_OBJECT = "null";
    private static final String NOT_NULL_OBJECT = "not_null";
    
    public static final int BLOCK_LENGTH_OFFSET = 2;
    public static final int BLOCK_LENGTH_STORE_LENGTH = 8;
    
    public static final int REMOVED_OFFSET = 10;
    public static final int REMOVED_STORE_LENGTH = 1;
    public static final byte UNREMOVED_MARK = 0x00;
    public static final byte REMOVED_MARK   = 0x01;
    
    public static final long BLOCK_LEAST_LENGTH = 11L;
    
    private volatile byte[] payload = null; // need to be persisted
    private volatile long blockLength = -1; // need to be persisted
    private volatile boolean removed = false; // need to be persisted

    public FileBaseContentStoreBlockImpl() {
    }

    /* (non-Javadoc)
     */
    @Override
    public long getContentLength() throws ContentStoreBlockException {
        if (blockLength == -1) {
            try (ByteArrayOutputStream outputstream = new ByteArrayOutputStream(IOHelper.BUFFER_DEFAULT_ALLOCATED_SIZE)) {
                IOHelper.writeClassVersion(outputstream, CLASS_VERSION);
                IOHelper.write64bit(outputstream, blockLength);
                IOHelper.writeBoolean(outputstream, removed);
                writePayload(outputstream);
                outputstream.flush();
                blockLength = outputstream.size();
            } catch (IOException ex) {
                throw new ContentStoreBlockException("Write block content to bytes failed.", ex);
            }
        }
        
        return blockLength;
    }

    /* (non-Javadoc)
     */
    @Override
    public byte[] toBytes() throws ContentStoreBlockException {
        try (ByteArrayOutputStream outputstream = new ByteArrayOutputStream(IOHelper.BUFFER_DEFAULT_ALLOCATED_SIZE)) {
            IOHelper.writeClassVersion(outputstream, CLASS_VERSION);
            IOHelper.write64bit(outputstream, this.getContentLength());
            IOHelper.writeBoolean(outputstream, removed);
            writePayload(outputstream);
            outputstream.flush();
            return outputstream.toByteArray();
        } catch (IOException ex) {
            throw new ContentStoreBlockException("Write block content to bytes failed.", ex);
        }
    }

    @Override
    public void fromBytes(byte[] bytes) throws ContentStoreBlockException {
        if (bytes == null || bytes.length == 0) {
            return;
        }

        try {
            ByteArrayInputStream inputstream = new ByteArrayInputStream(bytes);
            IOHelper.readClassVersion(inputstream);
            blockLength = IOHelper.read64bit(inputstream);
            removed = IOHelper.readBoolean(inputstream);
            readPayload(inputstream);
        } catch (IOException ex) {
            throw new ContentStoreBlockException("Read block content from bytes failed.", ex);
        }
    }

    private void writePayload(ByteArrayOutputStream outputstream) throws IOException {
        if (payload == null) {
            IOHelper.writeUTF32(outputstream, NULL_OBJECT);
        } else {
            IOHelper.writeUTF32(outputstream, NOT_NULL_OBJECT);
            IOHelper.writeByteArray32(outputstream, payload);
        }
    }
    
    private void readPayload(ByteArrayInputStream inputstream) throws IOException {
        String flag = IOHelper.readUTF32(inputstream);
        if (NULL_OBJECT.equals(flag)) {
            payload = null;
        } else {
            payload = IOHelper.readByteArray32(inputstream);
        }
    }

    public byte[] getPayload() {
        return payload;
    }
    
    public byte[] setPayload(byte[] payload) {
        resetBlockLength();
        byte[] oldPayload = this.payload;
        this.payload = payload;
        return oldPayload;
    }
    
    public FileBaseContentStoreBlockImpl copy() {
        FileBaseContentStoreBlockImpl block = new FileBaseContentStoreBlockImpl();
        block.setPayload(payload);
        return block;
    }

    private void resetBlockLength() {
        blockLength = -1;
    }    

}
