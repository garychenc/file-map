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

import java.io.IOException;

/**
 *
 * @author Gary CHEN
 *
 */
public class FileBaseBinaryFileStoreConvertorImpl implements BinaryFileStoreConvertor {

    public FileBaseBinaryFileStoreConvertorImpl() {
    }

    /* (non-Javadoc)
     */
    @Override
    public BinaryFileStoreValue fromStoreBlock(ContentStoreBlock block) throws ContentStoreBlockException {
        if (!(block instanceof FileBaseContentStoreBlockImpl)) {
            throw new IllegalArgumentException("ContentStoreBlock type error. Expected Type: " + FileBaseContentStoreBlockImpl.class.getName());
        }
        
        try {
            FileBaseContentStoreBlockImpl blockImpl = (FileBaseContentStoreBlockImpl) block;
            byte[] valueBytes = blockImpl.getPayload();
            FileBaseBinaryFileStoreValueImpl valueImpl = FileBaseBinaryFileStoreValueImpl.fromBytes(valueBytes);
            return valueImpl;
        } catch (Throwable ex) {
            throw new ContentStoreBlockException("Convert bytes to FileBaseBinaryFileStoreValueImpl failed.", ex);
        }
    }

    /* (non-Javadoc)
     */
    @Override
    public ContentStoreBlock toStoreBlock(BinaryFileStoreValue storeValue) throws ContentStoreBlockException {
        if (!(storeValue instanceof FileBaseBinaryFileStoreValueImpl)) {
            throw new IllegalArgumentException("BinaryFileStoreValue type error. Expected Type: " + FileBaseBinaryFileStoreValueImpl.class.getName());
        }
                
        try {
            FileBaseBinaryFileStoreValueImpl valueImpl = (FileBaseBinaryFileStoreValueImpl) storeValue;
            byte[] valueBytes = FileBaseBinaryFileStoreValueImpl.toBytes(valueImpl);
            FileBaseContentStoreBlockImpl blockImpl = new FileBaseContentStoreBlockImpl();
            blockImpl.setPayload(valueBytes);
            return blockImpl;
        } catch (IOException ex) {
            throw new ContentStoreBlockException("Convert FileBaseBinaryFileStoreValueImpl to bytes failed.", ex);
        }
    }

}





















































