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

import static com.github.garychenc.filemap.util.Asserts.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.github.garychenc.filemap.util.IOHelper;
import com.github.garychenc.filemap.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Gary CHEN
 *
 */
public class FileBaseContentStoreSlave implements ContentStoreSlave {
    
    //TODO : add a cache in this level to improve the performance of searching
    private final static Logger LOGGER = LoggerFactory.getLogger(FileBaseContentStoreSlave.class);

    //cache line padding 1
    private long p1, p2, p3, p4, p5, p6, p7, p8;

    private final FileBaseContentStoreMaster master;
    private final ContentStoreSlaveStoreFile file;
    private final ContentStoreSlaveIndex slaveIndex;
    
    private volatile boolean closed;
    private final ReadWriteLock slaveLock;

    //cache line padding 2
    private long p9, p10, p11, p12, p13, p14, p15, p16;

    public FileBaseContentStoreSlave(ContentStoreSlaveStoreFile file, FileBaseContentStoreMaster master, ContentStoreSlaveIndex slaveIndex) {
        notNull("master", master);
        notNull("slaveIndex", slaveIndex);
        notNull("file", file);

        this.master = master;
        this.slaveIndex = slaveIndex;
        
        this.file = file;
        this.closed = false;
        this.slaveLock = new ReentrantReadWriteLock();
    }

    private void checkClosed() throws ContentStoreSlaveException {
        if (closed) {
            throw new ContentStoreSlaveException("ContentStoreSlave had been closed. slaveNumber = " + slaveIndex.getSlaveNumber());
        }
    }
    
    /* (non-Javadoc)
     */
    @Override
    public void delete() throws ContentStoreSlaveException {
        slaveLock.writeLock().lock();
        try {
            checkClosed();
            
            byte[] bytes = new byte[FileBaseContentStoreBlockImpl.REMOVED_STORE_LENGTH];
            bytes[0] = FileBaseContentStoreBlockImpl.REMOVED_MARK;
            file.getSlaveStoreFileChannel().write(ByteBuffer.wrap(bytes), slaveIndex.getStoreStartAddress() + FileBaseContentStoreBlockImpl.REMOVED_OFFSET);
            if (master.isForceWriteData()) {
                file.getSlaveStoreFileChannel().force(true);
            }
            
            master.removeSlave(slaveIndex.getSlaveNumber());
        } catch (IOException ex) {
            throw new ContentStoreSlaveException("Mark FileBaseContentStoreBlockImpl as deleted failed. Slave store file : " + file.getSlaveStoreFilePath().getAbsolutePath() + ", Start Address: " + slaveIndex.getStoreStartAddress(), ex);
        } catch (ContentStoreMasterException ex) {
            try {
                byte[] bytes = new byte[FileBaseContentStoreBlockImpl.REMOVED_STORE_LENGTH];
                bytes[0] = FileBaseContentStoreBlockImpl.UNREMOVED_MARK;
                file.getSlaveStoreFileChannel().write(ByteBuffer.wrap(bytes), slaveIndex.getStoreStartAddress() + FileBaseContentStoreBlockImpl.REMOVED_OFFSET);
                if (master.isForceWriteData()) {
                    file.getSlaveStoreFileChannel().force(true);
                }
            } catch (IOException exce) {
                LOGGER.error("Undo mark FileBaseContentStoreBlockImpl as deleted failed.", exce);
            }

            throw new ContentStoreSlaveException("Remove slave from master failed. Slave Number : " + slaveIndex.getSlaveNumber(), ex);
        } finally {
            slaveLock.writeLock().unlock();
        }
    }

    /* (non-Javadoc)
     */
    @Override
    public long getSlaveNumber() {
        return slaveIndex.getSlaveNumber();
    }

    /* (non-Javadoc)
     */
    @Override
    public ContentStoreBlock read() throws ContentStoreSlaveException {
        slaveLock.readLock().lock();
        try {
            if (closed) {
                throw new IllegalStateException("Store slave had been closed.");
            }
            
            byte[] bytes = new byte[(int)slaveIndex.getStoreSize()];
            IOHelper.readFully(file.getSlaveStoreFileChannel(), bytes, slaveIndex.getStoreStartAddress());
            boolean removed = bytes[FileBaseContentStoreBlockImpl.REMOVED_OFFSET] != FileBaseContentStoreBlockImpl.UNREMOVED_MARK;
            if (removed) {
                return null;
            }

            ByteArrayInputStream inputstream4BlockLength = new ByteArrayInputStream(bytes, FileBaseContentStoreBlockImpl.BLOCK_LENGTH_OFFSET, FileBaseContentStoreBlockImpl.BLOCK_LENGTH_STORE_LENGTH);
            long blockLength = IOHelper.read64bit(inputstream4BlockLength);
            if (blockLength < FileBaseContentStoreBlockImpl.BLOCK_LEAST_LENGTH) {
                throw new ContentStoreSlaveException("ContentStoreBlock length error. min length : " + FileBaseContentStoreBlockImpl.BLOCK_LEAST_LENGTH + ", current length: " + blockLength);
            }
            
            if (blockLength > bytes.length) {
                throw new ContentStoreSlaveException("ContentStoreBlock length error. Max length : " + bytes.length + ", current length: " + blockLength);
            }

            FileBaseContentStoreBlockImpl fileBlock = new FileBaseContentStoreBlockImpl();
            fileBlock.fromBytes(Utils.copyOfRange(bytes, 0, (int)blockLength));
            return fileBlock;
        } catch (IOException ex) {
            throw new ContentStoreSlaveException("Read FileBaseContentStoreBlockImpl failed. Slave store file : " + file.getSlaveStoreFilePath().getAbsolutePath() + ", Start Address: " + slaveIndex.getStoreStartAddress(), ex);
        } catch (ContentStoreBlockException ex) {
            throw new ContentStoreSlaveException("Convert bytes to FileBaseContentStoreBlockImpl failed.", ex);
        } finally {
            slaveLock.readLock().unlock();
        }
    }

    /* (non-Javadoc)
     */
    @Override
    public void store(ContentStoreBlock block) throws ContentStoreSlaveException, ContentStoreBlockLengthExceedException {
        notNull("block", block);
        if (!(block instanceof FileBaseContentStoreBlockImpl)) {
            throw new ContentStoreSlaveException("ContentStoreBlock type error. Expect Type : " + FileBaseContentStoreBlockImpl.class.getName());
        }
        
        slaveLock.writeLock().lock();
        try {
            checkClosed();
            
            byte[] blockBytes = block.toBytes();
            if (blockBytes.length != block.getContentLength()) {
                throw new ContentStoreSlaveException("ContentStoreBlock length error. block.toBytes().length = " + blockBytes.length + ", block.getContentLength() = " + block.getContentLength() + ".");
            }
            
            long blockLength = block.getContentLength();
            if (blockLength > slaveIndex.getStoreSize()) {
                throw new ContentStoreBlockLengthExceedException("ContentStoreBlock Length: " + blockLength + ", Store Size: " + slaveIndex.getStoreSize() + ".");
            }
            
            file.getSlaveStoreFileChannel().write(ByteBuffer.wrap(blockBytes), slaveIndex.getStoreStartAddress());
            if (master.isForceWriteData()) {
                file.getSlaveStoreFileChannel().force(true);
            }
        } catch (ContentStoreBlockException ex) {
            throw new ContentStoreSlaveException("Convert block to bytes failed.", ex);
        } catch (IOException ex) {
            throw new ContentStoreSlaveException("Store block bytes to slave store file failed. Slave store file : " + file.getSlaveStoreFilePath().getAbsolutePath() + ", Start Address: " + slaveIndex.getStoreStartAddress(), ex);
        } finally {
            slaveLock.writeLock().unlock();
        }
    }

    public long getStoreSize() {
        return slaveIndex.getStoreSize();
    }

    public void close() {
        slaveLock.writeLock().lock();
        try {
            this.closed = true;
        } finally {
            slaveLock.writeLock().unlock();
        }
    }

    public void reopen() {
        slaveLock.writeLock().lock();
        try {
            this.closed = false;
        } finally {
            slaveLock.writeLock().unlock();
        }
    }
    
    public boolean isClosed() {
        slaveLock.readLock().lock();
        try {
            return closed;
        } finally {
            slaveLock.readLock().unlock();
        }        
    }

    public long getP1() {
        return p1;
    }

    public long getP2() {
        return p2;
    }

    public long getP3() {
        return p3;
    }

    public long getP4() {
        return p4;
    }

    public long getP5() {
        return p5;
    }

    public long getP6() {
        return p6;
    }

    public long getP7() {
        return p7;
    }

    public long getP8() {
        return p8;
    }

    public long getP9() {
        return p9;
    }

    public long getP10() {
        return p10;
    }

    public long getP11() {
        return p11;
    }

    public long getP12() {
        return p12;
    }

    public long getP13() {
        return p13;
    }

    public long getP14() {
        return p14;
    }

    public long getP15() {
        return p15;
    }

    public long getP16() {
        return p16;
    }

    public int getFileNumber() {
        return slaveIndex.getFileNumber();
    }

    public long getStoreStartAddress() {
        return slaveIndex.getStoreStartAddress();
    }

    public ContentStoreSlaveIndex getSlaveIndex() {
        return slaveIndex;
    }

}


















































