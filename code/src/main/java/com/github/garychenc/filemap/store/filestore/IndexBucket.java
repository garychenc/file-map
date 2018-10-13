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
import com.github.garychenc.filemap.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Gary CHEN
 *
 */
class IndexBucket {
    
    private final static Logger LOGGER = LoggerFactory.getLogger(IndexBucket.class);
    
    final static int BUCKET_BYTES_LENGTH = 13;
    final static byte[] NULL_BUCKET_BYTES;

    final static long REMOVED_SLAVE_NUMBER = -13;
    final static long NULL_SLAVE_NUMBER = -1;
    final static int  NULL_HASH_VALUE = -1;
    
    static {
        ByteArrayOutputStream outputstream = null;
        try {
            outputstream = new ByteArrayOutputStream();
            IOHelper.writeBoolean(outputstream, false);
            IOHelper.write64bit(outputstream, NULL_SLAVE_NUMBER);
            IOHelper.write32bit(outputstream, NULL_HASH_VALUE);
            outputstream.flush();
            NULL_BUCKET_BYTES = outputstream.toByteArray();
        } catch (Throwable ex) {
            throw new IllegalStateException("Write null bucket bytes failed.", ex);
        } finally {
            if (outputstream != null) {
                try { outputstream.close(); } catch (IOException exce) { }
            }
        }
    }

    //cache line padding 1
    private long p1, p2, p3, p4, p5, p6, p7, p8;

    private volatile long slaveNumber;
    private final int bucketIndex;

    //cache line padding 2
    private long p9, p10, p11, p12, p13, p14, p15, p16;

    IndexBucket(long slaveNumber, int bucketIndex) {
        this.slaveNumber = slaveNumber;
        this.bucketIndex = bucketIndex;
    }

    void setSlaveNumber(long slaveNumber) {
        this.slaveNumber = slaveNumber;
    }

    long getSlaveNumber() {
        return slaveNumber;
    }
    
    int getBucketIndex() {
        return bucketIndex;
    }

    static final class BucketFromBytesResult {
        private final IndexBucket bucket;
        private final boolean removed;
        private final int hashValue;
        
        private BucketFromBytesResult(IndexBucket bucket, boolean removed, int hashValue) {
            this.bucket = bucket;
            this.removed = removed;
            this.hashValue = hashValue;
        }

        IndexBucket getBucket() {
            return bucket;
        }

        boolean isRemoved() {
            return removed;
        }

        int getHashValue() {
            return hashValue;
        }
    }

    static BucketFromBytesResult fromBytes(byte[] bytes, int bucketIndex, boolean ignoreErrorEntry) throws IOException {
        if (ignoreErrorEntry) {
            if (bytes == null || bytes.length != BUCKET_BYTES_LENGTH) {
                IndexBucket bucket = new IndexBucket(REMOVED_SLAVE_NUMBER, bucketIndex);
                return new BucketFromBytesResult(bucket, true, NULL_HASH_VALUE);
            }

            ByteArrayInputStream inputstream = null;
            try {
                inputstream = new ByteArrayInputStream(bytes);
                boolean removed = IOHelper.readBoolean(inputstream);
                long number = IOHelper.read64bit(inputstream);
                int hash = IOHelper.read32bit(inputstream);
                if (number == NULL_SLAVE_NUMBER) {
                    return null;
                } else {
                    IndexBucket bucket = new IndexBucket(number, bucketIndex);
                    return new BucketFromBytesResult(bucket, removed, hash);
                }            
            } catch (Throwable ex) {
                LOGGER.error("Read IndexBucket failed. bucketIndex : " + bucketIndex, ex);
                IndexBucket bucket = new IndexBucket(REMOVED_SLAVE_NUMBER, bucketIndex);
                return new BucketFromBytesResult(bucket, true, NULL_HASH_VALUE);
            } finally {
                Utils.closeInputStream(inputstream);
            }
        } else {
            if (bytes == null || bytes.length != BUCKET_BYTES_LENGTH) {
                throw new IllegalArgumentException("Bytes length error. Expect length : " + BUCKET_BYTES_LENGTH);
            }

            ByteArrayInputStream inputstream = null;
            try {
                inputstream = new ByteArrayInputStream(bytes);
                boolean removed = IOHelper.readBoolean(inputstream);
                long number = IOHelper.read64bit(inputstream);
                int hash = IOHelper.read32bit(inputstream);
                if (number == NULL_SLAVE_NUMBER) {
                    return null;
                } else {
                    IndexBucket bucket = new IndexBucket(number, bucketIndex);
                    return new BucketFromBytesResult(bucket, removed, hash);
                }            
            } finally {
                Utils.closeInputStream(inputstream);
            }
        }
    }
    
    static byte[] toBytes(IndexBucket bucket, boolean removed, int hashValue) throws IOException {
        try (ByteArrayOutputStream outputstream = new ByteArrayOutputStream(IOHelper.BUFFER_DEFAULT_ALLOCATED_SIZE)) {
            if (bucket == null) {
                IOHelper.writeBoolean(outputstream, false);
                IOHelper.write64bit(outputstream, NULL_SLAVE_NUMBER);
                IOHelper.write32bit(outputstream, NULL_HASH_VALUE);
            } else {
                IOHelper.writeBoolean(outputstream, removed);
                IOHelper.write64bit(outputstream, bucket.slaveNumber);
                IOHelper.write32bit(outputstream, hashValue);
            }
            
            outputstream.flush();
            return outputstream.toByteArray();
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
}


























