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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;

/**
 *
 * @author Gary CHEN
 *
 */
public class ByteArrayObjectOutput implements ObjectOutput {

    private final ByteArrayOutputStream outputStream;
    private final boolean delegate;

    public ByteArrayObjectOutput(ByteArrayOutputStream outputStream, boolean delegate) {
        Asserts.notNull("ByteArrayOutputStream", outputStream);
        this.outputStream = outputStream;
        this.delegate = delegate;
    }

    /* (non-Javadoc)
     * @see java.io.ObjectOutput#close()
     */
    @Override
    public void close() throws IOException {
        if (!delegate) {
            outputStream.close();
        }
    }

    /* (non-Javadoc)
     * @see java.io.ObjectOutput#flush()
     */
    @Override
    public void flush() throws IOException {
        outputStream.flush();
    }

    /* (non-Javadoc)
     * @see java.io.ObjectOutput#write(int)
     */
    @Override
    public void write(int b) throws IOException {
        IOHelper.write8bit(outputStream, b);
    }

    /* (non-Javadoc)
     * @see java.io.ObjectOutput#write(byte[])
     */
    @Override
    public void write(byte[] b) throws IOException {
        IOHelper.writeByteArray32(outputStream, b);
    }

    /* (non-Javadoc)
     * @see java.io.ObjectOutput#write(byte[], int, int)
     */
    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        byte[] copy = Utils.copyOfRange(b, off, off + len);
        IOHelper.writeByteArray32(outputStream, copy);
        
    }

    /* (non-Javadoc)
     * @see java.io.ObjectOutput#writeObject(java.lang.Object)
     */
    @Override
    public void writeObject(Object obj) throws IOException {
        IOHelper.writeObject(outputStream, obj);
    }

    /* (non-Javadoc)
     * @see java.io.DataOutput#writeBoolean(boolean)
     */
    @Override
    public void writeBoolean(boolean v) throws IOException {
        IOHelper.writeBoolean(outputStream, v);
    }

    /* (non-Javadoc)
     * @see java.io.DataOutput#writeByte(int)
     */
    @Override
    public void writeByte(int v) throws IOException {
        IOHelper.write8bit(outputStream, v);
    }

    /* (non-Javadoc)
     * @see java.io.DataOutput#writeBytes(java.lang.String)
     */
    @Override
    public void writeBytes(String s) throws IOException {
        IOHelper.writeUTF32(outputStream, s);
    }

    /* (non-Javadoc)
     * @see java.io.DataOutput#writeChar(int)
     */
    @Override
    public void writeChar(int v) throws IOException {
        IOHelper.write16bit(outputStream, v);
    }

    /* (non-Javadoc)
     * @see java.io.DataOutput#writeChars(java.lang.String)
     */
    @Override
    public void writeChars(String s) throws IOException {
        IOHelper.writeUTF32(outputStream, s);
    }

    /* (non-Javadoc)
     * @see java.io.DataOutput#writeDouble(double)
     */
    @Override
    public void writeDouble(double v) throws IOException {
        IOHelper.writeDouble(outputStream, v);
    }

    /* (non-Javadoc)
     * @see java.io.DataOutput#writeFloat(float)
     */
    @Override
    public void writeFloat(float v) throws IOException {
        IOHelper.writeFloat(outputStream, v);
    }

    /* (non-Javadoc)
     * @see java.io.DataOutput#writeInt(int)
     */
    @Override
    public void writeInt(int v) throws IOException {
        IOHelper.write32bit(outputStream, v);
    }

    /* (non-Javadoc)
     * @see java.io.DataOutput#writeLong(long)
     */
    @Override
    public void writeLong(long v) throws IOException {
        IOHelper.write64bit(outputStream, v);
    }

    /* (non-Javadoc)
     * @see java.io.DataOutput#writeShort(int)
     */
    @Override
    public void writeShort(int v) throws IOException {
        IOHelper.write16bit(outputStream, v);
    }

    /* (non-Javadoc)
     * @see java.io.DataOutput#writeUTF(java.lang.String)
     */
    @Override
    public void writeUTF(String s) throws IOException {
        IOHelper.writeUTF32(outputStream, s);
    }

}
