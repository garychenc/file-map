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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInput;

/**
 *
 * @author Gary CHEN
 *
 */
public class ByteArrayObjectInput implements ObjectInput {

    private final ByteArrayInputStream inputstream;
    private final boolean delegate;

    public ByteArrayObjectInput(byte[] in) {
        Asserts.notNull("in", in);
        this.inputstream = new ByteArrayInputStream(in);
        this.delegate = false;
    }
    
    public ByteArrayObjectInput(ByteArrayInputStream inputstream, boolean delegate) {
        Asserts.notNull("inputstream", inputstream);
        this.inputstream = inputstream;
        this.delegate = delegate;
    }

    /* (non-Javadoc)
     * @see java.io.ObjectInput#available()
     */
    @Override
    public int available() throws IOException {
        return inputstream.available();
    }

    /* (non-Javadoc)
     * @see java.io.ObjectInput#close()
     */
    @Override
    public void close() throws IOException {
        if (!delegate) {
            inputstream.close();
        }        
    }

    /* (non-Javadoc)
     * @see java.io.ObjectInput#read()
     */
    @Override
    public int read() throws IOException {
        return IOHelper.read8bit(inputstream);
    }

    /* (non-Javadoc)
     * @see java.io.ObjectInput#read(byte[])
     */
    @Override
    public int read(byte[] b) throws IOException {
        byte[] bytes = IOHelper.readByteArray32(inputstream);
        if (b.length != bytes.length) {
            throw new IOException("the length of input argument bytes error. Expected: " + bytes.length + ",Actual: " + b.length + ".");
        }
        
        System.arraycopy(bytes, 0, b, 0, bytes.length);
        return bytes.length;
    }

    /* (non-Javadoc)
     * @see java.io.ObjectInput#read(byte[], int, int)
     */
    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return inputstream.read(b, off, len);
    }

    /* (non-Javadoc)
     * @see java.io.ObjectInput#readObject()
     */
    @Override
    public Object readObject() throws ClassNotFoundException, IOException {
        return IOHelper.readObject(inputstream);
    }

    /* (non-Javadoc)
     * @see java.io.ObjectInput#skip(long)
     */
    @Override
    public long skip(long n) throws IOException {
        return inputstream.skip(n);
    }

    /* (non-Javadoc)
     * @see java.io.DataInput#readBoolean()
     */
    @Override
    public boolean readBoolean() throws IOException {
        return IOHelper.readBoolean(inputstream);
    }

    /* (non-Javadoc)
     * @see java.io.DataInput#readByte()
     */
    @Override
    public byte readByte() throws IOException {
        return (byte)IOHelper.read8bit(inputstream);
    }

    /* (non-Javadoc)
     * @see java.io.DataInput#readChar()
     */
    @Override
    public char readChar() throws IOException {
        return (char)IOHelper.read16bit(inputstream);
    }

    /* (non-Javadoc)
     * @see java.io.DataInput#readDouble()
     */
    @Override
    public double readDouble() throws IOException {
        return IOHelper.readDouble(inputstream);
    }

    /* (non-Javadoc)
     * @see java.io.DataInput#readFloat()
     */
    @Override
    public float readFloat() throws IOException {
        return IOHelper.readFloat(inputstream);
    }

    /* (non-Javadoc)
     * @see java.io.DataInput#readFully(byte[])
     */
    @Override
    public void readFully(byte[] b) throws IOException {
        throw new UnsupportedOperationException();
    }

    /* (non-Javadoc)
     * @see java.io.DataInput#readFully(byte[], int, int)
     */
    @Override
    public void readFully(byte[] b, int off, int len) throws IOException {
        throw new UnsupportedOperationException();
    }

    /* (non-Javadoc)
     * @see java.io.DataInput#readInt()
     */
    @Override
    public int readInt() throws IOException {
        return IOHelper.read32bit(inputstream);
    }

    /* (non-Javadoc)
     * @see java.io.DataInput#readLine()
     */
    @Override
    public String readLine() throws IOException {
        return IOHelper.readUTF32(inputstream);
    }

    /* (non-Javadoc)
     * @see java.io.DataInput#readLong()
     */
    @Override
    public long readLong() throws IOException {
        return IOHelper.read64bit(inputstream);
    }

    /* (non-Javadoc)
     * @see java.io.DataInput#readShort()
     */
    @Override
    public short readShort() throws IOException {
        return (short)IOHelper.read16bit(inputstream);
    }

    /* (non-Javadoc)
     * @see java.io.DataInput#readUTF()
     */
    @Override
    public String readUTF() throws IOException {
        return IOHelper.readUTF32(inputstream);
    }

    /* (non-Javadoc)
     * @see java.io.DataInput#readUnsignedByte()
     */
    @Override
    public int readUnsignedByte() throws IOException {
        return IOHelper.read16bit(inputstream);
    }

    /* (non-Javadoc)
     * @see java.io.DataInput#readUnsignedShort()
     */
    @Override
    public int readUnsignedShort() throws IOException {
        return IOHelper.read16bit(inputstream);
    }

    /* (non-Javadoc)
     * @see java.io.DataInput#skipBytes(int)
     */
    @Override
    public int skipBytes(int n) throws IOException {
        return (int)inputstream.skip(n);
    }

}
