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

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.FileChannel;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 * @author Gary CHEN
 *
 */
public final class IOHelper {

    public  static final int UTF_BUFFER_SIZE = 8192;
    public  static final int BUFFER_DEFAULT_ALLOCATED_SIZE = 1024 * 512; //512K
    
    public  static final byte ZERO_BYTES[] = new byte[0];
    private static final AtomicReference<byte[]> readBuffer = new AtomicReference<byte[]>();
    
    private IOHelper() {
    }
    
    public static final void writeClassVersion(OutputStream outputstream, int classVersion) 
            throws IOException {
        IOHelper.write16bit(outputstream, classVersion);
    }
    
    public static final int readClassVersion(InputStream inputstream) 
            throws IOException {
        return IOHelper.read16bit(inputstream);
    }

    public static final boolean readBoolean(InputStream inputstream)
            throws IOException {
        int i = inputstream.read();
        if (i < 0)
            throw new EOFException();
        else
            return i != 0;
    }

    public static final int read8bit(InputStream inputstream)
            throws IOException {
        int i = inputstream.read();
        if (i < 0)
            throw new EOFException();
        else
            return i;
    }

    public static final int read16bit(InputStream inputstream)
            throws IOException {
        int i = inputstream.read();
        int j = inputstream.read();
        if ((i | j) < 0)
            throw new EOFException();
        else
            return (i << 8) + j;
    }

    public static final int read24bit(InputStream inputstream)
            throws IOException {
        int i = inputstream.read();
        int j = inputstream.read();
        int k = inputstream.read();
        if ((i | j | k) < 0)
            throw new EOFException();
        else
            return (i << 16) + (j << 8) + k;
    }

    public static final int read32bit(InputStream inputstream)
            throws IOException {
        int i = inputstream.read();
        int j = inputstream.read();
        int k = inputstream.read();
        int l = inputstream.read();
        if ((i | j | k | l) < 0)
            throw new EOFException();
        else
            return (i << 24) + (j << 16) + (k << 8) + l;
    }

    public static final long read64bit(InputStream inputstream)
            throws IOException {
        byte[] abyte0 = readBuffer.getAndSet(null);
        if (abyte0 == null)
            abyte0 = new byte[8];
        readFully(inputstream, abyte0);
        long l = (long) ((abyte0[0] & 0xff) << 24 | (abyte0[1] & 0xff) << 16
                | (abyte0[2] & 0xff) << 8 | abyte0[3] & 0xff) << 32
                | (long) ((abyte0[4] & 0xff) << 24 | (abyte0[5] & 0xff) << 16
                        | (abyte0[6] & 0xff) << 8 | abyte0[7] & 0xff)
                & 0xffffffffL;
        readBuffer.compareAndSet(null, abyte0);
        return l;
    }

    public static final float readFloat(InputStream inputstream)
            throws IOException {
        return Float.intBitsToFloat(read32bit(inputstream));
    }

    public static final double readDouble(InputStream inputstream)
            throws IOException {
        return Double.longBitsToDouble(read64bit(inputstream));
    }

    public static final void readFully(InputStream inputstream, byte abyte0[]) throws IOException {
        readFully(inputstream, abyte0, 0, abyte0.length);
    }

    public static final void readFully(InputStream inputstream, byte abyte0[], int i, int j) throws IOException {
        int l;
        for (int k = 0; k < j; k += l) {
            l = inputstream.read(abyte0, i + k, j - k);
            if (l < 0)
                return;
        }
    }
    
    public static final void readFully(RandomAccessFile randomAccessFile, byte abyte0[]) throws IOException {
        readFully(randomAccessFile, abyte0, 0, abyte0.length);
    }
    
    public static final void readFully(RandomAccessFile randomAccessFile, byte abyte0[], int i, int j) throws IOException {
        int l;
        for (int k = 0; k < j; k += l) {
            l = randomAccessFile.read(abyte0, i + k, j - k);
            if (l < 0)
                return;
        }
    }
    
    public static final void readFully(FileChannel fileChannel, byte[] bytes, long readingAddress) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        int j = buffer.remaining();
        int l;
        for (int k = 0; k < j; k += l) {
            l = fileChannel.read(buffer, readingAddress + k);
            if (l < 0)
                return;
        }
    }
    
    public static final void writeFully(AsynchronousSocketChannel writingSocket, ByteBuffer writingBuffer) throws InterruptedException, ExecutionException {
        int bufferLength = writingBuffer.remaining();
        int currWritedNumber = 0;
        for (int writedByteNumber = 0; writedByteNumber < bufferLength; writedByteNumber += currWritedNumber) {
            writingBuffer.position(writedByteNumber);
            Future<Integer> writingFuture = writingSocket.write(writingBuffer);
            currWritedNumber = writingFuture.get();
            if (currWritedNumber < 0) {
                return;
            }
        }
    }

    public static final byte[] readByteArray16(InputStream inputstream)
            throws IOException {
        byte abyte0[] = new byte[read16bit(inputstream)];
        readFully(inputstream, abyte0);
        return abyte0;
    }

    public static final byte[] readByteArray24(InputStream inputstream)
            throws IOException {
        byte abyte0[] = new byte[read24bit(inputstream)];
        readFully(inputstream, abyte0);
        return abyte0;
    }

    public static final byte[] readByteArray32(InputStream inputstream)
            throws IOException {
        byte abyte0[] = new byte[read32bit(inputstream)];
        readFully(inputstream, abyte0);
        return abyte0;
    }

    public static final String readUTF(InputStream inputstream)
            throws IOException {
        int i = read16bit(inputstream);
        if (i == 65535)
            return null;
        else
            return readUTF(inputstream, i);
    }

    public static final String readUTF32(InputStream inputstream)
            throws IOException {
        int i = read32bit(inputstream);
        if (i == -1)
            return null;
        else
            return readUTF(inputstream, i);
    }

    private static final String readUTF(InputStream inputstream, int size)
            throws IOException {
        byte byteBuffer[] = null;
        char charBuffer[] = null;
        
        if (size <= UTF_BUFFER_SIZE) {
            byteBuffer = new byte[UTF_BUFFER_SIZE];
            charBuffer = new char[UTF_BUFFER_SIZE];
        } else {
            byteBuffer = new byte[size];
            charBuffer = new char[size];
        }

        int l = 0;
        int i1 = 0;
        
        readFully(inputstream, byteBuffer, 0, size);
        
        do {
            if (l >= size)
                break;
            int j = byteBuffer[l] & 0xff;
            if (j > 127)
                break;
            l++;
            charBuffer[i1++] = (char) j;
        } while (true);
        
        do {
            if (l >= size)
                break;
            int k = byteBuffer[l] & 0xff;
            switch (k >> 4) {
            
            case 0: // '\0'
            case 1: // '\001'
            case 2: // '\002'
            case 3: // '\003'
            case 4: // '\004'
            case 5: // '\005'
            case 6: // '\006'
            case 7: // '\007'
                l++;
                charBuffer[i1++] = (char) k;
                break;

            case 12: // '\f'
            case 13: // '\r'
                if ((l += 2) > size)
                    throw new UTFDataFormatException("malformed input: partial character at end");
                
                byte byte0 = byteBuffer[l - 1];
                if ((byte0 & 0xc0) != 128)
                    throw new UTFDataFormatException((new StringBuilder()).append("malformed input around byte ").append(l).toString());
                
                charBuffer[i1++] = (char) ((k & 0x1f) << 6 | byte0 & 0x3f);
                break;

            case 14: // '\016'
                if ((l += 3) > size)
                    throw new UTFDataFormatException("malformed input: partial character at end");
                
                byte byte1 = byteBuffer[l - 2];
                byte byte2 = byteBuffer[l - 1];
                if ((byte1 & 0xc0) != 128 || (byte2 & 0xc0) != 128)
                    throw new UTFDataFormatException((new StringBuilder()).append("malformed input around byte ").append(l - 1).toString());
                
                charBuffer[i1++] = (char) ((k & 0xf) << 12 | (byte1 & 0x3f) << 6 | (byte2 & 0x3f) << 0);
                break;

            case 8: // '\b'
            case 9: // '\t'
            case 10: // '\n'
            case 11: // '\013'
            default:
                throw new UTFDataFormatException((new StringBuilder()).append("malformed input around byte ").append(l).toString());
            }
            
        } while (true);

        return new String(charBuffer, 0, i1);
    }

    public static final void writeBoolean(OutputStream outputstream, boolean flag) 
            throws IOException {
        outputstream.write(flag ? 1 : 0);
    }

    public static final void write8bit(OutputStream outputstream, int i)
            throws IOException {
        outputstream.write(i);
    }

    public static final void write16bit(OutputStream outputstream, int i)
            throws IOException {
        outputstream.write(i >>> 8 & 0xff);
        outputstream.write(i >>> 0 & 0xff);
    }

    public static final void write24bit(OutputStream outputstream, int i)
            throws IOException {
        outputstream.write(i >>> 16 & 0xff);
        outputstream.write(i >>> 8 & 0xff);
        outputstream.write(i >>> 0 & 0xff);
    }

    public static final void write32bit(OutputStream outputstream, int i)
            throws IOException {
        outputstream.write(i >>> 24 & 0xff);
        outputstream.write(i >>> 16 & 0xff);
        outputstream.write(i >>> 8 & 0xff);
        outputstream.write(i >>> 0 & 0xff);
    }

    public static final void write64bit(OutputStream outputstream, long l)
            throws IOException {
        byte abyte0[] = readBuffer.getAndSet(null);
        if (abyte0 == null)
            abyte0 = new byte[8];
        int i = (int) (l >>> 32 & 0xffffffffL);
        int j = (int) (l & 0xffffffffL);
        abyte0[0] = (byte) (i >>> 24 & 0xff);
        abyte0[1] = (byte) (i >>> 16 & 0xff);
        abyte0[2] = (byte) (i >>> 8 & 0xff);
        abyte0[3] = (byte) (i & 0xff);
        abyte0[4] = (byte) (j >>> 24 & 0xff);
        abyte0[5] = (byte) (j >>> 16 & 0xff);
        abyte0[6] = (byte) (j >>> 8 & 0xff);
        abyte0[7] = (byte) (j & 0xff);
        outputstream.write(abyte0);
        readBuffer.compareAndSet(null, abyte0);
    }

    public static final void writeFloat(OutputStream outputstream, float f)
            throws IOException {
        write32bit(outputstream, Float.floatToIntBits(f));
    }

    public static final void writeDouble(OutputStream outputstream, double d)
            throws IOException {
        write64bit(outputstream, Double.doubleToLongBits(d));
    }

    public static final void writeByteArray16(OutputStream outputstream, byte abyte0[]) 
            throws IOException {
        if (abyte0 == null)
            abyte0 = ZERO_BYTES;
        write16bit(outputstream, abyte0.length);
        outputstream.write(abyte0);
    }

    public static final void writeByteArray24(OutputStream outputstream, byte abyte0[])
            throws IOException {
        if (abyte0 == null)
            abyte0 = ZERO_BYTES;
        write24bit(outputstream, abyte0.length);
        outputstream.write(abyte0);
    }

    public static final void writeByteArray32(OutputStream outputstream, byte abyte0[]) 
            throws IOException {
        if (abyte0 == null)
            abyte0 = ZERO_BYTES;
        write32bit(outputstream, abyte0.length);
        outputstream.write(abyte0);
    }

    public static final void writeUTF(OutputStream outputstream, String s)
            throws IOException {
        if (s == null) {
            write16bit(outputstream, 65535);
            return;
        }
        int i = utflen(s);
        if (i >= 65535) {
            throw new UTFDataFormatException((new StringBuilder()).append("encoded string too long: ").append(i).append(" bytes").toString());
        } else {
            write16bit(outputstream, i);
            writeUTF(outputstream, s, i);
            return;
        }
    }

    public static final void writeUTF32(OutputStream outputstream, String s)
            throws IOException {
        if (s == null) {
            write32bit(outputstream, -1);
            return;
        } else {
            int i = utflen(s);
            write32bit(outputstream, i);
            writeUTF(outputstream, s, i);
            return;
        }
    }

    private static final int utflen(String s) {
        int i = s.length();
        int j = 0;
        for (int k = 0; k < i; k++) {
            char c = s.charAt(k);
            if (c >= '\001' && c <= '\177') {
                j++;
                continue;
            }
            if (c > '\u07FF')
                j += 3;
            else
                j += 2;
        }

        return j;
    }

    private static final void writeUTF(OutputStream outputstream, String s, int size) 
             throws IOException {
        byte buffer[] = null;

        int j = s.length();
        int k = 0;
        
        if (size <= UTF_BUFFER_SIZE) {
            buffer = new byte[UTF_BUFFER_SIZE];
        } else {
            buffer = new byte[size];
        }
        
        int l = 0;
        do {
            if (l >= j)
                break;
            char c = s.charAt(l);
            if (c < '\001' || c > '\177')
                break;
            buffer[k++] = (byte) c;
            l++;
        } while (true);
        
        for (; l < j; l++) {
            char c1 = s.charAt(l);
            if (c1 >= '\001' && c1 <= '\177') {
                buffer[k++] = (byte) c1;
                continue;
            }
            
            if (c1 > '\u07FF') {
                buffer[k++] = (byte) (0xe0 | c1 >> 12 & 0xf);
                buffer[k++] = (byte) (0x80 | c1 >> 6 & 0x3f);
                buffer[k++] = (byte) (0x80 | c1 >> 0 & 0x3f);
            } else {
                buffer[k++] = (byte) (0xc0 | c1 >> 6 & 0x1f);
                buffer[k++] = (byte) (0x80 | c1 >> 0 & 0x3f);
            }
        }

        outputstream.write(buffer, 0, k);
    }

    public static final Object readBasicObject(InputStream inputstream)
            throws IOException {
        int i = read8bit(inputstream);
        switch (i) {
        case 0: // '\0'
            return null;

        case 1: // '\001'
            return new Boolean(readBoolean(inputstream));

        case 2: // '\002'
            return new Byte((byte) read8bit(inputstream));

        case 3: // '\003'
            return new Character((char) read16bit(inputstream));

        case 4: // '\004'
            return new Short((short) read16bit(inputstream));

        case 5: // '\005'
            return new Integer(read32bit(inputstream));

        case 6: // '\006'
            return new Long(read64bit(inputstream));

        case 7: // '\007'
            return new Float(readFloat(inputstream));

        case 8: // '\b'
            return new Double(readDouble(inputstream));

        case 9: // '\t'
            return readUTF(inputstream);

        case 10: // '\n'
            return readByteArray24(inputstream);
        }
        throw new IOException((new StringBuilder()).append("Invalid object type: ").append(i).toString());
    }

    public static final void writeBasicObject(OutputStream outputstream, Object obj) 
            throws IOException {
        if (obj == null)
            write8bit(outputstream, 0);
        else if (obj instanceof String) {
            write8bit(outputstream, 9);
            writeUTF(outputstream, (String) obj);
        } else if (obj instanceof Boolean) {
            write8bit(outputstream, 1);
            writeBoolean(outputstream, ((Boolean) obj).booleanValue());
        } else if (obj instanceof Byte) {
            write8bit(outputstream, 2);
            write8bit(outputstream, ((Byte) obj).byteValue());
        } else if (obj instanceof Character) {
            write8bit(outputstream, 3);
            write16bit(outputstream, ((Character) obj).charValue());
        } else if (obj instanceof Short) {
            write8bit(outputstream, 4);
            write16bit(outputstream, ((Short) obj).shortValue());
        } else if (obj instanceof Integer) {
            write8bit(outputstream, 5);
            write32bit(outputstream, ((Integer) obj).intValue());
        } else if (obj instanceof Long) {
            write8bit(outputstream, 6);
            write64bit(outputstream, ((Long) obj).longValue());
        } else if (obj instanceof Float) {
            write8bit(outputstream, 7);
            writeFloat(outputstream, ((Float) obj).floatValue());
        } else if (obj instanceof Double) {
            write8bit(outputstream, 8);
            writeDouble(outputstream, ((Double) obj).doubleValue());
        } else if (obj instanceof byte[]) {
            write8bit(outputstream, 10);
            writeByteArray24(outputstream, (byte[]) (byte[]) obj);
        } else {
            throw new IllegalArgumentException((new StringBuilder()).append("Invalid basic object type: ").append(obj.getClass().getName()).toString());
        }
    }

    public static final Object readObject(InputStream inputstream)
            throws IOException, ClassNotFoundException {
        byte abyte0[] = readByteArray24(inputstream);
        ByteArrayInputStream bytearrayinputstream = new ByteArrayInputStream(abyte0);
        ObjectInputStream objectinputstream = new ObjectInputStream(bytearrayinputstream);
        return objectinputstream.readObject();
    }

    public static final void writeObject(OutputStream outputstream, Object obj)
            throws IOException {
        ByteArrayOutputStream bytearrayoutputstream = new ByteArrayOutputStream();
        ObjectOutputStream objectoutputstream = new ObjectOutputStream(bytearrayoutputstream);
        objectoutputstream.writeObject(obj);
        objectoutputstream.close();
        writeByteArray24(outputstream, bytearrayoutputstream.toByteArray());
    }

    public static final void writeUuid(OutputStream outputstream, UUID uuid)
            throws IOException {
        write64bit(outputstream, uuid.getMostSignificantBits());
        write64bit(outputstream, uuid.getLeastSignificantBits());
    }

    public static final UUID readUuid(InputStream inputstream)
            throws IOException {
        long l = read64bit(inputstream);
        long l1 = read64bit(inputstream);
        return new UUID(l, l1);
    }

}
