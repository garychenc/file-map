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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 *
 * @author Gary CHEN
 *
 */
public final class SerializationUtils {
    
    private SerializationUtils() {
    }
    
    /**
     * Serialize the given object to a byte array.
     * 
     * @param object
     *            the object to serialize
     * @return an array of bytes representing the object in a portable fashion
     */
    public final static byte[] serialize(Object object) {
        if (object == null) {
            return null;
        }

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(IOHelper.BUFFER_DEFAULT_ALLOCATED_SIZE)) {
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(object);
            oos.flush();
            return baos.toByteArray();
        } catch (IOException ex) {
            throw new IllegalArgumentException("Failed to serialize object of type: " + object.getClass(), ex);
        }
    }
    
    /**
     * Deserialize the byte array into an object.
     * 
     * @param bytes
     *            a serialized object
     * @return the result of deserializing the bytes
     */
    public final static Object deserialize(byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        
        try {
            ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes));
            return ois.readObject();
        } catch (IOException ex) {
            throw new IllegalArgumentException("Failed to deserialize object", ex);
        } catch (ClassNotFoundException ex) {
            throw new IllegalStateException("Failed to deserialize object type", ex);
        }
    }
    
    // Clone
    //-----------------------------------------------------------------------
    /**
     * <p>Deep clone an <code>Object</code> using serialization.</p>
     *
     * <p>This is many times slower than writing clone methods by hand
     * on all objects in your object graph. However, for complex object
     * graphs, or for those that don't support deep cloning this can
     * be a simple alternative implementation. Of course all the objects
     * must be <code>Serializable</code>.</p>
     * 
     * @param object  the <code>Serializable</code> object to clone
     * @return the cloned object
     */
    public final static Object clone(Serializable object) {
        return deserialize(serialize(object));
    }
    
}
