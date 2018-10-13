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
package com.github.garychenc.filemap.store;

import java.io.Externalizable;
import java.io.Serializable;

/**
 *
 * @author Gary CHEN
 *
 */
public interface StoreWriter extends Store {

    public boolean add(String key, byte[] value) throws StoreIsFullException, StoreWriterException;
    public boolean add(long key, byte[] value) throws StoreIsFullException, StoreWriterException;
    
    public boolean add(String key, String value) throws StoreIsFullException, StoreWriterException;
    public boolean add(long key, String value) throws StoreIsFullException, StoreWriterException;
    
    public boolean add(String key, Externalizable value) throws StoreIsFullException, StoreWriterException;
    public boolean add(long key, Externalizable value) throws StoreIsFullException, StoreWriterException;
    
    public boolean add(String key, Serializable value) throws StoreIsFullException, StoreWriterException;
    public boolean add(long key, Serializable value) throws StoreIsFullException, StoreWriterException;
    
    public boolean remove(String key, long versionNumber) throws StoreWriterException, VersionConflictedException;
    public boolean remove(long key, long versionNumber) throws StoreWriterException, VersionConflictedException;

    public boolean update(String key, byte[] value, long versionNumber) throws StoreWriterException, VersionConflictedException;
    public boolean update(long key, byte[] value, long versionNumber) throws StoreWriterException, VersionConflictedException;
    
    public boolean update(String key, String value, long versionNumber) throws StoreWriterException, VersionConflictedException;
    public boolean update(long key, String value, long versionNumber) throws StoreWriterException, VersionConflictedException;
    
    public boolean update(String key, Externalizable value, long versionNumber) throws StoreWriterException, VersionConflictedException;
    public boolean update(long key, Externalizable value, long versionNumber) throws StoreWriterException, VersionConflictedException;
    
    public boolean update(String key, Serializable value, long versionNumber) throws StoreWriterException, VersionConflictedException;
    public boolean update(long key, Serializable value, long versionNumber) throws StoreWriterException, VersionConflictedException;

}
