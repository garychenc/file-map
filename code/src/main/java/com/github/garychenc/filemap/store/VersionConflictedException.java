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

/**
 *
 * @author Gary CHEN
 *
 */
public class VersionConflictedException extends Exception {

    private static final long serialVersionUID = 1L;

    private final long providedVersionNumber;
    private final long expectedVersionNumber;
    
    /**
     * 
     */
    public VersionConflictedException(long providedVersionNumber, long expectedVersionNumber) {
        this.providedVersionNumber = providedVersionNumber;
        this.expectedVersionNumber = expectedVersionNumber;
    }

    /**
     * @param message
     */
    public VersionConflictedException(String message, long providedVersionNumber, long expectedVersionNumber) {
        super(message);
        this.providedVersionNumber = providedVersionNumber;
        this.expectedVersionNumber = expectedVersionNumber;
    }

    /**
     * @param cause
     */
    public VersionConflictedException(Throwable cause, long providedVersionNumber, long expectedVersionNumber) {
        super(cause);
        this.providedVersionNumber = providedVersionNumber;
        this.expectedVersionNumber = expectedVersionNumber;
    }

    /**
     * @param message
     * @param cause
     */
    public VersionConflictedException(String message, Throwable cause, long providedVersionNumber, long expectedVersionNumber) {
        super(message, cause);
        this.providedVersionNumber = providedVersionNumber;
        this.expectedVersionNumber = expectedVersionNumber;
    }

    public long getProvidedVersionNumber() {
        return providedVersionNumber;
    }

    public long getExpectedVersionNumber() {
        return expectedVersionNumber;
    }


}
