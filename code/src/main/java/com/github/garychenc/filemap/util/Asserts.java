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

/**
 *
 * @author Gary CHEN
 *
 */
public final class Asserts {

    private Asserts() {
    }

    public static final void notNull(String name, Object value) {
        if (value == null) {
            throw new IllegalArgumentException("Parameter '" + name + "' can not be null.");
        }
    }
    
    public static final void stringNotEmpty(String name, String value) {
        notNull(name, value);
        if ("".equals(value.trim())) {
            throw new IllegalArgumentException("String parameter '" + name + "' can not be empty.");
        }
    }
    
    public static final void longIsPositive(String name, long value) {
        if (value < 0) {
            throw new IllegalArgumentException("Long parameter '" + name + "' must be a positive number.");
        }
    }

    public static final void longGtZero(String name, long value) {
        if (value <= 0) {
            throw new IllegalArgumentException("Long parameter '" + name + "' must be greater than zero.");
        }        
    }
    
    public static final void integerIsPositive(String name, int value) {
        if (value < 0) {
            throw new IllegalArgumentException("Integer parameter '" + name + "' must be a positive number.");
        }
    }

    public static final void doubleIsPositive(String name, double value) {
        if (value < 0) {
            throw new IllegalArgumentException("Double parameter '" + name + "' must be a positive number.");
        }
    }
    
    public static final void integerGtZero(String name, int value) {
        if (value <= 0) {
            throw new IllegalArgumentException("Integer parameter '" + name + "' must be greater than zero.");
        }        
    }

    public static void isTrue(boolean expression, String message) {
            if (!expression) {
                    throw new IllegalArgumentException(message);
            }
    }

    public static void isTrue(boolean expression) {
            isTrue(expression, "[Assertion failed] - this expression must be true");
    }

    public static void isNull(Object object, String message) {
            if (object != null) {
                    throw new IllegalArgumentException(message);
            }
    }

    public static void isNull(Object object) {
            isNull(object, "[Assertion failed] - the object argument must be null");
    }
}

















