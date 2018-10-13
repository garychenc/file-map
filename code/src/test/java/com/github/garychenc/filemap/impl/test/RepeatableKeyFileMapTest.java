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
package com.github.garychenc.filemap.impl.test;

import com.github.garychenc.filemap.FileMap;
import com.github.garychenc.filemap.RepeatableKeyFileMap;
import com.github.garychenc.filemap.store.StoreEntry;
import com.github.garychenc.filemap.store.StoreReaderException;
import com.github.garychenc.filemap.store.StoreWriterException;
import com.github.garychenc.filemap.store.VersionConflictedException;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.net.URL;
import java.util.concurrent.*;

public class RepeatableKeyFileMapTest {

    @Test
    public void testNormalUseCase() throws Exception {
        URL forLocationFileUrl = this.getClass().getClassLoader().getResource("for-location.txt");
        File forLocationFile = new File(forLocationFileUrl.toURI());
        File storeDirFile = new File(forLocationFile.getParentFile(), "test-store-1");

        long startTime = System.currentTimeMillis();
        try (FileMap fileMap = new RepeatableKeyFileMap(storeDirFile.getAbsolutePath(), "Test-1", true, false, 256)) {
            System.out.println("Time Cost : " + (System.currentTimeMillis() - startTime));

            Assert.assertTrue(fileMap.add("TEST-KEY-1", "TEST-VALUE-1"));
            Assert.assertTrue(fileMap.add("TEST-KEY-2", "TEST-VALUE-2"));
            Assert.assertTrue(fileMap.add("TEST-KEY-3", "TEST-VALUE-3"));
            Assert.assertTrue(fileMap.add("TEST-KEY-4", "TEST-VALUE-4"));
            Assert.assertTrue(fileMap.add("TEST-KEY-5", "TEST-VALUE-5"));
            Assert.assertTrue(fileMap.add("TEST-KEY-1", "TEST-VALUE-1-001"));

            Assert.assertEquals(6, fileMap.size());

            StoreEntry testValue1 = fileMap.read("TEST-KEY-1");
            StoreEntry testValue2 = fileMap.read("TEST-KEY-2");
            StoreEntry testValue3 = fileMap.read("TEST-KEY-3");
            StoreEntry testValue4 = fileMap.read("TEST-KEY-4");
            StoreEntry testValue5 = fileMap.read("TEST-KEY-5");
            StoreEntry testValue6 = fileMap.read("TEST-KEY-6");

            Assert.assertEquals("TEST-VALUE-1", testValue1.getValue());
            Assert.assertEquals(0, testValue1.getVersionNumber());
            Assert.assertEquals("TEST-VALUE-2", testValue2.getValue());
            Assert.assertEquals(0, testValue2.getVersionNumber());
            Assert.assertEquals("TEST-VALUE-3", testValue3.getValue());
            Assert.assertEquals(0, testValue3.getVersionNumber());
            Assert.assertEquals("TEST-VALUE-4", testValue4.getValue());
            Assert.assertEquals(0, testValue4.getVersionNumber());
            Assert.assertEquals("TEST-VALUE-5", testValue5.getValue());
            Assert.assertEquals(0, testValue5.getVersionNumber());
            Assert.assertNull(testValue6);

            Assert.assertTrue(fileMap.update("TEST-KEY-2", "TEST-VALUE-2-001", testValue2.getVersionNumber()));
            Assert.assertTrue(fileMap.update("TEST-KEY-3", "TEST-VALUE-3-000001", testValue3.getVersionNumber()));
            Assert.assertFalse(fileMap.update("TEST-KEY-6", "TEST-VALUE-6-000001", testValue3.getVersionNumber()));

            testValue2 = fileMap.read("TEST-KEY-2");
            testValue3 = fileMap.read("TEST-KEY-3");
            testValue6 = fileMap.read("TEST-KEY-6");

            Assert.assertEquals("TEST-VALUE-2-001", testValue2.getValue());
            Assert.assertEquals(1, testValue2.getVersionNumber());
            Assert.assertEquals("TEST-VALUE-3-000001", testValue3.getValue());
            Assert.assertEquals(1, testValue3.getVersionNumber());
            Assert.assertNull(testValue6);

            try {
                fileMap.update("TEST-KEY-3", "TEST-VALUE-3-000002", 0);
                Assert.fail();
            } catch (VersionConflictedException ex) {
                ex.printStackTrace();
            }

            testValue3 = fileMap.read("TEST-KEY-3");
            Assert.assertEquals("TEST-VALUE-3-000001", testValue3.getValue());
            Assert.assertEquals(1, testValue3.getVersionNumber());

            Assert.assertTrue(fileMap.remove("TEST-KEY-2", testValue2.getVersionNumber()));
            Assert.assertTrue(fileMap.remove("TEST-KEY-3", testValue3.getVersionNumber()));
            Assert.assertFalse(fileMap.remove("TEST-KEY-6", testValue3.getVersionNumber()));

            testValue2 = fileMap.read("TEST-KEY-2");
            testValue3 = fileMap.read("TEST-KEY-3");
            testValue6 = fileMap.read("TEST-KEY-6");

            Assert.assertNull(testValue2);
            Assert.assertNull(testValue3);
            Assert.assertNull(testValue6);

            try {
                fileMap.remove("TEST-KEY-1", 1);
                Assert.fail();
            } catch (VersionConflictedException ex) {
                ex.printStackTrace();
            }

            testValue1 = fileMap.read("TEST-KEY-1");
            Assert.assertEquals("TEST-VALUE-1", testValue1.getValue());
            Assert.assertEquals(0, testValue1.getVersionNumber());

            Assert.assertTrue(fileMap.remove("TEST-KEY-1", testValue1.getVersionNumber()));
            Assert.assertTrue(fileMap.remove("TEST-KEY-4", testValue4.getVersionNumber()));
            Assert.assertTrue(fileMap.remove("TEST-KEY-5", testValue5.getVersionNumber()));
            Assert.assertTrue(fileMap.remove("TEST-KEY-1", testValue1.getVersionNumber()));
        }
    }

    @Test
    public void testExtendStorageUseCase() throws Exception {
        URL forLocationFileUrl = this.getClass().getClassLoader().getResource("for-location.txt");
        File forLocationFile = new File(forLocationFileUrl.toURI());
        File storeDirFile = new File(forLocationFile.getParentFile(), "test-store-1");

        long startTime = System.currentTimeMillis();
        try (FileMap fileMap = new RepeatableKeyFileMap(storeDirFile.getAbsolutePath(), "Test-2", true, false, 256)) {
            System.out.println("Time Cost : " + (System.currentTimeMillis() - startTime));

            for (int i = 0; i < 255; i++) {
                Assert.assertTrue(fileMap.add("TEST-KEY-1-" + String.valueOf(i), "TEST-VALUE-0000001-" + String.valueOf(i)));
            }

            Assert.assertTrue(fileMap.add("TEST-KEY-1-" + String.valueOf(255), "TEST-VALUE-0000001-" + String.valueOf(255)));
            Assert.assertTrue(fileMap.add("TEST-KEY-1-" + String.valueOf(256), "TEST-VALUE-0000001-" + String.valueOf(256)));

            for (int i = 257; i < 512; i++) {
                Assert.assertTrue(fileMap.add("TEST-KEY-1-" + String.valueOf(i), "TEST-VALUE-0000001-" + String.valueOf(i)));
            }

            Assert.assertTrue(fileMap.add("TEST-KEY-1-" + String.valueOf(512), "TEST-VALUE-0000001-" + String.valueOf(512)));
            Assert.assertTrue(fileMap.add("TEST-KEY-1-" + String.valueOf(513), "TEST-VALUE-0000001-" + String.valueOf(513)));
            Assert.assertTrue(fileMap.add("TEST-KEY-1-" + String.valueOf(514), "TEST-VALUE-0000001-" + String.valueOf(514)));
            Assert.assertTrue(fileMap.add("TEST-KEY-1-" + String.valueOf(515), "TEST-VALUE-0000001-" + String.valueOf(515)));
        }

        try (FileMap fileMap = new RepeatableKeyFileMap(storeDirFile.getAbsolutePath(), "Test-2", true, false, 256)) {
            for (int i = 0; i < 516; i++) {
                StoreEntry testValue = fileMap.read("TEST-KEY-1-" + String.valueOf(i));
                Assert.assertEquals("TEST-VALUE-0000001-" + String.valueOf(i), testValue.getValue());
                Assert.assertEquals(0, testValue.getVersionNumber());
            }

            StoreEntry testValue516 = fileMap.read("TEST-KEY-1-" + String.valueOf(516));
            StoreEntry testValue517 = fileMap.read("TEST-KEY-1-" + String.valueOf(517));
            Assert.assertNull(testValue516);
            Assert.assertNull(testValue517);

            for (int i = 0; i < 255; i++) {
                Assert.assertTrue(fileMap.add("TEST-KEY-1-" + String.valueOf(i), "TEST-VALUE-1110001-" + String.valueOf(i)));
            }

            Assert.assertEquals(771, fileMap.size());

            for (int i = 0; i < 516; i++) {
                StoreEntry testValue = fileMap.read("TEST-KEY-1-" + String.valueOf(i));
                Assert.assertTrue(fileMap.remove((String) testValue.getKey(), testValue.getVersionNumber()));
            }

            for (int i = 0; i < 255; i++) {
                StoreEntry testValue = fileMap.read("TEST-KEY-1-" + String.valueOf(i));
                Assert.assertTrue(fileMap.remove((String) testValue.getKey(), testValue.getVersionNumber()));
            }

        }
    }

    @Test
    public void testMultiThreadNormalUseCase1() throws Exception {
        URL forLocationFileUrl = this.getClass().getClassLoader().getResource("for-location.txt");
        File forLocationFile = new File(forLocationFileUrl.toURI());
        File storeDirFile = new File(forLocationFile.getParentFile(), "test-store-1");

        ExecutorService threadPool = new ThreadPoolExecutor(20, 20, 180000, TimeUnit.MILLISECONDS, new LinkedTransferQueue<>());
        long startTime = System.currentTimeMillis();
        try (FileMap fileMap = new RepeatableKeyFileMap(storeDirFile.getAbsolutePath(), "Test-3")) {
            System.out.println("Create Finished. Time Cost : " + (System.currentTimeMillis() - startTime));

            startTime = System.currentTimeMillis();
            CountDownLatch l1 = new CountDownLatch(20 * 20000);
            for (int j = 0; j < 20; j++) {
                final int k = j;
                threadPool.submit(() -> {
                    for (int i = 0; i < 20000; i++) {
                        try {
                            Assert.assertTrue(fileMap.add("TEST-KEY-" + String.valueOf(k) + "-" + String.valueOf(i), "TEST-VALUE-000000" + String.valueOf(k) + "-" + String.valueOf(i)));
                            l1.countDown();
                        } catch (StoreWriterException e) {
                            e.printStackTrace();
                        }
                    }

                });
            }

            l1.await();
            long cost = (System.currentTimeMillis() - startTime);
            System.out.println("Add Finished. Time Cost : " + cost + ", TPS : " + ((float)(20 * 20000) / (float)cost * 1000F));

            startTime = System.currentTimeMillis();
            CountDownLatch l2 = new CountDownLatch(20 * 20000);
            for (int j = 0; j < 20; j++) {
                final int k = j;
                threadPool.submit(() -> {
                    for (int i = 0; i < 20000; i++) {
                        try {
                            StoreEntry testValue = fileMap.read("TEST-KEY-" + String.valueOf(k) + "-" + String.valueOf(i));
                            Assert.assertEquals("TEST-VALUE-000000" + String.valueOf(k) + "-" + String.valueOf(i), testValue.getValue());
                            Assert.assertEquals(0, testValue.getVersionNumber());
                            l2.countDown();
                        } catch (StoreReaderException e) {
                            e.printStackTrace();
                        }
                    }
                });
            }

            l2.await();
            cost = (System.currentTimeMillis() - startTime);
            System.out.println("Read Finished. Time Cost : " + cost + ", QPS : " + ((float)(20 * 20000) / (float)cost * 1000F));

            startTime = System.currentTimeMillis();
            CountDownLatch l3 = new CountDownLatch(20 * 20000);
            for (int j = 0; j < 20; j++) {
                final int k = j;
                threadPool.submit(() -> {
                    for (int i = 0; i < 20000; i++) {
                        try {
                            Assert.assertTrue(fileMap.update("TEST-KEY-" + String.valueOf(k) + "-" + String.valueOf(i), "TEST-VALUE-111100" + String.valueOf(k) + "-" + String.valueOf(i), 0));
                            l3.countDown();
                        } catch (StoreWriterException | VersionConflictedException e) {
                            e.printStackTrace();
                        }
                    }
                });
            }

            l3.await();
            cost = (System.currentTimeMillis() - startTime);
            System.out.println("Update Finished. Time Cost : " + cost + ", TPS : " + ((float)(20 * 20000) / (float)cost * 1000F));

            startTime = System.currentTimeMillis();
            CountDownLatch l4 = new CountDownLatch(20 * 20000);
            for (int j = 0; j < 20; j++) {
                final int k = j;
                threadPool.submit(() -> {
                    for (int i = 0; i < 20000; i++) {
                        try {
                            StoreEntry testValue = fileMap.read("TEST-KEY-" + String.valueOf(k) + "-" + String.valueOf(i));
                            Assert.assertEquals("TEST-VALUE-111100" + String.valueOf(k) + "-" + String.valueOf(i), testValue.getValue());
                            Assert.assertEquals(1, testValue.getVersionNumber());
                            l4.countDown();
                        } catch (StoreReaderException e) {
                            e.printStackTrace();
                        }
                    }
                });
            }

            l4.await();
            cost = (System.currentTimeMillis() - startTime);
            System.out.println("Read after update Finished. Time Cost : " + cost + ", QPS : " + ((float)(20 * 20000) / (float)cost * 1000F));

            startTime = System.currentTimeMillis();
            CountDownLatch l5 = new CountDownLatch(20 * 20000);
            for (int j = 0; j < 20; j++) {
                final int k = j;
                threadPool.submit(() -> {
                    for (int i = 0; i < 20000; i++) {
                        try {
                            Assert.assertTrue(fileMap.remove("TEST-KEY-" + String.valueOf(k) + "-" + String.valueOf(i), 1));
                            l5.countDown();
                        } catch (StoreWriterException | VersionConflictedException e) {
                            e.printStackTrace();
                        }
                    }
                });
            }

            l5.await();
            cost = (System.currentTimeMillis() - startTime);
            System.out.println("Remove Finished. Time Cost : " + cost + ", TPS : " + ((float)(20 * 20000) / (float)cost * 1000F));
        }
    }

    @Test
    public void testMultiThreadNormalUseCase2() throws Exception {
        URL forLocationFileUrl = this.getClass().getClassLoader().getResource("for-location.txt");
        File forLocationFile = new File(forLocationFileUrl.toURI());
        File storeDirFile = new File(forLocationFile.getParentFile(), "test-store-1");

        ExecutorService threadPool = new ThreadPoolExecutor(200, 200, 180000, TimeUnit.MILLISECONDS, new LinkedTransferQueue<>());
        long startTime = System.currentTimeMillis();
        try (FileMap fileMap = new RepeatableKeyFileMap(storeDirFile.getAbsolutePath(), "Test-3")) {
            System.out.println("Create Finished. Time Cost : " + (System.currentTimeMillis() - startTime));

            startTime = System.currentTimeMillis();
            CountDownLatch l1 = new CountDownLatch(200 * 2000);
            for (int j = 0; j < 200; j++) {
                final int k = j;
                threadPool.submit(() -> {
                    for (int i = 0; i < 2000; i++) {
                        try {
                            Assert.assertTrue(fileMap.add("TEST-KEY-" + String.valueOf(k) + "-" + String.valueOf(i), "TEST-VALUE-000000" + String.valueOf(k) + "-" + String.valueOf(i)));
                            l1.countDown();
                        } catch (StoreWriterException e) {
                            e.printStackTrace();
                        }
                    }

                });
            }

            l1.await();
            long cost = (System.currentTimeMillis() - startTime);
            System.out.println("Add Finished. Time Cost : " + cost + ", TPS : " + ((float)(200 * 2000) / (float)cost * 1000F));

            startTime = System.currentTimeMillis();
            CountDownLatch l2 = new CountDownLatch(200 * 2000);
            for (int j = 0; j < 200; j++) {
                final int k = j;
                threadPool.submit(() -> {
                    for (int i = 0; i < 2000; i++) {
                        try {
                            StoreEntry testValue = fileMap.read("TEST-KEY-" + String.valueOf(k) + "-" + String.valueOf(i));
                            Assert.assertEquals("TEST-VALUE-000000" + String.valueOf(k) + "-" + String.valueOf(i), testValue.getValue());
                            Assert.assertEquals(0, testValue.getVersionNumber());
                            l2.countDown();
                        } catch (StoreReaderException e) {
                            e.printStackTrace();
                        }
                    }
                });
            }

            l2.await();
            cost = (System.currentTimeMillis() - startTime);
            System.out.println("Read Finished. Time Cost : " + cost + ", QPS : " + ((float)(200 * 2000) / (float)cost * 1000F));

            startTime = System.currentTimeMillis();
            CountDownLatch l3 = new CountDownLatch(200 * 2000);
            for (int j = 0; j < 200; j++) {
                final int k = j;
                threadPool.submit(() -> {
                    for (int i = 0; i < 2000; i++) {
                        try {
                            Assert.assertTrue(fileMap.update("TEST-KEY-" + String.valueOf(k) + "-" + String.valueOf(i), "TEST-VALUE-111100" + String.valueOf(k) + "-" + String.valueOf(i), 0));
                            l3.countDown();
                        } catch (StoreWriterException | VersionConflictedException e) {
                            e.printStackTrace();
                        }
                    }
                });
            }

            l3.await();
            cost = (System.currentTimeMillis() - startTime);
            System.out.println("Update Finished. Time Cost : " + cost + ", TPS : " + ((float)(200 * 2000) / (float)cost * 1000F));

            startTime = System.currentTimeMillis();
            CountDownLatch l4 = new CountDownLatch(200 * 2000);
            for (int j = 0; j < 200; j++) {
                final int k = j;
                threadPool.submit(() -> {
                    for (int i = 0; i < 2000; i++) {
                        try {
                            StoreEntry testValue = fileMap.read("TEST-KEY-" + String.valueOf(k) + "-" + String.valueOf(i));
                            Assert.assertEquals("TEST-VALUE-111100" + String.valueOf(k) + "-" + String.valueOf(i), testValue.getValue());
                            Assert.assertEquals(1, testValue.getVersionNumber());
                            l4.countDown();
                        } catch (StoreReaderException e) {
                            e.printStackTrace();
                        }
                    }
                });
            }

            l4.await();
            cost = (System.currentTimeMillis() - startTime);
            System.out.println("Read after update Finished. Time Cost : " + cost + ", QPS : " + ((float)(200 * 2000) / (float)cost * 1000F));

            startTime = System.currentTimeMillis();
            CountDownLatch l5 = new CountDownLatch(200 * 2000);
            for (int j = 0; j < 200; j++) {
                final int k = j;
                threadPool.submit(() -> {
                    for (int i = 0; i < 2000; i++) {
                        try {
                            Assert.assertTrue(fileMap.remove("TEST-KEY-" + String.valueOf(k) + "-" + String.valueOf(i), 1));
                            l5.countDown();
                        } catch (StoreWriterException | VersionConflictedException e) {
                            e.printStackTrace();
                        }
                    }
                });
            }

            l5.await();
            cost = (System.currentTimeMillis() - startTime);
            System.out.println("Remove Finished. Time Cost : " + cost + ", TPS : " + ((float)(200 * 2000) / (float)cost * 1000F));
        }
    }

    @Test
    public void testMultiThreadNormalUseCase3() throws Exception {
        URL forLocationFileUrl = this.getClass().getClassLoader().getResource("for-location.txt");
        File forLocationFile = new File(forLocationFileUrl.toURI());
        File storeDirFile = new File(forLocationFile.getParentFile(), "test-store-1");

        ExecutorService threadPool = new ThreadPoolExecutor(30, 30, 180000, TimeUnit.MILLISECONDS, new LinkedTransferQueue<>());
        long startTime = System.currentTimeMillis();
        try (FileMap fileMap = new RepeatableKeyFileMap(storeDirFile.getAbsolutePath(), "Test-4")) {
            System.out.println("Create Finished. Time Cost : " + (System.currentTimeMillis() - startTime));

            startTime = System.currentTimeMillis();
            CountDownLatch l1 = new CountDownLatch(15 * 3000);
            for (int j = 0; j < 15; j++) {
                final int k = j;
                threadPool.submit(() -> {
                    for (int i = 0; i < 3000; i++) {
                        try {
                            Assert.assertTrue(fileMap.add("TEST-KEY-" + String.valueOf(k) + "-" + String.valueOf(i), "TEST-VALUE-000000" + String.valueOf(k) + "-" + String.valueOf(i)));
                        } catch (StoreWriterException e) {
                            e.printStackTrace();
                        }
                    }

                });
            }

            for (int j = 0; j < 15; j++) {
                final int k = j;
                threadPool.submit(() -> {
                    for (int i = 0; i < 3000; i++) {
                        try {
                            StoreEntry testValue = fileMap.read("TEST-KEY-" + String.valueOf(k) + "-" + String.valueOf(i));
                            while (testValue == null) {
                                try {
                                    Thread.sleep(50);
                                } catch (InterruptedException e) {
                                    return;
                                }

                                testValue = fileMap.read("TEST-KEY-" + String.valueOf(k) + "-" + String.valueOf(i));
                            }

                            Assert.assertEquals("TEST-VALUE-000000" + String.valueOf(k) + "-" + String.valueOf(i), testValue.getValue());
                            Assert.assertEquals(0, testValue.getVersionNumber());
                            l1.countDown();
                        } catch (StoreReaderException e) {
                            e.printStackTrace();
                        }
                    }
                });
            }

            l1.await();
            long cost = (System.currentTimeMillis() - startTime);
            System.out.println("Add and Read Finished. Time Cost : " + cost + ", QPS : " + ((float)(15 * 3000) / (float)cost * 1000F));

            startTime = System.currentTimeMillis();
            CountDownLatch l3 = new CountDownLatch(15 * 3000);
            for (int j = 0; j < 15; j++) {
                final int k = j;
                threadPool.submit(() -> {
                    for (int i = 0; i < 3000; i++) {
                        try {
                            StoreEntry testValue = fileMap.read("TEST-KEY-" + String.valueOf(k) + "-" + String.valueOf(i));
                            Assert.assertTrue(fileMap.update("TEST-KEY-" + String.valueOf(k) + "-" + String.valueOf(i), "TEST-VALUE-111100" + String.valueOf(k) + "-" + String.valueOf(i), testValue.getVersionNumber()));
                            l3.countDown();
                        } catch (StoreWriterException | VersionConflictedException | StoreReaderException e) {
                            e.printStackTrace();
                        }
                    }
                });
            }

            l3.await();
            cost = (System.currentTimeMillis() - startTime);
            System.out.println("Update Finished. Time Cost : " + cost + ", TPS : " + ((float)(15 * 3000) / (float)cost * 1000F));

            startTime = System.currentTimeMillis();
            CountDownLatch l4 = new CountDownLatch(15 * 3000);
            for (int j = 0; j < 15; j++) {
                final int k = j;
                threadPool.submit(() -> {
                    for (int i = 0; i < 3000; i++) {
                        try {
                            StoreEntry testValue = fileMap.read("TEST-KEY-" + String.valueOf(k) + "-" + String.valueOf(i));
                            Assert.assertEquals("TEST-VALUE-111100" + String.valueOf(k) + "-" + String.valueOf(i), testValue.getValue());
                            Assert.assertEquals(1, testValue.getVersionNumber());
                            l4.countDown();
                        } catch (StoreReaderException e) {
                            e.printStackTrace();
                        }
                    }
                });
            }

            l4.await();
            cost = (System.currentTimeMillis() - startTime);
            System.out.println("Read after update Finished. Time Cost : " + cost + ", QPS : " + ((float)(15 * 3000) / (float)cost * 1000F));

            startTime = System.currentTimeMillis();
            CountDownLatch l5 = new CountDownLatch(15 * 3000);
            for (int j = 0; j < 15; j++) {
                final int k = j;
                threadPool.submit(() -> {
                    for (int i = 0; i < 3000; i++) {
                        try {
                            StoreEntry testValue = fileMap.read("TEST-KEY-" + String.valueOf(k) + "-" + String.valueOf(i));
                            Assert.assertTrue(fileMap.remove("TEST-KEY-" + String.valueOf(k) + "-" + String.valueOf(i), testValue.getVersionNumber()));
                            l5.countDown();
                        } catch (StoreWriterException | VersionConflictedException | StoreReaderException e) {
                            e.printStackTrace();
                        }
                    }
                });
            }

            l5.await();
            cost = (System.currentTimeMillis() - startTime);
            System.out.println("Remove Finished. Time Cost : " + cost + ", TPS : " + ((float)(15 * 3000) / (float)cost * 1000F));
        }
    }
}
