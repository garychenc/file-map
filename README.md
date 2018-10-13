# 简介

## FileMap 是一个用 Java 编写的，基于文件的高性能本地 Key - Value 存储库，可以理解为一个将数据存储在文件上的 Map，因此，其可存储巨大的数据量，而且具有持久化的效果。

FileMap 使用非常简单，创建一个 com.github.garychenc.filemap.RepeatableKeyFileMap 对象或 com.github.garychenc.filemap.UniqueKeyFileMap 对象即可开始使用。创建对象的时候指定数据存储的本地目录即可。

UniqueKeyFileMap 类确保添加新 Key - Value 的时候，新的 Key 与目前 Map 中所有的 Key 都不重复才能添加成功。

RepeatableKeyFileMap 类允许添加新 Key - Value 的时候，新的 Key 可与原来 Map 中已经存在的 Key 重复。假如需要 Key 的唯一性，由客户端自己保证。

UniqueKeyFileMap 类由于每次 add 操作都需要搜索 FileMap，确保即将添加的 key 不存在，所以性能会比 RepeatableKeyFileMap 差一些。而 RepeatableKeyFileMap则每次直接将记录添加到 FileMap 中，不进行判断，性能比 UniqueKeyFileMap 好一些，假如应用能够确保产生的 key 的唯一性，并且即使 key 重复也不造成大的影响，则使用 RepeatableKeyFileMap 性能好一些。

UniqueKeyFileMap 和 RepeatableKeyFileMap 类都是线程安全的。

FileMap 采用乐观离线锁对记录进行并发更新管理，所以，每次更新或者移除记录的时候需要提供预期的版本号，假如版本号不正确，说明在此之前记录已经被修改了，更新或移除此记录会抛出 VersionConflictedException，需要重新读出记录，拿到最新的版本号之后再进行更新或移除操作。一般操作模式为 ：

```

do {
     try {
         StoreEntry testValue = fileMap.read("TEST-KEY-1");
         fileMap.update((String) testValue.getKey(), "TEST-VALUE-111100-1", testValue.getVersionNumber());
         break;
     } catch (VersionConflictedException e) {
         continue;
     } catch (StoreReaderException | StoreWriterException e) {
         e.printStackTrace();
         break;
     }
 } while (true);

```

FileMap 使用完毕之后，必须调用其 close() 方法关闭 FileMap，否则可能造成资源泄露。

FileMap 基本使用方法示例 ：

```

try (FileMap fileMap = new UniqueKeyFileMap(storeDir.getAbsolutePath(), "Test-1")) {

   fileMap.add("TEST-KEY-1", "TEST-VALUE-1");
   fileMap.add("TEST-KEY-2", "TEST-VALUE-2");

   System.out.println(fileMap.size()); // Size = 2

   StoreEntry testValue1 = fileMap.read("TEST-KEY-1");
   StoreEntry testValue2 = fileMap.read("TEST-KEY-2");

   System.out.println(testValue1.getValue()); // Value = "TEST-VALUE-1"
   System.out.println(testValue1.getVersionNumber()); // Version = 0

   System.out.println(testValue2.getValue()); // Value = "TEST-VALUE-2"
   System.out.println(testValue2.getVersionNumber()); // Version = 0

   fileMap.update("TEST-KEY-2", "TEST-VALUE-2-001", testValue2.getVersionNumber());

   testValue2 = fileMap.read("TEST-KEY-2");
   System.out.println(testValue2.getValue()); // Value = "TEST-VALUE-2-001"
   System.out.println(testValue2.getVersionNumber()); // Version = 1

   fileMap.remove("TEST-KEY-1", testValue1.getVersionNumber());
   fileMap.remove("TEST-KEY-2", testValue2.getVersionNumber());
}

```

# 快速入门

接下来将会介绍如何将 FileMap 快速使用起来。首先，读者需要先安装 JDK 和 Maven，并且具有一定的 Java 开发知识，具有一个可用的 Java 开发 IDE。

+ 用 GIT clone 该项目的源代码，代码路径：https://github.com/garychenc/file-map.git 。

+ 使用 eclipse 或 idea 等 Java 开发 IDE 将代码以 Maven 项目的形式导入到 IDE 中。代码在 clone 路径的 code 目录中。

+ 在 IDE 中直接运行 test 源码目录的 com.github.garychenc.filemap.impl.test 包中的测试案例，无须做任何配置。

+ RepeatableKeyFileMapTest 测试案例对 RepeatableKeyFileMap 类进行测试。UniqueKeyFileMapTest 测试案例对 UniqueKeyFileMap 类进行测试。

+ testNormalUseCase 测试基本使用 case，testMultiThreadNormalUseCase1，testMultiThreadNormalUseCase2 进行并发使用测试，并且也可作为性能测试案例使用。

+ 通过对 com.github.garychenc.filemap.impl.test 包中的测试案例进行学习，即可掌握 FileMap 的使用方法。由于是作为类库使用，无须任何配置。

+ 使用 IDE 中 Maven 的 install 功能即可将 FileMap 打包为一个 jar 包，在其它项目中使用。

# 联系方式

Gary CHEN : email : gary.chen.c@qq.com
