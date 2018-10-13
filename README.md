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

# 系统架构简介

**系统整体设计思想 ：**

![Overview-1](https://github.com/garychenc/file-map/blob/master/doc/img/Overview-1.png "系统整体设计思想")

**系统设计思想详细描述 ：**

![Overview-2](https://github.com/garychenc/file-map/blob/master/doc/img/Overview-2.png "系统设计思想详细描述")

**系统详细架构设计描述 ：**

![Archi-1](https://github.com/garychenc/file-map/blob/master/doc/img/Sys-Arch.png "系统详细架构设计描述")

+ FileMap 接口是系统的入口，其实现类有 RepeatableKeyFileMap 和 UniqueKeyFileMap，两个类大同小异，FileMap 实现包含一个 BinaryFileStore 列表，每个 BinaryFileStore 对象代表一个文件存储集合。

+ BinaryFileStore 实现了上述描述的基于内存的 Map 类型索引，索引的每个 IndexBucket 指向一个 FileBaseContentStoreSlave 对象，该对象管理该 IndexBucket 指向的文件存储位置和该位置存储的数据。 FileBaseContentStoreSlave 对象管理着在哪个 Slave Data Store File 的哪个位置存储着真实的数据，并且提供数据的读取、写入方法。一个 Slave Data Store File 可对应多个 FileBaseContentStoreSlave 对象，整个 FileMap 包含多个 Slave Data Store File，数据分布在这些 Slave Data Store File 上，每个文件最大存储 64M 的数据。

+ FileBaseContentStoreMaster 对所有 FileBaseContentStoreSlave 和 Slave Data Store File 进行管理，每次 BinaryFileStore 需要存储一个新数据的时候，首先需要向 FileBaseContentStoreMaster 申请一个 FileBaseContentStoreSlave，申请的时候 FileBaseContentStoreMaster 就会在一个空闲的 Slave Data Store File 中为新申请的 FileBaseContentStoreSlave 分配存储空间，并且返回文件存储位置给 Slave，接着将 Slave 纳入 FileBaseContentStoreMaster 进行管理。然后，BinaryFileStore 通过新申请的 FileBaseContentStoreSlave 将数据存储到刚刚分配的文件存储空间中。假如，没有文件可以分配需要的空间，则新建一个文件，在该文件上分配所需空间，并且将该文件纳入管理列表，对文件进行管理。由于 BinaryFileStore 的 IndexBucket 索引只存储 FileBaseContentStoreSlave 的编号，所以每次读取数据的时候，首先需要通过 FileBaseContentStoreMaster 根据 Slave 编号获取 FileBaseContentStoreSlave 对象，然后通过 FileBaseContentStoreSlave 读取其管理的文件位置的数据。

+ FileBaseContentStoreMaster 具有无用文件存储块和无用文件垃圾回收功能，当某个 FileBaseContentStoreSlave 被标记为删除之后，该 Slave 和其管理的文件存储块将加入已删除 Slave 列表，该列表按照可存储空间从大到小排序，每次申请新的 Slave 的时候会优先从该列表中拿出存储空间刚好合适的 Slave 重新使用。当某个文件包含的所有 FileBaseContentStoreSlave 都被删除之后，该文件和其包含的 FileBaseContentStoreSlave 都会被从系统中删除，以节省内存和磁盘存储空间。但是文件编号保留，下次需要创建新文件的时候，优先使用该文件编号生产新文件的文件名。

+ 对数据进行更新的时候，首先按照数据读取方法的步骤搜索到需要的 FileBaseContentStoreSlave ，看看该 FileBaseContentStoreSlave 的存储空间是否能够容纳下更新后的数据，假如可以，则直接将新的数据写入到该 FileBaseContentStoreSlave 所指向的文件存储块中，假如不能容纳，则向 FileBaseContentStoreMaster 申请新的 FileBaseContentStoreSlave ，然后将数据写入到该新申请的 Slave 中，并且更新内存索引和索引文件中对应的数据。

+ 在数据插入到 FileMap 中的时候，从 FileBaseContentStoreMaster 中申请 FileBaseContentStoreSlave 时就会按照需存储数据实际大小的 2 倍申请存储空间，以使后续更新数据的时候可以在原空间上写入新的数据，除非新数据大小超过了原来数据大小的两倍才需要申请新的 FileBaseContentStoreSlave ，这样就避免了更新数据时，需频繁申请新的 FileBaseContentStoreSlave 。

+ BinaryFileStore 中的内存索引会按照一定的数据结构存储在索引存储文件中，索引存储文件的文件名后缀为 ： meta.db.map，FileBaseContentStoreMaster 管理的 Slave 和 Slave Data Store File 也会按照一定的数据结构存储在 Slave 管理文件和 Slave Data Store File 管理文件中，管理文件的后缀名分别为 ： slaves.management 和 store-files.management。 FileMap 第一次创建的时候会根据可存储数据量参数的大小生成索引存储文件、Slave 管理文件和 Slave Data Store File 管理文件，所以，第一次创建的时候速度会有点慢，可存储数据量参数设置越大，创建越慢，默认可存储数据量参数的大小为 50 万，创建时间为几秒。从第二次开始，创建 FileMap 只是加载索引存储文件、Slave 管理文件和 Slave Data Store File 管理文件，速度会快很多。可存储数据量为 50 万的这些文件总的加载时间为 200 毫秒左右。 FileMap 加载索引存储文件、Slave 管理文件和 Slave Data Store File 管理文件之后，将创建文件中存储的内存索引和 Slave、Slave Data Store File等数据结构。

+ Slave Data Store File 文件名的后缀为 slave.data，文件名最后的号码为文件编号，每个文件最大存储 64M 的数据。当文件大于等于 64M，新申请 FileBaseContentStoreSlave 时，将会新创建 Slave Data Store File， 用于存储新 Slave 的数据，新创建的文件编号将在原来最后一个文件编号的基础上加一。
