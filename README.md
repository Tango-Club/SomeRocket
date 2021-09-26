### TODOLIST
* 多线程同步落盘优化
* page size 调参
* 堆外内存cache
* PMEM cache优化
	* 已读page清出pmem 把pmem存储热数据占比提升到50%
	* pmem lib 接入

### 参考资料
* llpl库源码以及example - https://github.com/pmem/llpl
* llpl入门指南 https://pmem.io/2020/05/27/llpl-intro1.html
* JDK核心JAVA源码解析（5） - JAVA File MMAP原理解析 - https://zhuanlan.zhihu.com/p/258934554
* Linux中的Page Cache [一] - https://zhuanlan.zhihu.com/p/68071761
* Kafka存储模型 - https://blog.csdn.net/FlyingAngelet/article/details/84761466

### 资料总结

* pmem一次读写块大小256byte时读写性能较好。
* pmem写入线程数为8时可以达到最大性能。

### 当前存储结构

##### 内存(2G堆内内存/6G堆外内存)
* TopicQueueIdMap/StorageEngine 等内存对象
* 系统级的page cache
##### PMEM(60G)
* 热数据-33%
##### SSD(400G)
* 热数据-66%
* 冷数据-全量
