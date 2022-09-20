# tiny-lsmt

一个简易的LSM-Tree键值型数据库.

本项目在构建之初参考了 [TinyKvStore](https://github.com/x-hansong/TinyKvStore), 后者使用的是 Java 语言,
局限性在于没有 Compaction、数据的序列化使用了相对低效的 JSON 格式、封装性有所欠缺. 这里借鉴了它对 SSTable 模块的设计,
并且在 Compaction、序列化、封装性上都做了一些改进.