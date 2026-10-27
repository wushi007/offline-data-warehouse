# 正式环境配置模板

小样本 demo 不读取这些文件。正式 HDFS/Hive 模式需将对应内容配置到你自己的 Spark 安装，不要复制本机用户名路径。

`$SPARK_HOME/conf/spark-defaults.conf` 最小示意：

```properties
spark.master spark://localhost:7077
spark.sql.catalogImplementation hive
spark.hadoop.fs.defaultFS hdfs://localhost:8020
spark.sql.session.timeZone UTC
spark.sql.sources.partitionOverwriteMode dynamic
```

若使用远端 Hive Metastore，需按实际安装设置 hive.metastore.uris；若使用嵌入式 metastore，必须保证不同进程解析到同一受控的 Derby 路径，避免误建第二份空元数据库。不要让多个进程并发写同一个 Derby。

Worker 配置见 `spark-env.sh.example`，内存参数按机器资源调整。环境文件决定 HDFS 的位置，项目 `.env` 决定业务目录，两者需一致。
