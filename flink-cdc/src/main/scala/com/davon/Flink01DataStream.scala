package com.davon

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

object Flink01DataStream {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "atguigu")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    env.enableCheckpointing(5000L)
    env.getCheckpointConfig.setCheckpointTimeout(5000L)
    env.setStateBackend(new FsStateBackend("hdfs://hadoop102:9000/flink1109/ck"))

    // 使用cdc方式读取 mysql 数据
    val cdcSource = MySQLSource.builder()
      .hostname("hadoop103").port(3306)
      .username("root").password("123456")
      .databaseList("gmall_2021").tableList("gmall_2021.spu_sale_attr_value")
      .startupOptions(StartupOptions.initial()).deserializer(new StringDebeziumDeserializationSchema())
      .build()

    val sourceStream = env.addSource(cdcSource)

    sourceStream.print()

    env.execute()
  }

}
