package com.davon.app.ods

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions
import com.davon.app.func.MyDebeziumDeserialization
import com.davon.app.util.MyKafkaUtil
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

object FlinkCDC {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "atguigu")
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    env.enableCheckpointing(5000L)
    env.getCheckpointConfig.setCheckpointTimeout(10*1000L)
    env.setStateBackend(new FsStateBackend("hdfs://hadoop102:9000/flink1109/ck"))
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000L))

    val cdcSource = MySQLSource.builder()
      .hostname("hadoop103").port(3306)
      .username("root").password("123456")
      .databaseList("gmall_2021")
      .startupOptions(StartupOptions.initial())
      .deserializer(new MyDebeziumDeserialization())
      .build()

    val cdcStream = env.addSource(cdcSource)

    val topic = "ods_base_db"
    cdcStream.addSink(MyKafkaUtil.getFlinkKafkaProducer(topic))

    env.execute()
  }
}
