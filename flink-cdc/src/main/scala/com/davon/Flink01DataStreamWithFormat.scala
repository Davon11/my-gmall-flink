package com.davon

import com.alibaba.fastjson.JSONObject
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions
import com.alibaba.ververica.cdc.debezium.{DebeziumDeserializationSchema, StringDebeziumDeserializationSchema}
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.util.Collector
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.source.SourceRecord

object Flink01DataStreamWithFormat {
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
      .startupOptions(StartupOptions.initial())
      .deserializer(new StringDebeziumDeserializationSchema())
      .build()

    val sourceStream = env.addSource(cdcSource)

    sourceStream.print()

    env.execute()
  }

  @SerialVersionUID(-3168848963265670603L)
  class StringDebeziumDeserializationSchema() extends DebeziumDeserializationSchema[String] {
    /**
     * {
     * "data":"{"id":11,"tm_name":"sasa"}",
     * "db":"",
     * "tableName":"",
     * "op":"c u d",
     * "ts":""
     * }
     */
    @throws[Exception]
    override def deserialize(record: SourceRecord, out: Collector[String]): Unit = {
      val value = record.value().asInstanceOf[Struct]
      val jsonObj = new JSONObject()
      import scala.collection.JavaConversions._
      val after = value.getStruct("after")
      val fields = after.schema().fields()
      for (field <- fields) {
        jsonObj.put(field.name(), after.get(field))
      }
      val data = jsonObj.toString
      val source = value.getStruct("source")
      val db = source.getString("db")
      val table = source.getString("table")
      val op = value.getString("op")
      val ts = value.getInt64("ts_ms")

      val result = new JSONObject()
      result.put("data", data)
      result.put("db", db)
      result.put("tableName", table)
      result.put("op", op)
      result.put("ts", ts)

      out.collect(result.toString)
    }

    override def getProducedType: TypeInformation[String] = BasicTypeInfo.STRING_TYPE_INFO
  }

}
