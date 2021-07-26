package com.davon.app.func

import com.alibaba.fastjson.JSONObject
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.util.Collector
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.source.SourceRecord

class MyDebeziumDeserialization extends DebeziumDeserializationSchema[String]{
  @throws[Exception]
  override def deserialize(record: SourceRecord, out: Collector[String]): Unit = {
    val value = record.value().asInstanceOf[Struct]
    val op = value.getString("op")
    val afterJson = new JSONObject()
    val beforeJson = new JSONObject()
    import scala.collection.JavaConversions._
    if (!"c".equals(op)) {
      val before = value.getStruct("before")
      val fields = before.schema().fields()
      for (field <- fields) {
        beforeJson.put(field.name(), before.get(field))
      }
    }
    if (!"d".equals(op)) {
      val after = value.getStruct("after")
      val fields = after.schema().fields()
      for (field <- fields) {
        afterJson.put(field.name(), after.get(field))
      }
    }
    val source = value.getStruct("source")
    val db = source.getString("db")
    val table = source.getString("table")
    val ts = value.getInt64("ts_ms")

    val result = new JSONObject()
    result.put("data", afterJson.toString)
    result.put("beforeDate", beforeJson.toJSONString)
    result.put("db", db)
    result.put("tableName", table)
    result.put("op", op)
    result.put("ts", ts)

    out.collect(result.toString)
  }

  override def getProducedType: TypeInformation[String] = BasicTypeInfo.STRING_TYPE_INFO

}
