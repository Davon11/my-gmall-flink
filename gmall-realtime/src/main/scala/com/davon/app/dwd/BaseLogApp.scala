package com.davon.app.dwd

import java.util

import com.alibaba.fastjson.{JSON, JSONObject}
import com.davon.util.{MyFlinkUtil, MyKafkaUtil}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @description: ${description}
 * @author: Davon
 * @created: 2021/07/28 22:08
 */
object BaseLogApp {
  private val baseLogTopic = "ods_base_log"
  private val groupId = "base_log_app_group"

  private val dirtyJsonTag = new OutputTag[String]("DirtyData")
  private val startTag = new OutputTag[String]("start")
  private val displayTag = new OutputTag[String]("display")

  def main(args: Array[String]): Unit = {
    // 1. 获取 env，设置 ck
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//    MyFlinkUtil.setCKProperties(env, 5000L, 10000L)

    // 2. 从 kafka 读取 log 数据
    val sourceStream: DataStream[String] = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(groupId, baseLogTopic))

    // 3. 转换为 json对象， 不能转换的残缺 string 走侧输出
    val jsonDs: DataStream[JSONObject] = sourceStream.process(new NotJsonStrProcess)
    val dirtyDs: DataStream[String] = jsonDs.getSideOutput(dirtyJsonTag)

    // 4. 按设备id分组 并 校验是否为新用户
    val pageLogDS = jsonDs.keyBy(jsonObj => jsonObj.getJSONObject("common").getString("mid"))
      .process(new JudgeIsNewDevice)
      .process(new LogPartitionProcess)

    // 5. 将三种日志分流
    val startLogDS: DataStream[String] = pageLogDS.getSideOutput(startTag)
    val displayLogDS: DataStream[String] = pageLogDS.getSideOutput(displayTag)

    dirtyDs.print("Dirty>>>>>>>>>>>")
    startLogDS.print("Start>>>>>>>>>>>>")
    displayLogDS.print("Display>>>>>>>>>>>>>")
    pageLogDS.print("Page>>>>>>>>>>>")

//    dirtyDs.addSink(MyKafkaUtil.getFlinkKafkaProducer("dwd_dirty_log"))
    startLogDS.addSink(MyKafkaUtil.getFlinkKafkaProducer("dwd_start_log"))
    displayLogDS.addSink(MyKafkaUtil.getFlinkKafkaProducer("dwd_display_log"))
    pageLogDS.addSink(MyKafkaUtil.getFlinkKafkaProducer("dwd_page_log"))

    // 6. 执行
    env.execute()
  }

  class JudgeIsNewDevice extends KeyedProcessFunction[String, JSONObject, JSONObject] {
    lazy private val isOldDevice: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor("isOldDevice", classOf[Boolean]))

    override def processElement(i: JSONObject, context: KeyedProcessFunction[String, JSONObject, JSONObject]#Context, collector: Collector[JSONObject]): Unit = {
      val isNew: String = i.getJSONObject("common").getString("is_new")

      if ("1".equals(isNew)) {
        if (isOldDevice.value()) {
          i.getJSONObject("common").put("is_new", "0")
        }
      }

      isOldDevice.update(true)

      collector.collect(i)
    }
  }

  class NotJsonStrProcess extends ProcessFunction[String, JSONObject] {
    override def processElement(i: String, context: ProcessFunction[String, JSONObject]#Context, collector: Collector[JSONObject]): Unit = {
      try {
        val jsonObj: JSONObject = JSON.parseObject(i)
        collector.collect(jsonObj)
      } catch {
        case e: Exception =>
          context.output(dirtyJsonTag, i)
      }
    }
  }

  /*//获取启动数据
                String start = jsonObject.getString("start");
                if (start != null && start.length() > 0) {
                    //为启动数据
                    ctx.output(startOutPutTag, jsonObject.toJSONString());
                } else {
                    //不是启动数据,则一定为页面数据
                    out.collect(jsonObject.toJSONString());

                    //获取曝光数据
                    JSONArray displays = jsonObject.getJSONArray("displays");

                    //取出公共字段、页面信息、时间戳
                    JSONObject common = jsonObject.getJSONObject("common");
                    JSONObject page = jsonObject.getJSONObject("page");
                    Long ts = jsonObject.getLong("ts");

                    //判断曝光数据是否存在
                    if (displays != null && displays.size() > 0) {

                        JSONObject displayObj = new JSONObject();
                        displayObj.put("common", common);
                        displayObj.put("page", page);
                        displayObj.put("ts", ts);

                        //遍历每一个曝光信息
                        for (Object display : displays) {
                            displayObj.put("display", display);
                            //输出到侧输出流
                            ctx.output(displayOutputTag, displayObj.toJSONString());
                        }
                    }
                }*/

  class LogPartitionProcess extends ProcessFunction[JSONObject, String] {
    override def processElement(jsonObject: JSONObject, context: ProcessFunction[JSONObject, String]#Context, collector: Collector[String]): Unit = {
      val startLogStr: String = jsonObject.getString("start")
      if (startLogStr != null && !startLogStr.isEmpty) {
        context.output(startTag, startLogStr)
      } else {
        collector.collect(jsonObject.toJSONString)
        //获取曝光数据
        val displays = jsonObject.getJSONArray("displays");

        if (displays == null || displays.isEmpty) {
          return
        }

        //取出公共字段、页面信息、时间戳
        val common = jsonObject.getJSONObject("common");
        val page = jsonObject.getJSONObject("page");
        val ts = jsonObject.getLong("ts");

        import scala.collection.JavaConversions._
        for(display <- displays) {
          val displayObj = new JSONObject()
          displayObj.put("common", common)
          displayObj.put("page", page)
          displayObj.put("ts", ts)
          displayObj.put("display", display)
          context.output(displayTag, displayObj)
        }

/*        val iterator: util.Iterator[AnyRef] = displays.iterator()
        while (iterator.hasNext) {
          val displayObj = new JSONObject()
          displayObj.put("common", common)
          displayObj.put("page", page)
          displayObj.put("ts", ts)
          displayObj.put("display", iterator.next())
          context.output(displayTag, displayObj)
        }*/
      }
    }
  }
}

