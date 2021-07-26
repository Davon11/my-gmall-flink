package com.davon.app.util

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer, KafkaSerializationSchema}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig

import java.util.Properties

object MyKafkaUtil {
  val DEFAULT_TOPIC = "dwd_default_topic"
  val properties = new Properties
  properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092,hadoop103:9092,hadoop104:9092")

  def getFlinkKafkaProducerBySchema[T](kafkaSerializationSchema: KafkaSerializationSchema[T]) = new FlinkKafkaProducer[T](DEFAULT_TOPIC, kafkaSerializationSchema, properties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE)

  def getFlinkKafkaProducer(topic: String) = {
    new FlinkKafkaProducer[String](topic, new SimpleStringSchema(), properties)
  }

  def getFlinkKafkaConsumer(groupId: String, topic: String) = {
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), properties)
  }

}
