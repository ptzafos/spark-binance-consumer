package pandora.consumers

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import scala.util.parsing.json._

class MainConsumer(ssc: StreamingContext) {

  def streamingContext() = ssc
  private var topics: Array[String] = _
  private var stream: InputDStream[ConsumerRecord[String, String]] = _
  private var kafkaParams: Map[String, Object] = _

  generateParams()
  generateTopics()
  generateStream()
  println("initialized...")

  def generateParams(): Unit ={

       kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "192.168.1.5:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "pandora-analytics",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
  }

  def generateTopics(): Unit ={
      topics = Array("pandora.ostbtc","pandora.linkbtc","pandora.elfbtc","pandora.ethbtc","pandora.xlmbtc","ethbtc")
  }

  def generateStream(): Unit = {
      stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
  }

  def startStreamingProcess(): Unit = {
    stream.map(record => (record.topic(), record.value())).print()
  }

  //    stream.map(record => (record.topic,JSON.parseFull(record.value.toString))).print
  //    stream.foreachRDD(record => println(record.value.toString))
}
