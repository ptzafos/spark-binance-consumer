package main.pandora.manager

import scala.util.parsing.json._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object ConsumerManager {
  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local").setAppName("BinanceConsumers")
    val ssc = new StreamingContext(conf, Seconds(1))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "192.168.1.5:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "pandora",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("pandora.ostbtc","pandora.linkbtc","pandora.elfbtc","pandora.ethbtc","pandora.xlmbtc","ethbtc")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    stream.map(record => (record.topic,JSON.parseFull(record.value.toString))).print
//    stream.foreachRDD(record => println(record.value.toString))
    ssc.start()
    ssc.awaitTermination()
  }
}
