package pandora.consumers

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import pandora.repository.PandoraRepository

import scala.util.parsing.json._



class CoreConsumer(ssc: StreamingContext) extends Serializable{

  private var topics: Array[String] = _
  private var dstream: InputDStream[ConsumerRecord[String, String]] = _
  private var kafkaParams: Map[String, Object] = _

  generateParams()
  generateTopics()
  generateStream()

  def generateParams(): Unit ={
    kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "192.168.1.41:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "pandora-analytics",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (true: java.lang.Boolean)
    )
  }

  def generateTopics(): Unit ={
//    topics = Array("pandora.ostbtc","pandora.linkbtc","pandora.elfbtc","pandora.ethbtc","pandora.xlmbtc","ethbtc")
        topics = Array("pandora.ethbtc")
  }

  def generateStream(): Unit = {
    dstream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
  }

  def startStreamingProcess(): Unit = {
    dstream.foreachRDD(rdd => {
      rdd.foreach(record => {
        val repo = new PandoraRepository
        repo.persist(JSON.parseFull(record.value()).get.asInstanceOf[Map[String,String]])
        repo.releaseResources()
      })
    })
  }
}
