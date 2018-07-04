package pandora.manager

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import pandora.consumers.MainConsumer

object ConsumerManager {
  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local").setAppName("BinanceConsumers")
    val ssc = new StreamingContext(conf, Seconds(1))

    val coreConsumer = new MainConsumer(ssc)
    coreConsumer.startStreamingProcess()

    ssc.start()
    ssc.awaitTermination()
  }

//  def startMainConsuming(tickerConsumer: MainConsumer): Unit={
//    tickerConsumer.generateParams()
//    tickerConsumer.generateTopics()
//    tickerConsumer.generateStream()
//    tickerConsumer.startStreamingProcess()
//  }
}
