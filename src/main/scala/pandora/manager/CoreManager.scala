package pandora.manager

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}
import pandora.consumers.CoreConsumer

object CoreManager {
  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local[2]").setAppName("BinanceConsumers")
    val ssc = new StreamingContext(conf, Milliseconds(200))

    val coreConsumer = new CoreConsumer(ssc)
    coreConsumer.startStreamingProcess()

    ssc.start()
    ssc.awaitTermination()
  }
}
