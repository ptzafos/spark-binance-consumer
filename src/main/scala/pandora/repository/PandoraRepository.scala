package pandora.repository

import java.text.{DecimalFormat, SimpleDateFormat}
import java.util.{Date, TimeZone}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes


class PandoraRepository extends Serializable() {

  val df: DecimalFormat = new DecimalFormat("#");
  val conf: Configuration = HBaseConfiguration.create()
  val connection = ConnectionFactory.createConnection(conf)

  def persist(record: Map[String, String]): Unit = {
    record("e") match {
      case "kline" => {
        val table = connection.getTable(TableName.valueOf(Bytes.toBytes("default:pandora")))
        val kMap = record("k").asInstanceOf[Map[String, String]]
        val put = new Put(Bytes.toBytes(record("s") + convertToDate(kMap("t").asInstanceOf[Double])))
        put.addColumn(Bytes.toBytes("kline_30m"), Bytes.toBytes("kline_start"), Bytes.toBytes(df.format(kMap("t"))))
        put.addColumn(Bytes.toBytes("kline_30m"), Bytes.toBytes("kline_close"), Bytes.toBytes(df.format(kMap("T"))))
        put.addColumn(Bytes.toBytes("kline_30m"), Bytes.toBytes("open"), Bytes.toBytes(kMap("o")))
        put.addColumn(Bytes.toBytes("kline_30m"), Bytes.toBytes("close"), Bytes.toBytes(kMap("c")))
        put.addColumn(Bytes.toBytes("kline_30m"), Bytes.toBytes("high"), Bytes.toBytes(kMap("h")))
        put.addColumn(Bytes.toBytes("kline_30m"), Bytes.toBytes("low"), Bytes.toBytes(kMap("l")))
        put.addColumn(Bytes.toBytes("kline_30m"), Bytes.toBytes("volume"), Bytes.toBytes(kMap("v")))
        put.addColumn(Bytes.toBytes("kline_30m"), Bytes.toBytes("kline_level"), Bytes.toBytes(kMap("i")))
        put.addColumn(Bytes.toBytes("kline_30m"), Bytes.toBytes("btc_volume"), Bytes.toBytes(kMap("q")))
        table.put(put)
        table.close()
      }
    }
  }

  def convertToDate(timestamp: Double): String = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd;hh:mm:ss;zzz");
    dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"))
    val date = new Date(timestamp.toLong)

    dateFormat.format(date)
  }

  def release_resources(): Unit = {
    connection.close()
  }


}
