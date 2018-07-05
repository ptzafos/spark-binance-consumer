package pandora.repository

import java.text.{DecimalFormat, SimpleDateFormat}
import java.util.{Date, TimeZone}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{Cell, CellUtil, HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.PrefixFilter
import org.apache.hadoop.hbase.util.Bytes

import collection.JavaConversions._





class PandoraRepository extends Serializable() {

  val df: DecimalFormat = new DecimalFormat("#");
  val conf: Configuration = HBaseConfiguration.create()
  val connection = ConnectionFactory.createConnection(conf)
  val table = connection.getTable(TableName.valueOf(Bytes.toBytes("default:pandora")))

  def persist(record: Map[String, String]): Unit = {
    record("e") match {
      case "kline" => {
        val kMap = record("k").asInstanceOf[Map[String, String]]
        val rowKey = record("s") + convertToDate(kMap("t").asInstanceOf[Double])
        val put = new Put(Bytes.toBytes(rowKey))
        put.addColumn(Bytes.toBytes("kline_30m"), Bytes.toBytes("kline_start"), Bytes.toBytes(df.format(kMap("t"))))
        put.addColumn(Bytes.toBytes("kline_30m"), Bytes.toBytes("kline_close"), Bytes.toBytes(df.format(kMap("T"))))
        put.addColumn(Bytes.toBytes("kline_30m"), Bytes.toBytes("open"), Bytes.toBytes(kMap("o")))
        put.addColumn(Bytes.toBytes("kline_30m"), Bytes.toBytes("close"), Bytes.toBytes(kMap("c")))
        put.addColumn(Bytes.toBytes("kline_30m"), Bytes.toBytes("high"), Bytes.toBytes(kMap("h")))
        put.addColumn(Bytes.toBytes("kline_30m"), Bytes.toBytes("low"), Bytes.toBytes(kMap("l")))
        put.addColumn(Bytes.toBytes("kline_30m"), Bytes.toBytes("volume"), Bytes.toBytes(kMap("v")))
        put.addColumn(Bytes.toBytes("kline_30m"), Bytes.toBytes("kline_level"), Bytes.toBytes(kMap("i")))
        put.addColumn(Bytes.toBytes("kline_30m"), Bytes.toBytes("btc_volume"), Bytes.toBytes(kMap("q")))
        scanEmaHistory(record("s"))
        if(kMap("x").asInstanceOf[Boolean]){
          updateHistory(rowKey, record("s"))
        }
        table.put(put)
      }
    }
  }

  def updateHistory(rowKey: String, tokenPrefix: String): Unit = {
    val scan = new Scan()
    scan.addColumn(Bytes.toBytes("kline_30m"), Bytes.toBytes("close"))
    val filter = new PrefixFilter(Bytes.toBytes(tokenPrefix));
    scan.setFilter(filter)
    scan.setReversed(true)
    scan.setLimit(26)
    val results = table.getScanner(scan)
    var period = 0
    for(result: Result <- results){
      val cells = result.listCells()
      for (c: Cell <- cells) {
        val put = new Put(Bytes.toBytes(rowKey))
        if(period < 10) {
          put.addColumn(Bytes.toBytes("history"), Bytes.toBytes("closed-0" + period), CellUtil.cloneValue(c))
        } else {
          put.addColumn(Bytes.toBytes("history"), Bytes.toBytes("closed-" + period), CellUtil.cloneValue(c))
        }
        table.put(put)
      }
      period += 1
    }
  }


  def scanEmaHistory(tokenPrefix: String): Unit = synchronized {
    val scan = new Scan()
    scan.addColumn(Bytes.toBytes("kline_30m"), Bytes.toBytes("close"))
    val filter = new PrefixFilter(Bytes.toBytes(tokenPrefix))
    scan.setFilter(filter)
    scan.setLimit(26)
    scan.setReversed(true)
    val results = table.getScanner(scan)
    for(result: Result <- results){
        val cells = result.listCells()
        for (c: Cell <- cells) {
          val columnFamily = CellUtil.cloneFamily(c)
          val qualifier = CellUtil.cloneQualifier(c)
          val value = CellUtil.cloneValue(c)
          val rowKey = CellUtil.cloneRow(c)
          System.out.println("rowKey:" + Bytes.toString(rowKey) + ", columnFamily:" + Bytes.toString(columnFamily) + ", qualifier:" + Bytes.toString(qualifier) + ", value:" + Bytes.toFloat(value))
        }
    }
  }

  def calculateMACD(cells: List[Cell]): Unit = {


  }

  def convertToDate(timestamp: Double): String = {
    val dateFormat = new SimpleDateFormat("yyyy-M-d;h:m:s;zzz")
    dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"))
    val date = new Date(timestamp.toLong)

    dateFormat.format(date)
  }

  def release_resources(): Unit = {
    connection.close()
    table.close()
  }


}
