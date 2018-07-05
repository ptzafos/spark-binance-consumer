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
  val dfPerc: DecimalFormat = new DecimalFormat("0.00000");
  val conf: Configuration = HBaseConfiguration.create()
  val connection = ConnectionFactory.createConnection(conf)
  val table = connection.getTable(TableName.valueOf(Bytes.toBytes("default:pandora")))

  def persist(record: Map[String, String]): Unit = {
    record("e") match {
      case "kline" => {
        val kMap = record("k").asInstanceOf[Map[String, String]]
        val rowKey = record("s") + convertToDate(kMap("t").asInstanceOf[Double])
        val put = new Put(Bytes.toBytes(rowKey))
        put.addColumn(Bytes.toBytes("kline_30m"), Bytes.toBytes("start_time"), Bytes.toBytes(df.format(kMap("t"))))
        put.addColumn(Bytes.toBytes("kline_30m"), Bytes.toBytes("end_time"), Bytes.toBytes(df.format(kMap("T"))))
        put.addColumn(Bytes.toBytes("kline_30m"), Bytes.toBytes("open"), Bytes.toBytes(kMap("o")))
        put.addColumn(Bytes.toBytes("kline_30m"), Bytes.toBytes("close"), Bytes.toBytes(kMap("c")))
        put.addColumn(Bytes.toBytes("kline_30m"), Bytes.toBytes("high"), Bytes.toBytes(kMap("h")))
        put.addColumn(Bytes.toBytes("kline_30m"), Bytes.toBytes("low"), Bytes.toBytes(kMap("l")))
        put.addColumn(Bytes.toBytes("kline_30m"), Bytes.toBytes("volume"), Bytes.toBytes(kMap("v")))
        put.addColumn(Bytes.toBytes("kline_30m"), Bytes.toBytes("kline_level"), Bytes.toBytes(kMap("i")))
        put.addColumn(Bytes.toBytes("kline_30m"), Bytes.toBytes("btc_volume"), Bytes.toBytes(kMap("q")))
        put.addColumn(Bytes.toBytes("kline_30m"), Bytes.toBytes("closed"), Bytes.toBytes(String.valueOf(kMap("x"))))
        put.addColumn(Bytes.toBytes("history"), Bytes.toBytes("closed-00"), Bytes.toBytes(kMap("c")))
        put.addColumn(Bytes.toBytes("history"), Bytes.toBytes("open-00"), Bytes.toBytes(kMap("o")))
        val gain = calculateGain(kMap("o").toFloat, kMap("c").toFloat)
        put.addColumn(Bytes.toBytes("history"), Bytes.toBytes("gain-00"), Bytes.toBytes(dfPerc.format(gain)))
        if (isPreviousClosed(kMap("s"))) {
          updateHistory(rowKey, record("s"))
        }
        table.put(put)
      }
    }
  }

  def isPreviousClosed(tokenPrefix: String): Boolean = {
    val scan = new Scan()
    val filter = new PrefixFilter(Bytes.toBytes(tokenPrefix))
    scan.addColumn(Bytes.toBytes("kline_30m"), Bytes.toBytes("closed"))
    scan.setFilter(filter)
    scan.setLimit(1)
    scan.setReversed(true)
    val results = table.getScanner(scan)
    val result = Option(results.next())
    if (result.isEmpty) {
      return false
    }
    val cell = result.get.listCells().get(0)
    if (Bytes.toString(CellUtil.cloneValue(cell)).equals("true")) {
      return true
    } else {
      return false
    }
  }

  def calculateEMAs(tokenPrefix: String): Unit = {
    val scan = new Scan()
    scan.addColumn(Bytes.toBytes("kline_30m"), Bytes.toBytes("close"))

  }


  def updateHistory(rowKey: String, tokenPrefix: String): Unit = {
    val scan = new Scan()
    scan.addColumn(Bytes.toBytes("kline_30m"), Bytes.toBytes("close"))
    scan.addColumn(Bytes.toBytes("kline_30m"), Bytes.toBytes("open"))
    val filter = new PrefixFilter(Bytes.toBytes(tokenPrefix));
    scan.setFilter(filter)
    scan.setReversed(true)
    scan.setLimit(25)
    val results = table.getScanner(scan)
    val resultList = results.toList
    var period = 1
    for (result: Result <- resultList) {
      val cells = result.listCells()
      var open: Float = 0
      var close: Float = 0
      var gainString: String = null
      for (c: Cell <- cells) {
        val put = new Put(Bytes.toBytes(rowKey))
        var closedString: String = null
        var openString: String = null
        val qualifier = Bytes.toString(CellUtil.cloneQualifier(c))
        if (period < 10) {
          closedString = "closed-0"
          openString = "open-0"
          gainString = "gain-0"
        } else {
          closedString = "closed-"
          openString = "open-"
          gainString = "gain-"
        }
        qualifier match {
          case "open" => {
            put.addColumn(Bytes.toBytes("history"), Bytes.toBytes(openString + period), CellUtil.cloneValue(c))
            open = Bytes.toFloat(CellUtil.cloneValue(c))
            table.put(put)
          }
          case "close" => {
            put.addColumn(Bytes.toBytes("history"), Bytes.toBytes(closedString + period), CellUtil.cloneValue(c))
            close = Bytes.toFloat(CellUtil.cloneValue(c))
            table.put(put)
          }
        }
      }
      val putGain = new Put(Bytes.toBytes(rowKey))
      val gain = calculateGain(open, close)
      putGain.addColumn(Bytes.toBytes("history"), Bytes.toBytes(gainString + period), Bytes.toBytes(dfPerc.format(gain)))
      table.put(putGain)
      period += 1
    }
  }

  def calculateMACD(cells: List[Cell]): Unit = {


  }

  def calculateGain(open: Float, close: Float): Float = {
    if (open > close) {
      return -(open - close) / open
    } else {
      return (close - open) / close
    }
  }


  def convertToDate(timestamp: Double): String = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd;hh:mm:ss;zzz")
    dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"))
    val date = new Date(timestamp.toLong)

    dateFormat.format(date)
  }

  def release_resources(): Unit = {
    connection.close()
    table.close()
  }


}
