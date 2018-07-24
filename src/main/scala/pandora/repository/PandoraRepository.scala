package pandora.repository

import java.text.{DecimalFormat, SimpleDateFormat}
import java.util.{Date, TimeZone}

import org.apache.commons.lang.time.DateUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{Cell, CellUtil, HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.{ColumnPrefixFilter, PrefixFilter}
import org.apache.hadoop.hbase.util.Bytes

import scala.util.control.Breaks._
import collection.JavaConversions._


class PandoraRepository extends Serializable() {

  val dfTimestamp: DecimalFormat = new DecimalFormat("#");
  val dfRate: DecimalFormat = new DecimalFormat("0.0000000");
  val df: DecimalFormat = new DecimalFormat("#.########");
  val conf: Configuration = HBaseConfiguration.create()
  val connection = ConnectionFactory.createConnection(conf)
  val table = connection.getTable(TableName.valueOf(Bytes.toBytes("default:pandora")))
  val kline: Integer = 1 //kline in minutes

  def persist(record: Map[String, String]): Unit = {
    record("e") match {
      case "kline" => {
        val kMap = record("k").asInstanceOf[Map[String, String]]
        val rowKey = record("s") + convertToDate(kMap("t").asInstanceOf[Double])
        val put = new Put(Bytes.toBytes(rowKey))
        put.addColumn(Bytes.toBytes("kline_30m"), Bytes.toBytes("start_time"), Bytes.toBytes(dfTimestamp.format(kMap("t"))))
        put.addColumn(Bytes.toBytes("kline_30m"), Bytes.toBytes("end_time"), Bytes.toBytes(dfTimestamp.format(kMap("T"))))
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
        put.addColumn(Bytes.toBytes("history"), Bytes.toBytes("volume-00"), Bytes.toBytes(kMap("v")))
        val gain = calculateGain(kMap("o").toFloat, kMap("c").toFloat)
        put.addColumn(Bytes.toBytes("history"), Bytes.toBytes("gain-00"), Bytes.toBytes(dfRate.format(gain)))
        if (isPreviousClosed(rowKey)) {
          updateHistory(rowKey)
        }
        table.put(put)
        updateEMAs(rowKey, kMap("c").toFloat)
        updateRSI(rowKey)
      }
    }
  }

  def isPreviousClosed(rowKey: String): Boolean = {
    val get = new Get(Bytes.toBytes(getPreviousRowKey(rowKey, kline)))
    get.addColumn(Bytes.toBytes("kline_30m"), Bytes.toBytes("closed"))
    val result = table.get(get)
    if (result.isEmpty) {
      return false
    }
    val cell = result.listCells().get(0)
    if (Bytes.toString(CellUtil.cloneValue(cell)).equals("true")) {
      return true
    } else {
      return false
    }
  }

  def updateSMAs(rowKey: String, resultList: List[Cell], periods: Int): Unit = {
    var ema: Float = 0
    val put = new Put(Bytes.toBytes(rowKey))
    for (c: Cell <- resultList) {
      ema += Bytes.toString(CellUtil.cloneValue(c)).toFloat
    }
    if (periods == 12) {
      put.addColumn(Bytes.toBytes("indexes"), Bytes.toBytes("ema12"), Bytes.toBytes(df.format(ema / periods)))
      table.put(put)
    }
    if (periods == 26) {
      put.addColumn(Bytes.toBytes("indexes"), Bytes.toBytes("ema26"), Bytes.toBytes(df.format(ema / periods)))
      table.put(put)
    }
  }

  def previousColumnValue(rowKey: String, columnFamily: String, column: String): Option[String] = {
    val get = new Get(Bytes.toBytes(getPreviousRowKey(rowKey, kline)))
    get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column))
    val result = table.get(get)
    if (result.isEmpty) {
      return None
    }
    val cell = result.listCells().get(0)
    return Option(Bytes.toString(CellUtil.cloneValue(cell)))
  }

  def updateEMAs(rowKey: String, closePrice: Float): Unit = {
    val get = new Get(Bytes.toBytes(rowKey))
    val columnFilter = new ColumnPrefixFilter(Bytes.toBytes("closed-"))
    get.addFamily(Bytes.toBytes("history"))
    get.setFilter(columnFilter)
    val result = table.get(get)
    val cells = Option(result.listCells())
    if (cells.isEmpty) {
      return
    }
    val resultList = cells.get.toList
    val ema12Multiplier: Float = 2f / (12f + 1f)
    val ema26Multiplier: Float = 2f / (26f + 1f)
    if (resultList.size < 12) {
      return
    }
    if (resultList.size == 12 && previousColumnValue(rowKey, "indexes", "ema12").isEmpty) {
      updateSMAs(rowKey, resultList, 12)
      return
    }
    if (resultList.size < 26) {
      val preEma12: Float = previousColumnValue(rowKey, "indexes", "ema12").get.toFloat
      val put = new Put(Bytes.toBytes(rowKey))
      val newEma12: Float = (closePrice - preEma12.toFloat) * ema12Multiplier + preEma12
      put.addColumn(Bytes.toBytes("indexes"), Bytes.toBytes("ema12"), Bytes.toBytes(df.format(newEma12)))
      table.put(put)
      return
    }
    if (resultList.size == 26 && previousColumnValue(rowKey, "indexes", "ema26").isEmpty) {
      updateSMAs(rowKey, resultList, 26)
      return
    }
    val preEma12: Float = previousColumnValue(rowKey, "indexes", "ema12").get.toFloat
    val preEma26: Float = previousColumnValue(rowKey, "indexes", "ema26").get.toFloat
    val put = new Put(Bytes.toBytes(rowKey))
    val newEma12: Float = (closePrice - preEma12) * ema12Multiplier + preEma12
    val newEma26: Float = (closePrice - preEma26) * ema26Multiplier + preEma26
    val macdLine = newEma12 - newEma26
    put.addColumn(Bytes.toBytes("indexes"), Bytes.toBytes("ema12"), Bytes.toBytes(df.format(newEma12)))
    put.addColumn(Bytes.toBytes("indexes"), Bytes.toBytes("ema26"), Bytes.toBytes(df.format(newEma26)))
    table.put(put)
    updateMACD(rowKey, macdLine)
  }


  def updateHistory(rowKey: String): Unit = {
    val scan = new Scan()
    scan.addColumn(Bytes.toBytes("kline_30m"), Bytes.toBytes("close"))
    scan.addColumn(Bytes.toBytes("kline_30m"), Bytes.toBytes("open"))
    scan.addColumn(Bytes.toBytes("kline_30m"), Bytes.toBytes("volume"))
    val filter = new PrefixFilter(Bytes.toBytes(rowKey.split(";")(0)))
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
        var volumeString: String = null
        val qualifier = Bytes.toString(CellUtil.cloneQualifier(c))
        if (period < 10) {
          closedString = "closed-0"
          openString = "open-0"
          gainString = "gain-0"
          volumeString = "volume-0"
        } else {
          closedString = "closed-"
          openString = "open-"
          gainString = "gain-"
          volumeString = "volume-"
        }
        qualifier match {
          case "open" => {
            put.addColumn(Bytes.toBytes("history"), Bytes.toBytes(openString + period), CellUtil.cloneValue(c))
            open = Bytes.toString(CellUtil.cloneValue(c)).toFloat
            table.put(put)
          }
          case "close" => {
            put.addColumn(Bytes.toBytes("history"), Bytes.toBytes(closedString + period), CellUtil.cloneValue(c))
            close = Bytes.toString(CellUtil.cloneValue(c)).toFloat
            table.put(put)
          }
          case "volume" => {
            put.addColumn(Bytes.toBytes("history"), Bytes.toBytes(volumeString + period), CellUtil.cloneValue(c))
            table.put(put)
          }
        }
      }
      val putGain = new Put(Bytes.toBytes(rowKey))
      val gain = calculateGain(open, close)
      putGain.addColumn(Bytes.toBytes("history"), Bytes.toBytes(gainString + period), Bytes.toBytes(dfRate.format(gain)))
      table.put(putGain)
      period += 1
    }
  }

  def updateRSI(rowKey: String): Unit = {
    val get = new Get(Bytes.toBytes(rowKey))
    val columnFilter = new ColumnPrefixFilter(Bytes.toBytes("gain-"))
    get.addFamily(Bytes.toBytes("history"))
    get.setFilter(columnFilter)
    val result = table.get(get)
    var cells = Option(result.listCells())
    if (cells.isEmpty) {
      return
    }
    var avgGain: Float = 0
    var avgLoss: Float = 0
    var cellCounter: Integer = 0
    breakable {
      for (c: Cell <- cells.get) {
        if (cellCounter == 14) {
          val rs: Float = (avgGain / 14f) / (-avgLoss / 14f)
          val rsi: Float = 100f - (100f / (1 + rs))
          val put = new Put(Bytes.toBytes(rowKey))
          put.addColumn(Bytes.toBytes("indexes"), Bytes.toBytes("rsi"), Bytes.toBytes(df.format(rsi)))
          table.put(put)
          break
        }
        val gain = Bytes.toString(CellUtil.cloneValue(c)).toFloat
        if (gain > 0) {
          avgGain += gain
        } else {
          avgLoss += gain
        }
        cellCounter += 1
      }
    }
  }

  def calculateGain(open: Float, close: Float): Float = {
    if (open > close) {
      return -((open - close) / open)
    } else {
      return (close - open) / close
    }
  }

  def convertToDate(timestamp: Double): String = {
    val dateFormat = new SimpleDateFormat(";yyyy-MM-dd hh:mm:ss zzz")
    dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"))
    val date = new Date(timestamp.toLong)

    dateFormat.format(date)
  }

  def getPreviousRowKey(rowKey: String, minutes: Integer): String = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss zzz")
    val splitted = rowKey.split(";")
    val date: Date = DateUtils.addMinutes(dateFormat.parse(splitted(1)), -minutes)

    splitted(0) + ";" + dateFormat.format(date)
  }

  def releaseResources(): Unit = {
    table.close()
    connection.close()
  }

  def updateFirstSignalLine(rowKey:String , resultList: List[Result], macdLine:Float): Unit = {
    var macd_line_sum: Float = 0f
    for (result: Result <- resultList) {
      val cell = result.listCells().get(0)
      macd_line_sum += Bytes.toString(CellUtil.cloneValue(cell)).toFloat
    }
    val put = new Put(Bytes.toBytes(rowKey))
    val macd: Float = macdLine - (macd_line_sum/9f)
    put.addColumn(Bytes.toBytes("indexes"), Bytes.toBytes("macd"), Bytes.toBytes(df.format(macd)))
    put.addColumn(Bytes.toBytes("indexes"), Bytes.toBytes("signal_line"), Bytes.toBytes(df.format(macd_line_sum/9f)))
    table.put(put)
  }

  def updateMACD(rowKey: String, macdLine: Float): Unit = {
    val putMacdLine = new Put(Bytes.toBytes(rowKey))
    putMacdLine.addColumn(Bytes.toBytes("indexes"), Bytes.toBytes("macd_line"), Bytes.toBytes(df.format(macdLine)))
    table.put(putMacdLine)

    val scan = new Scan()
    scan.addColumn(Bytes.toBytes("indexes"), Bytes.toBytes("macd_line"))
    val filter = new PrefixFilter(Bytes.toBytes(rowKey.split(";")(0)))
    scan.setFilter(filter)
    scan.setReversed(true)
    scan.setLimit(9)
    val results = table.getScanner(scan)
    val resultList = results.toList
    if(resultList.size < 9){
      return
    }
    if (resultList.size == 9 && previousColumnValue(rowKey, "indexes", "macd").isEmpty) {
      updateFirstSignalLine(rowKey, resultList, macdLine)
      return
    }
    val prevEmaMacd: Float = previousColumnValue(rowKey, "indexes", "signal_line").get.toFloat
    val macdMultiplier: Float = 2f / (9f + 1f)
    val newEmaMacd: Float = (macdLine - prevEmaMacd) * macdMultiplier + prevEmaMacd
    val macd: Float = macdLine - newEmaMacd
    val put = new Put(Bytes.toBytes(rowKey))
    put.addColumn(Bytes.toBytes("indexes"), Bytes.toBytes("macd"), Bytes.toBytes(df.format(macd)))
    put.addColumn(Bytes.toBytes("indexes"), Bytes.toBytes("signal_line"), Bytes.toBytes(df.format(newEmaMacd)))
    table.put(put)
  }
}
