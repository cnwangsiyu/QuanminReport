package app

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by WangSiyu on 07/04/2017.
  */
object WeeklyReport {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("weekly-report").setMaster("local[8]")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)

    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val cal = Calendar.getInstance
    val pathArray = new Array[String](7)

    for (i <- 0 until 7) {
      cal.add(Calendar.DATE, -1)
      val yesterday = dateFormat.format(cal.getTime)
      pathArray(i) = "/Users/WangSiyu/Desktop/quanmin/export_%s-*".format(yesterday)
    }
    sqlContext.read.parquet(pathArray: _*).registerTempTable("quanmin_this_week")

    for (i <- 0 until 7) {
      cal.add(Calendar.DATE, -1)
      val yesterday = dateFormat.format(cal.getTime)
      pathArray(i) = "/Users/WangSiyu/Desktop/quanmin/export_%s-*".format(yesterday)
    }
    sqlContext.read.parquet(pathArray: _*).registerTempTable("quanmin_last_week")

    var attachmentStringsToSend = scala.collection.mutable.Map[String, String]()
    var tmpString = ""

    def getDeviceName(id: Long): String = {
      id match {
        case 1 => "Android"
        case 2 => "iOS"
        case 5 => "PC"
        case 9 => "H5"
        case 14 => "PC新秀"
        case _ => "其他"
      }
    }

  }
}
