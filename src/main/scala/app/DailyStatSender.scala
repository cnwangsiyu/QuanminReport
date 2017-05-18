package app

import java.text.SimpleDateFormat
import java.util

import com.qiniu.pandora.pipeline.sender.DataSender
import com.qiniu.pandora.util.Auth
import com.qiniu.pandora.pipeline.points.Point
import java.util.Calendar

import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by WangSiyu on 04/05/2017.
  */
object DailyStatSender {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().
      setAppName("daily-report").
      setMaster("local[8]")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)

    sc.hadoopConfiguration.set("fs.qiniu.bucket.domain", "http://oihu9i4fk.bkt.clouddn.com")

    val tmp = Calendar.getInstance
    tmp.add(Calendar.DATE, -1)
    val cal = Calendar.getInstance
    cal.clear()
    cal.set(tmp.get(Calendar.YEAR), tmp.get(Calendar.MONTH), tmp.get(Calendar.DATE))
    val yesterday = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)

    val dataPath = "qiniu://quanmin2/export_%s-*".format(yesterday)
    val quanmin = sqlContext.read.parquet(dataPath)
    quanmin.printSchema()
    quanmin.registerTempTable("quanmin_raw")

    sqlContext.udf.register("contains", (s1: String, s2: String) => {
      s1.contains(s2)
    })

    sqlContext.sql(
      """
        |SELECT CASE
        |    WHEN v1='bd' OR v1='baidu' THEN '百度'
        |    WHEN v1='qn' THEN '七牛'
        |    WHEN v1='tx' THEN '腾讯'
        |    WHEN v1='al' OR v1='ali' THEN '阿里'
        |    WHEN v1='ws' THEN '网宿'
        |    WHEN v1='yf' THEN '云帆'
        |    WHEN v1='js' THEN '金山'
        |    ELSE '未定义' END AS cdn,
        |CASE
        |    WHEN platform=14 THEN 5
        |    ELSE platform END AS platform1,
        |CASE
        |    WHEN contains(isp, "鹏博士") then "鹏博士"
        |    WHEN contains(isp, "教育网") then "教育网"
        |    WHEN contains(isp, "铁通") then "铁通"
        |    WHEN contains(isp, "电信") then "电信"
        |    WHEN contains(isp, "移动") then "移动"
        |    WHEN contains(isp, "联通") then "联通"
        |    ELSE "其他" END AS isp1, *
        |    FROM quanmin_raw WHERE tag='monitor' AND room_id!=-1 AND v5>1 AND country='中国'
      """.stripMargin).cache().registerTempTable("quanmin_lag")

    sqlContext.sql(
      """
        |SELECT CASE
        |    WHEN v1='bd' OR v1='baidu' THEN '百度'
        |    WHEN v1='qn' THEN '七牛'
        |    WHEN v1='tx' THEN '腾讯'
        |    WHEN v1='al' OR v1='ali' THEN '阿里'
        |    WHEN v1='ws' THEN '网宿'
        |    WHEN v1='yf' THEN '云帆'
        |    WHEN v1='js' THEN '金山'
        |    ELSE '未定义' END AS cdn,
        |CASE
        |    WHEN platform=14 THEN 5
        |    ELSE platform END AS platform1,
        |CASE
        |    WHEN contains(isp, "鹏博士") then "鹏博士"
        |    WHEN contains(isp, "教育网") then "教育网"
        |    WHEN contains(isp, "铁通") then "铁通"
        |    WHEN contains(isp, "电信") then "电信"
        |    WHEN contains(isp, "移动") then "移动"
        |    WHEN contains(isp, "联通") then "联通"
        |    ELSE "其他" END AS isp1, *
        |FROM quanmin_raw WHERE tag='first' AND room_id!=-1 AND v5<=30000 AND v5>0 AND country='中国'
      """.stripMargin).cache().registerTempTable("quanmin_first")

    sqlContext.sql(
      """
        |SELECT CASE
        |    WHEN v1='bd' OR v1='baidu' THEN '百度'
        |    WHEN v1='qn' THEN '七牛'
        |    WHEN v1='tx' THEN '腾讯'
        |    WHEN v1='al' OR v1='ali' THEN '阿里'
        |    WHEN v1='ws' THEN '网宿'
        |    WHEN v1='yf' THEN '云帆'
        |    WHEN v1='js' THEN '金山'
        |    ELSE '未定义' END AS cdn,
        |CASE
        |    WHEN platform=14 THEN 5
        |    ELSE platform END AS platform1,
        |CASE
        |    WHEN contains(isp, "鹏博士") then "鹏博士"
        |    WHEN contains(isp, "教育网") then "教育网"
        |    WHEN contains(isp, "铁通") then "铁通"
        |    WHEN contains(isp, "电信") then "电信"
        |    WHEN contains(isp, "移动") then "移动"
        |    WHEN contains(isp, "联通") then "联通"
        |    ELSE "其他" END AS isp1, *
        |FROM quanmin_raw WHERE tag='connect' AND room_id!=-1 AND v5<=30000 AND v5>0 AND country='中国'
      """.stripMargin).cache().registerTempTable("quanmin_connect")

    val auth = Auth.create("YFvDcv7ie2tmSCRjX8aYHwrfqpeXR4M_ef2Az1CK", "MCBFkF6tv55uxavHTnxKEFt8f7uKL5rD0Lv2gL5n")

    val lagRepoName = "quanmin_report_lag"
    val lagSender = new DataSender(lagRepoName, auth)
    val lagPoints = new util.ArrayList[Point]

    sqlContext.sql(
      """
        |select * from
        |    (select cdn, platform, province, isp1, sum(v4) as total_lag, count(*) as total_point, v2 as cdn_ip from quanmin_lag group by cdn, platform, province, isp1, v2) t1
        |where total_point>300
      """.stripMargin).
      collect().foreach((row: Row) => {
      val p = new Point
      p.append("time", new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX").format(cal.getTime))
      p.append("cdn", row.getString(0))
      p.append("platform", Long.box(row.getLong(1)))
      p.append("province", row.getString(2))
      p.append("isp", row.getString(3))
      p.append("total_lag", Long.box(row.getLong(4)))
      p.append("total_point", Long.box(row.getLong(5)))
      p.append("cdn_ip", row.getString(6))
      lagPoints.add(p)
    })
    var err = lagSender.send(lagPoints)
    if (err.getExceptions.size > 0) {
      println("error.getExceptions() = ", err.getExceptions)
    }
    lagSender.close()

    val firstRepoName = "quanmin_report_first"
    val firstSender = new DataSender(firstRepoName, auth)
    val firstPoints = new util.ArrayList[Point]

    sqlContext.sql(
      """
        |select * from
        |    (select cdn, platform, province, isp1, avg(v5) as first_avg, v2 as cdn_ip, count(*) as total_count from quanmin_first group by cdn, platform, province, isp1, v2) t1
        |where total_count>50
      """.stripMargin).
      collect().foreach((row: Row) => {
      val p = new Point
      p.append("time", new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX").format(cal.getTime))
      p.append("cdn", row.getString(0))
      p.append("platform", Long.box(row.getLong(1)))
      p.append("province", row.getString(2))
      p.append("isp", row.getString(3))
      p.append("first_avg", Double.box(row.getDouble(4)))
      p.append("cdn_ip", row.getString(5))
      firstPoints.add(p)
    })
    err = firstSender.send(firstPoints)
    if (err.getExceptions.size > 0) {
      println("error.getExceptions() = ", err.getExceptions)
    }
    firstSender.close()

    val connectRepoName = "quanmin_report_connect"
    val connectSender = new DataSender(firstRepoName, auth)
    val connectPoints = new util.ArrayList[Point]

    sqlContext.sql(
      """
        |select * from
        |    (select cdn, platform, province, isp1, avg(v5) as connect_avg, v2 as cdn_ip, count(*) as total_count from quanmin_connect group by cdn, platform, province, isp1, v2) t1
        |where total_count>50
      """.stripMargin).
      collect().foreach((row: Row) => {
      val p = new Point
      p.append("time", new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX").format(cal.getTime))
      p.append("cdn", row.getString(0))
      p.append("platform", Long.box(row.getLong(1)))
      p.append("province", row.getString(2))
      p.append("isp", row.getString(3))
      p.append("connect_avg", Double.box(row.getDouble(4)))
      p.append("cdn_ip", row.getString(5))
      connectPoints.add(p)
    })
    err = connectSender.send(connectPoints)
    if (err.getExceptions.size > 0) {
      println("error.getExceptions() = ", err.getExceptions)
    }
    connectSender.close()
  }
}
