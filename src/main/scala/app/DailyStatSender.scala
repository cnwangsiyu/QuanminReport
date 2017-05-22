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
        |select cdn_ip from
        |    (select v2 as cdn_ip, count(*) as count from quanmin_raw group by v2) t
        |where count > 1000
      """.stripMargin).cache().registerTempTable("quanmin_valid_cdn_ip")
    sqlContext.sql(
      """
        |select * from
        |    (select case
        |        when v1='bd' or v1='baidu' then '百度'
        |        when v1='qn' then '七牛'
        |        when v1='tx' then '腾讯'
        |        when v1='al' or v1='ali' then '阿里'
        |        when v1='ws' then '网宿'
        |        when v1='yf' then '云帆'
        |        when v1='js' then '金山'
        |        else '未定义' end as cdn,
        |    case
        |        when platform=14 then 5
        |        else platform end as platform1,
        |    case
        |        when contains(isp, "鹏博士") then "鹏博士"
        |        when contains(isp, "教育网") then "教育网"
        |        when contains(isp, "铁通") then "铁通"
        |        when contains(isp, "电信") then "电信"
        |        when contains(isp, "移动") then "移动"
        |        when contains(isp, "联通") then "联通"
        |        else "其他" end as isp1, *
        |        from quanmin_raw where tag='monitor' and room_id!=-1 and v5>1 and country='中国') t1
        |inner join
        |    quanmin_valid_cdn_ip
        |on t1.v2=quanmin_valid_cdn_ip.cdn_ip
      """.stripMargin).registerTempTable("quanmin_lag")

    sqlContext.sql(
      """
        |select * from
        |    (select case
        |        when v1='bd' or v1='baidu' then '百度'
        |        when v1='qn' then '七牛'
        |        when v1='tx' then '腾讯'
        |        when v1='al' or v1='ali' then '阿里'
        |        when v1='ws' then '网宿'
        |        when v1='yf' then '云帆'
        |        when v1='js' then '金山'
        |        else '未定义' end as cdn,
        |    case
        |        when platform=14 then 5
        |        else platform end as platform1,
        |    case
        |        when contains(isp, "鹏博士") then "鹏博士"
        |        when contains(isp, "教育网") then "教育网"
        |        when contains(isp, "铁通") then "铁通"
        |        when contains(isp, "电信") then "电信"
        |        when contains(isp, "移动") then "移动"
        |        when contains(isp, "联通") then "联通"
        |        else "其他" end as isp1,
        |    case
        |        when v5 <= 1000 THEN 1
        |        when v5 > 1000 and v5 <= 3000 then 2
        |        else 3 end as first_range, *
        |    from quanmin_raw where tag='first' and room_id!=-1 and v5<=30000 and v5>0 and country='中国') t1
        |inner join
        |    quanmin_valid_cdn_ip
        |on t1.v2=quanmin_valid_cdn_ip.cdn_ip
      """.stripMargin).registerTempTable("quanmin_first")

    sqlContext.sql(
      """
        |select * from
        |    (select case
        |        when v1='bd' or v1='baidu' then '百度'
        |        when v1='qn' then '七牛'
        |        when v1='tx' then '腾讯'
        |        when v1='al' or v1='ali' then '阿里'
        |        when v1='ws' then '网宿'
        |        when v1='yf' then '云帆'
        |        when v1='js' then '金山'
        |        else '未定义' end as cdn,
        |    case
        |        when platform=14 then 5
        |        else platform end as platform1,
        |    case
        |        when contains(isp, "鹏博士") then "鹏博士"
        |        when contains(isp, "教育网") then "教育网"
        |        when contains(isp, "铁通") then "铁通"
        |        when contains(isp, "电信") then "电信"
        |        when contains(isp, "移动") then "移动"
        |        when contains(isp, "联通") then "联通"
        |        else "其他" end as isp1,
        |    case
        |        when v5 <= 1000 then 1
        |        when v5 > 1000 and v5 <= 3000 then 2
        |        else 3 end as connect_range, *
        |    from quanmin_raw where tag='connect' and room_id!=-1 and v5<=30000 and v5>0 and country='中国') t1
        |inner join
        |    quanmin_valid_cdn_ip
        |on t1.v2=quanmin_valid_cdn_ip.cdn_ip
      """.stripMargin).registerTempTable("quanmin_connect")

    val auth = Auth.create("YFvDcv7ie2tmSCRjX8aYHwrfqpeXR4M_ef2Az1CK", "MCBFkF6tv55uxavHTnxKEFt8f7uKL5rD0Lv2gL5n")

    val lagRepoName = "quanmin_report_lag"
    val lagSender = new DataSender(lagRepoName, auth)
    val lagPoints = new util.ArrayList[Point]



    sqlContext.sql(
      """
        |select cdn, platform, province, isp1, cdn_ip, sum(v4) as total_lag, count(*) as total_point from quanmin_lag group by cdn, platform, province, isp1, cdn_ip
      """.stripMargin).
      collect().foreach((row: Row) => {
      val p = new Point
      p.append("time", new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX").format(cal.getTime))
      p.append("cdn", row.getString(0))
      p.append("platform", Long.box(row.getLong(1)))
      p.append("province", row.getString(2))
      p.append("isp", row.getString(3))
      p.append("cdn_ip", row.getString(4))
      p.append("total_lag", Long.box(row.getLong(5)))
      p.append("total_point", Long.box(row.getLong(6)))
      lagPoints.add(p)
    })
    var err = lagSender.send(lagPoints)
    if (err.getExceptions.size > 0) {
      println("error.getExceptions() = ", err.getExceptions)
    }
    lagSender.close()

    val connectRepoName = "quanmin_report_connect"
    val connectSender = new DataSender(connectRepoName, auth)
    val connectPoints = new util.ArrayList[Point]

    sqlContext.sql(
      """
        |select cdn, platform, province, isp1, cdn_ip, avg(v5) as connect_avg, sum(v5) as connect_sum, count(*) as total_count from quanmin_connect group by cdn, platform, province, isp1, cdn_ip
      """.stripMargin).
      collect().foreach((row: Row) => {
      val p = new Point
      p.append("time", new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX").format(cal.getTime))
      p.append("cdn", row.getString(0))
      p.append("platform", Long.box(row.getLong(1)))
      p.append("province", row.getString(2))
      p.append("isp", row.getString(3))
      p.append("cdn_ip", row.getString(4))
      p.append("connect_avg", Double.box(row.getDouble(5)))
      p.append("connect_sum", Long.box(row.getLong(6)))
      p.append("total_point", Long.box(row.getLong(7)))
      connectPoints.add(p)
    })
    err = connectSender.send(connectPoints)
    if (err.getExceptions.size > 0) {
      println("error.getExceptions() = ", err.getExceptions)
    }
    connectSender.close()

    val firstRepoName = "quanmin_report_first"
    val firstSender = new DataSender(firstRepoName, auth)
    val firstPoints = new util.ArrayList[Point]

    sqlContext.sql(
      """
        |select cdn, platform, province, isp1, cdn_ip, first_range, avg(v5) as first_avg, sum(v5) as first_sum, count(*) as total_point from quanmin_first group by cdn, platform, province, isp1, cdn_ip, first_range
      """.stripMargin).
      collect().foreach((row: Row) => {
      val p = new Point
      p.append("time", new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX").format(cal.getTime))
      p.append("cdn", row.getString(0))
      p.append("platform", Long.box(row.getLong(1)))
      p.append("province", row.getString(2))
      p.append("isp", row.getString(3))
      p.append("cdn_ip", row.getString(4))
      p.append("first_range", Int.box(row.getInt(5)))
      p.append("first_avg", Double.box(row.getDouble(6)))
      p.append("first_sum", Long.box(row.getLong(7)))
      p.append("total_point", Long.box(row.getLong(8)))
      firstPoints.add(p)
    })
    err = firstSender.send(firstPoints)
    if (err.getExceptions.size > 0) {
      println("error.getExceptions() = ", err.getExceptions)
    }
    firstSender.close()
  }
}
