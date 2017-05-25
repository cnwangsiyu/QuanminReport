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

    val repoName = "quanmin_report"
    val sender = new DataSender(repoName, auth)
    val repoName_noip = "quanmin_report_noip"
    val sender_noip = new DataSender(repoName_noip, auth)
    val points = new util.ArrayList[Point]

    sqlContext.sql(
      """
        |select t1.cdn, t1.platform, t1.province, t1.isp1, t1.cdn_ip, connect_avg, connect_sum, connect_point, lag_sum, lag_point from
        |    (select cdn, platform, province, isp1, cdn_ip, avg(v5) as connect_avg, sum(v5) as connect_sum, count(*) as connect_point from quanmin_connect group by cdn, platform, province, isp1, cdn_ip) t1
        |left join
        |    (select cdn, platform, province, isp1, cdn_ip, sum(v4) as lag_sum, count(*) as lag_point from quanmin_lag group by cdn, platform, province, isp1, cdn_ip) t2
        |on t1.cdn=t2.cdn and t1.platform=t2.platform and t1.province=t2.province and t1.isp1=t2.isp1 and t1.cdn_ip=t2.cdn_ip
      """.stripMargin).
      collect().foreach((row: Row) => {
      val p = new Point
      p.append("time", new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX").format(cal.getTime))
      p.append("cdn", row.getString(0))
      p.append("platform", Long.box(row.getLong(1)))
      p.append("province", row.getString(2))
      p.append("isp", row.getString(3))
      p.append("cdn_ip", row.getString(4))
      if (row.get(5) != null) {
        p.append("connect_avg", Double.box(row.getDouble(5)))
      }
      if (row.get(6) != null) {
        p.append("connect_sum", Long.box(row.getLong(6)))
      }
      if (row.get(7) != null) {
        p.append("connect_point", Long.box(row.getLong(7)))
      }
      if (row.get(8) != null) {
        p.append("lag_sum", Long.box(row.getLong(8)))
      }
      if (row.get(9) != null) {
        p.append("lag_point", Long.box(row.getLong(9)))
      }
      points.add(p)
    })
    var err = sender.send(points)
    if (err.getExceptions.size > 0) {
      println("error.getExceptions() = ", err.getExceptions)
    }
    sender.close()
    points.clear()

    sqlContext.sql(
      """
        |select cdn, platform, province, isp1, cdn_ip, first_range, avg(v5) as first_avg, sum(v5) as first_sum, count(*) as first_point from quanmin_first group by cdn, platform, province, isp1, cdn_ip, first_range
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
      p.append("first_point", Long.box(row.getLong(8)))
      points.add(p)
    })
    err = sender.send(points)
    if (err.getExceptions.size > 0) {
      println("error.getExceptions() = ", err.getExceptions)
    }
    sender.close()
    points.clear()

    sqlContext.sql(
      """
        |select t1.cdn, t1.platform, t1.province, t1.isp1, connect_avg, connect_sum, connect_point, lag_sum, lag_point from
        |    (select cdn, platform, province, isp1, avg(v5) as connect_avg, sum(v5) as connect_sum, count(*) as connect_point from quanmin_connect group by cdn, platform, province, isp1) t1
        |left join
        |    (select cdn, platform, province, isp1, sum(v4) as lag_sum, count(*) as lag_point from quanmin_lag group by cdn, platform, province, isp1) t2
        |on t1.cdn=t2.cdn and t1.platform=t2.platform and t1.province=t2.province and t1.isp1=t2.isp1
      """.stripMargin).
      collect().foreach((row: Row) => {
      val p = new Point
      p.append("time", new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX").format(cal.getTime))
      p.append("cdn", row.getString(0))
      p.append("platform", Long.box(row.getLong(1)))
      p.append("province", row.getString(2))
      p.append("isp", row.getString(3))
      if (row.get(4) != null) {
        p.append("connect_avg", Double.box(row.getDouble(4)))
      }
      if (row.get(5) != null) {
        p.append("connect_sum", Long.box(row.getLong(5)))
      }
      if (row.get(6) != null) {
        p.append("connect_point", Long.box(row.getLong(6)))
      }
      if (row.get(7) != null) {
        p.append("lag_sum", Long.box(row.getLong(7)))
      }
      if (row.get(8) != null) {
        p.append("lag_point", Long.box(row.getLong(8)))
      }
      points.add(p)
    })
    err = sender_noip.send(points)
    if (err.getExceptions.size > 0) {
      println("error.getExceptions() = ", err.getExceptions)
    }
    sender_noip.close()
    points.clear()

    sqlContext.sql(
      """
        |select cdn, platform, province, isp1, first_range, avg(v5) as first_avg, sum(v5) as first_sum, count(*) as first_point from quanmin_first group by cdn, platform, province, isp1, first_range
      """.stripMargin).
      collect().foreach((row: Row) => {
      val p = new Point
      p.append("time", new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX").format(cal.getTime))
      p.append("cdn", row.getString(0))
      p.append("platform", Long.box(row.getLong(1)))
      p.append("province", row.getString(2))
      p.append("isp", row.getString(3))
      p.append("first_range", Int.box(row.getInt(4)))
      p.append("first_avg", Double.box(row.getDouble(5)))
      p.append("first_sum", Long.box(row.getLong(6)))
      p.append("first_point", Long.box(row.getLong(7)))
      points.add(p)
    })
    err = sender_noip.send(points)
    if (err.getExceptions.size > 0) {
      println("error.getExceptions() = ", err.getExceptions)
    }
    sender_noip.close()
    points.clear()
  }
}
