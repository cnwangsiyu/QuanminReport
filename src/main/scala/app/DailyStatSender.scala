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
    for (_ <- 0 to 30) {
      tmp.add(Calendar.DATE, -1)
      val cal = Calendar.getInstance
      cal.clear()
      cal.set(tmp.get(Calendar.YEAR), tmp.get(Calendar.MONTH), tmp.get(Calendar.DATE))
      val yesterday = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)

      val dataPath = "qiniu://quanmin2/export_%s-*".format(yesterday)
      val quanmin = sqlContext.read.parquet(dataPath)
      quanmin.printSchema()
      quanmin.registerTempTable("quanmin_raw")

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
          |    ELSE platform END AS platform1, *
          |FROM quanmin_raw WHERE tag='monitor' AND room_id!=-1 AND v5>1
        """.stripMargin).cache().registerTempTable("quanmin_lag")

      val auth = Auth.create("YFvDcv7ie2tmSCRjX8aYHwrfqpeXR4M_ef2Az1CK", "MCBFkF6tv55uxavHTnxKEFt8f7uKL5rD0Lv2gL5n")
      val repoName = "quanmin_report_lag"
      val sender = new DataSender(repoName, auth)
      val points = new util.ArrayList[Point]

      sqlContext.sql("select v1 as cdn, platform, province, isp, sum(v4) as total_lag, count(*) as total_point from quanmin_lag where country='中国' group by v1, platform, province, isp").
        collect().foreach((row: Row) => {
        val p = new Point
        p.append("time", new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX").format(cal.getTime))
        println(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX").format(cal.getTime))
        p.append("cdn", row.getString(0))
        p.append("platform", Long.box(row.getLong(1)))
        p.append("province", row.getString(2))
        p.append("isp", row.getString(3))
        p.append("total_lag", Long.box(row.getLong(4)))
        p.append("total_point", Long.box(row.getLong(5)))
        points.add(p)
      })
      val err = sender.send(points)
      if (err.getExceptions.size > 0) {
        println("error.getExceptions() = ", err.getExceptions)
      }
      sender.close()
    }
  }
}
