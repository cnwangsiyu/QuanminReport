package app

import java.io.{File, FileOutputStream}
import java.text.SimpleDateFormat
import java.util.Calendar

import mail.HtmlMultiPartEmail
import org.apache.spark.sql.Row
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
    var dateString = ""
    val tmpCal = Calendar.getInstance
    var attachmentStringsToSend = scala.collection.mutable.Map[String, String]()
    var tmpString = ""
    var tmpArray1:Array[Row] = Array()
    var tmpArray2:Array[Row] = Array()

    tmpCal.add(Calendar.DATE, -7)
    dateString += dateFormat.format(tmpCal.getTime)
    dateString += "-"
    tmpCal.add(Calendar.DATE, 6)
    dateString += dateFormat.format(tmpCal.getTime)

    for (i <- 0 until 7) {
      cal.add(Calendar.DATE, -1)
      pathArray(i) = "qiniu://quanmin2/export_%s-*".format(dateFormat.format(cal.getTime))
      println(pathArray(i))
    }
    sqlContext.read.parquet(pathArray: _*).registerTempTable("quanmin_this_week")

    for (i <- 0 until 7) {
      cal.add(Calendar.DATE, -1)
      pathArray(i) = "qiniu://quanmin2/export_%s-*".format(dateFormat.format(cal.getTime))
      println(pathArray(i))
    }
    sqlContext.read.parquet(pathArray: _*).registerTempTable("quanmin_last_week")

    tmpString = ", 本周卡顿率, 上周卡顿率, 差异, 趋势\n"
    sqlContext.sql(
      """
        |SELECT t1.cdn, lag_ratio_this, lag_ratio_last, CASE
        |    WHEN lag_ratio_this>lag_ratio_last THEN '↑'
        |    WHEN lag_ratio_this<lag_ratio_last THEN '↓'
        |    ELSE '' END AS diff, lag_ratio_this-lag_ratio_last AS trend FROM
        |    (SELECT v1 AS cdn, avg(lag_ratio) AS lag_ratio_this FROM
        |        (SELECT sum(v4)/count(*) AS lag_ratio, CASE
        |            WHEN v1='bd' OR v1='baidu' THEN '百度'
        |            WHEN v1='qn' THEN '七牛'
        |            WHEN v1='tx' THEN '腾讯'
        |            WHEN v1='al' OR v1='ali' THEN '阿里'
        |            WHEN v1='ws' THEN '网宿'
        |            ELSE '其他' END AS v1 FROM quanmin_this_week_lag GROUP BY v1, day(time)) t
        |    WHERE v1!='其他' GROUP BY v1) t1
        |    JOIN
        |    (SELECT v1 AS cdn, avg(lag_ratio) AS lag_ratio_last FROM
        |        (SELECT sum(v4)/count(*) AS lag_ratio, CASE
        |            WHEN v1='bd' OR v1='baidu' THEN '百度'
        |            WHEN v1='qn' THEN '七牛'
        |            WHEN v1='tx' THEN '腾讯'
        |            WHEN v1='al' OR v1='ali' THEN '阿里'
        |            WHEN v1='ws' THEN '网宿'
        |            ELSE '其他' END AS v1 FROM quanmin_last_week_lag GROUP BY v1, day(time)) t
        |    WHERE v1!='其他' GROUP BY v1) t2
        |    ON t1.cdn=t2.cdn
      """.stripMargin).
      collect().foreach((row: Row) => {
      tmpString += "%s, %f, %f, %s, %f\n".format(row.getString(0), row.getDouble(1), row.getDouble(2), row.getString(3), row.getDouble(4))
    })
    attachmentStringsToSend.update("卡顿率-各家卡顿率（%s）".format(dateString), tmpString)

    tmpString = "本周卡顿率, 上周卡顿率, 差异, 趋势\n"
    sqlContext.sql(
      """
        |SELECT *, CASE
        |    WHEN lag_ratio_this>lag_ratio_last THEN '↑'
        |    WHEN lag_ratio_this<lag_ratio_last THEN '↓'
        |    ELSE '' END AS diff, lag_ratio_this-lag_ratio_last AS trend FROM
        |    (SELECT avg(lag_ratio) AS lag_ratio_this FROM
        |        (SELECT sum(v4)/count(*) AS lag_ratio FROM quanmin_this_week_lag GROUP BY day(time)) t) t1
        |    JOIN
        |    (SELECT avg(lag_ratio) AS lag_ratio_last FROM
        |        (SELECT sum(v4)/count(*) AS lag_ratio FROM quanmin_last_week_lag GROUP BY day(time)) t) t2
      """.stripMargin).
      collect().foreach((row: Row) => {
      tmpString += "%f, %f, %s, %f\n".format(row.getDouble(0), row.getDouble(1), row.getString(2), row.getDouble(3))
    })
    attachmentStringsToSend.update("卡顿率-平台总卡顿率（%s）".format(dateString), tmpString)

    tmpString = "本周卡顿率, 上周卡顿率, 差异, 趋势\n"
    sqlContext.sql(
      """
        |SELECT *, CASE
        |    WHEN lag_ratio_this>lag_ratio_last THEN '↑'
        |    WHEN lag_ratio_this<lag_ratio_last THEN '↓'
        |    ELSE '' END AS diff, lag_ratio_this-lag_ratio_last AS trend FROM
        |    (SELECT avg(lag_ratio) AS lag_ratio_this FROM
        |        (SELECT sum(v4)/count(*) AS lag_ratio FROM quanmin_this_week_lag WHERE isp='教育网' GROUP BY day(time)) t) t1
        |    JOIN
        |    (SELECT avg(lag_ratio) AS lag_ratio_last FROM
        |        (SELECT sum(v4)/count(*) AS lag_ratio FROM quanmin_last_week_lag WHERE isp='教育网' GROUP BY day(time)) t) t2
      """.stripMargin).
      collect().foreach((row: Row) => {
      tmpString += "%f, %f, %s, %f\n".format(row.getDouble(0), row.getDouble(1), row.getString(2), row.getDouble(3))
    })
    attachmentStringsToSend.update("卡顿率-平台教育网总卡顿率（%s）".format(dateString), tmpString)

    tmpString = "省份, 运营商, 卡顿率, 省份, 运营商, 卡顿率\n"
    tmpArray1 = sqlContext.sql(
      """
        |SELECT province, avg(lag_ratio) AS lag_ratio FROM
        |    (SELECT province, sum(v4)/count(*) AS lag_ratio FROM quanmin_this_week_lag WHERE country='中国' GROUP BY province, day(time)) t
        |GROUP BY province ORDER BY lag_ratio DESC LIMIT 5
      """.stripMargin).collect()
    tmpArray2 = sqlContext.sql(
      """
        |SELECT province, avg(lag_ratio) AS lag_ratio FROM
        |    (SELECT province, sum(v4)/count(*) AS lag_ratio FROM quanmin_this_week_lag WHERE country='中国' AND isp='教育网' GROUP BY province, day(time)) t
        |GROUP BY province ORDER BY lag_ratio DESC LIMIT 5
      """.stripMargin).collect()
    for(i <- tmpArray1.indices) {
      tmpString += "%s, %s, %f, %s, %s, %f\n".format(tmpArray1(i).getString(0), "ALL", tmpArray1(i).getDouble(1), tmpArray2(i).getString(0), "教育网", tmpArray2(i).getDouble(1))
    }
    attachmentStringsToSend.update("省份卡顿率排名-最差TOP5（%s）".format(dateString), tmpString)

    tmpString = "省份, 运营商, 卡顿率, 省份, 运营商, 卡顿率\n"
    tmpArray1 = sqlContext.sql(
      """
        |SELECT province, avg(lag_ratio) AS lag_ratio FROM
        |    (SELECT province, sum(v4)/count(*) AS lag_ratio FROM quanmin_this_week_lag WHERE country='中国' GROUP BY province, day(time)) t
        |GROUP BY province ORDER BY lag_ratio LIMIT 5
      """.stripMargin).collect()
    tmpArray2 = sqlContext.sql(
      """
        |SELECT province, avg(lag_ratio) AS lag_ratio FROM
        |    (SELECT province, sum(v4)/count(*) AS lag_ratio FROM quanmin_this_week_lag WHERE country='中国' AND isp='教育网' GROUP BY province, day(time)) t
        |GROUP BY province ORDER BY lag_ratio LIMIT 5
      """.stripMargin).collect()
    for(i <- tmpArray1.indices) {
      tmpString += "%s, %s, %f, %s, %s, %f\n".format(tmpArray1(i).getString(0), "ALL", tmpArray1(i).getDouble(1), tmpArray2(i).getString(0), "教育网", tmpArray2(i).getDouble(1))
    }
    attachmentStringsToSend.update("省份卡顿率排名-最优TOP5（%s）".format(dateString), tmpString)

    tmpString = "cdn, 平台, 首屏时间, 平台, 首屏时间\n"
    tmpArray1 = sqlContext.sql(
      """
        |SELECT v1 AS cdn, avg(first) AS first FROM
        |    (SELECT avg(v5) AS first, CASE
        |        WHEN v1='bd' OR v1='baidu' THEN '百度'
        |        WHEN v1='qn' THEN '七牛'
        |        WHEN v1='tx' THEN '腾讯'
        |        WHEN v1='al' OR v1='ali' THEN '阿里'
        |        WHEN v1='ws' THEN '网宿'
        |        ELSE '其他' END AS v1 FROM quanmin_this_week_first WHERE v5<=10000 AND v5>0 AND (platform=5 OR platform=14) GROUP BY v1, day(time)) t
        |WHERE v1!='其他' GROUP BY v1 ORDER BY cdn
      """.stripMargin).collect()
    tmpArray2 = sqlContext.sql(
      """
        |SELECT v1 AS cdn, avg(first) AS first FROM
        |    (SELECT avg(v5) AS first, CASE
        |        WHEN v1='bd' OR v1='baidu' THEN '百度'
        |        WHEN v1='qn' THEN '七牛'
        |        WHEN v1='tx' THEN '腾讯'
        |        WHEN v1='al' OR v1='ali' THEN '阿里'
        |        WHEN v1='ws' THEN '网宿'
        |        ELSE '其他' END AS v1
        |    FROM quanmin_this_week_first WHERE v5<=10000 AND v5>0 AND platform!=5 AND platform!=14 GROUP BY v1, day(time)) t
        |WHERE v1!='其他' GROUP BY v1 ORDER BY cdn
      """.stripMargin).collect()
    for(i <- tmpArray1.indices) {
      tmpString += "%s, %s, %f, %s, %f\n".format(tmpArray1(i).getString(0), "PC端", tmpArray1(i).getDouble(1), "非PC端", tmpArray2(i).getDouble(1))
    }
    attachmentStringsToSend.update("各家首屏数据（%s）".format(dateString), tmpString)

    tmpString = "排名, cdn厂商, 省份, 本周卡顿率\n"
    sqlContext.sql(
      """
        |SELECT * FROM
        |    (SELECT row_number() OVER (PARTITION BY cdn ORDER BY lag_ratio) AS row_number, * FROM
        |        (SELECT v1 AS cdn, province, avg(lag_ratio) AS lag_ratio FROM
        |            (SELECT province, sum(v4)/count(*) AS lag_ratio,
        |            CASE
        |                WHEN v1='bd' OR v1='baidu' THEN '百度'
        |                WHEN v1='qn' THEN '七牛'
        |                WHEN v1='tx' THEN '腾讯'
        |                WHEN v1='al' OR v1='ali' THEN '阿里'
        |                WHEN v1='ws' THEN '网宿'
        |                ELSE '其他' END AS v1
        |            FROM quanmin_this_week_lag WHERE country='中国' GROUP BY day(time), v1, province) t
        |        WHERE v1!='其他' GROUP BY province, v1) t) t
        |WHERE row_number<=5 ORDER BY cdn, lag_ratio
      """.stripMargin).
      collect().foreach((row: Row) => {
      tmpString += "%d, %s, %s, %f\n".format(row.getInt(0), row.getString(1), row.getString(2), row.getDouble(3))
    })
    attachmentStringsToSend.update("各家CDN卡顿率-省份最优top5排名", tmpString)

    tmpString = "排名, cdn厂商, 省份, 本周卡顿率\n"
    sqlContext.sql(
      """
        |SELECT * FROM
        |    (SELECT row_number() OVER (PARTITION BY cdn ORDER BY lag_ratio DESC) AS row_number, * FROM
        |        (SELECT v1 AS cdn, province, avg(lag_ratio) AS lag_ratio FROM
        |            (SELECT province, sum(v4)/count(*) AS lag_ratio,
        |            CASE
        |                WHEN v1='bd' OR v1='baidu' THEN '百度'
        |                WHEN v1='qn' THEN '七牛'
        |                WHEN v1='tx' THEN '腾讯'
        |                WHEN v1='al' OR v1='ali' THEN '阿里'
        |                WHEN v1='ws' THEN '网宿'
        |                ELSE '其他' END AS v1
        |            FROM quanmin_this_week_lag WHERE country='中国' GROUP BY day(time), v1, province) t
        |        WHERE v1!='其他' GROUP BY province, v1) t) t
        |WHERE row_number<=5 ORDER BY cdn, lag_ratio DESC
      """.stripMargin).
      collect().foreach((row: Row) => {
      tmpString += "%d, %s, %s, %f\n".format(row.getInt(0), row.getString(1), row.getString(2), row.getDouble(3))
    })
    attachmentStringsToSend.update("各家CDN卡顿率-省份最差top5排名", tmpString)

    try {
      val email = new HtmlMultiPartEmail()
      email.setCharset("UTF-8")
      email.setHostName("smtp.sendcloud.net")
      email.setAuthentication("postmaster@apm.mail.qiniu.com", "gW6q6lbbiwFXEoyg")
      email.setFrom("no-reply@apm.mail.qiniu.com", "PILI-APM")
      email.addTo("wangsiyu@qiniu.com")
      email.setSubject("周报数据（%s）".format(dateString))
      email.setHtml("周报见附件")
      attachmentStringsToSend.foreach[Unit]((test: (String, String)) => {
        val fileHandler = new File("/tmp/%s.csv".format(test._1))
        val fileWriter = new FileOutputStream(fileHandler)
        // 为了兼容恶心的微软 excel，我只能手动添加 BOM 了，囧
        val bs = Array(0xEF.toByte, 0xBB.toByte, 0xBF.toByte)
        fileWriter.write(bs)
        fileWriter.write(test._2.getBytes("UTF-8"))
        fileWriter.close()
        email.attach(fileHandler)
      })
      email.send()
    } catch {
      case e: Exception => println(e)
    }
  }
}
