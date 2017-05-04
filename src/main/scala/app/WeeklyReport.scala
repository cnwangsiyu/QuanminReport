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
    var tmpArray1: Array[Row] = Array()
    var tmpArray2: Array[Row] = Array()
    var tmpArray3: Array[Row] = Array()
    var htmlTemplateString =
      """
        |<html>
        |
        |<body>
        |
        |<h4>卡顿率-平台总卡顿率：</h4>
        |<table border="1">
        |<tr>
        |  <td>本周卡顿率</td>
        |  <td>上周卡顿率</td>
        |  <td>差异</td>
        |  <td>趋势</td>
        |</tr>
        |%s
        |</table>
        |
        |<h4>卡顿率-平台教育网总卡顿率：</h4>
        |<table border="1">
        |<tr>
        |  <td>本周卡顿率</td>
        |  <td>上周卡顿率</td>
        |  <td>差异</td>
        |  <td>趋势</td>
        |</tr>
        |%s
        |</table>
        |
        |<h4>卡顿率-各家卡顿率：</h4>
        |<table border="1">
        |<tr>
        |  <td></td>
        |  <td>本周卡顿率</td>
        |  <td>上周卡顿率</td>
        |  <td>差异</td>
        |  <td>趋势</td>
        |</tr>
        |%s
        |</table>
        |
        |<h4>卡顿率-各家分平台卡顿率：</h4>
        |<table border="1">
        |<tr>
        |  <td>cdn</td>
        |  <td>平台</td>
        |  <td>卡顿率</td>
        |  <td>平台</td>
        |  <td>卡顿率</td>
        |  <td>平台</td>
        |  <td>卡顿率</td>
        |</tr>
        |%s
        |</table>
        |
        |<h4>卡顿次数比率-本周各运营商卡顿率：</h4>
        |<table border="1">
        |<tr>
        |  <td>cdn厂商</td>
        |  <td>运营商</td>
        |  <td>卡顿次数比率</td>
        |</tr>
        |%s
        |</table>
        |
        |<h4>各家分平台首屏：</h4>
        |<table border="1">
        |<tr>
        |  <td>cdn</td>
        |  <td>平台</td>
        |  <td>首屏时间</td>
        |  <td>平台</td>
        |  <td>首屏时间</td>
        |  <td>平台</td>
        |  <td>首屏时间</td>
        |</tr>
        |%s
        |</table>
        |
        |<h4>省份卡顿率排名-最差TOP5：</h4>
        |<table border="1">
        |<tr>
        |  <td>省份</td>
        |  <td>运营商</td>
        |  <td>卡顿率</td>
        |  <td>省份</td>
        |  <td>运营商</td>
        |  <td>卡顿率</td>
        |</tr>
        |%s
        |</table>
        |
        |<h4>省份卡顿率排名-最优TOP5：</h4>
        |<table border="1">
        |<tr>
        |  <td>省份</td>
        |  <td>运营商</td>
        |  <td>卡顿率</td>
        |  <td>省份</td>
        |  <td>运营商</td>
        |  <td>卡顿率</td>
        |</tr>
        |%s
        |</table>
        |
        |<h4>各家CDN卡顿率-省份最优top5排名：</h4>
        |<table border="1">
        |<tr>
        |  <td>排名</td>
        |  <td>cdn厂商</td>
        |  <td>省份</td>
        |  <td>本周卡顿率</td>
        |</tr>
        |%s
        |</table>
        |
        |<h4>各家CDN卡顿率-省份最差top5排名：</h4>
        |<table border="1">
        |<tr>
        |  <td>排名</td>
        |  <td>cdn厂商</td>
        |  <td>省份</td>
        |  <td>本周卡顿率</td>
        |</tr>
        |%s
        |</table>
        |
        |</body>
        |</html>
      """.stripMargin
    var htmlRows = new Array[String](10)

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
        |    ELSE '未定义' END AS cdn, *
        |FROM quanmin_this_week WHERE tag='monitor' AND room_id!=-1 AND v5>1 AND client_ip!="125.90.93.217" AND client_ip!="125.90.93.83" AND client_ip!="125.90.88.103" AND client_ip!="183.56.171.42" AND client_ip!="125.90.93.215" AND client_ip!="183.56.170.244" AND client_ip!="218.30.131.254
      """.stripMargin).cache().registerTempTable("quanmin_this_week_lag")
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
        |    ELSE '未定义' END AS cdn, *
        |FROM quanmin_last_week WHERE tag='monitor' AND room_id!=-1 AND v5>1 AND client_ip!="125.90.93.217" AND client_ip!="125.90.93.83" AND client_ip!="125.90.88.103" AND client_ip!="183.56.171.42" AND client_ip!="125.90.93.215" AND client_ip!="183.56.170.244" AND client_ip!="218.30.131.254
      """.stripMargin).cache().registerTempTable("quanmin_last_week_lag")
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
        |    ELSE '未定义' END AS cdn, *
        |FROM quanmin_this_week WHERE tag='first' AND room_id!=-1 AND v5<=10000 AND v5>0 AND client_ip!="125.90.93.217" AND client_ip!="125.90.93.83" AND client_ip!="125.90.88.103" AND client_ip!="183.56.171.42" AND client_ip!="125.90.93.215" AND client_ip!="183.56.170.244" AND client_ip!="218.30.131.254
      """.stripMargin).cache().registerTempTable("quanmin_this_week_first")
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
        |    ELSE '未定义' END AS cdn, *
        |FROM quanmin_last_week WHERE tag='first' AND room_id!=-1 AND v5<=10000 AND v5>0 AND client_ip!="125.90.93.217" AND client_ip!="125.90.93.83" AND client_ip!="125.90.88.103" AND client_ip!="183.56.171.42" AND client_ip!="125.90.93.215" AND client_ip!="183.56.170.244" AND client_ip!="218.30.131.254
      """.stripMargin).cache().registerTempTable("quanmin_last_week_first")
    sqlContext.sql(
      """
        |SELECT province AS province2 FROM
        |    (SELECT province, count(*) AS total_count FROM quanmin_this_week_lag GROUP BY province) t
        |WHERE total_count>100000
      """.stripMargin).cache().registerTempTable("quanmin_valid_province")


    tmpString = "本周卡顿率, 上周卡顿率, 差异, 趋势\n"
    htmlRows(0) = ""
    sqlContext.sql(
      """
        |SELECT *, CASE
        |    WHEN lag_ratio_this>lag_ratio_last THEN '↑'
        |    WHEN lag_ratio_this<lag_ratio_last THEN '↓'
        |    ELSE '' END AS diff, lag_ratio_this-lag_ratio_last AS trend FROM
        |        (SELECT avg(lag_ratio) AS lag_ratio_this FROM
        |            (SELECT sum(v4)/count(*) AS lag_ratio FROM quanmin_this_week_lag GROUP BY day(time)) t) t1
        |        JOIN
        |        (SELECT avg(lag_ratio) AS lag_ratio_last FROM
        |            (SELECT sum(v4)/count(*) AS lag_ratio FROM quanmin_last_week_lag GROUP BY day(time)) t) t2
      """.stripMargin).
      collect().foreach((row: Row) => {
      tmpString += "%f, %f, %s, %f\n".format(row.getDouble(0), row.getDouble(1), row.getString(2), row.getDouble(3))
      htmlRows(0) +=
        """
          |<tr>
          |  <td>%f</td>
          |  <td>%f</td>
          |  <td>%s</td>
          |  <td>%f</td>
          |</tr>
        """.stripMargin.format(row.getDouble(0), row.getDouble(1), row.getString(2), row.getDouble(3))
    })
    attachmentStringsToSend.update("卡顿率-平台总卡顿率（%s）".format(dateString), tmpString)

    tmpString = "本周卡顿率, 上周卡顿率, 差异, 趋势\n"
    htmlRows(1) = ""
    sqlContext.sql(
      """
        |SELECT *, CASE
        |    WHEN lag_ratio_this>lag_ratio_last THEN '↑'
        |    WHEN lag_ratio_this<lag_ratio_last THEN '↓'
        |    ELSE '' END AS diff, lag_ratio_this-lag_ratio_last AS trend FROM
        |        (SELECT avg(lag_ratio) AS lag_ratio_this FROM
        |            (SELECT sum(v4)/count(*) AS lag_ratio FROM quanmin_this_week_lag WHERE isp='教育网' GROUP BY day(time)) t) t1
        |        JOIN
        |        (SELECT avg(lag_ratio) AS lag_ratio_last FROM
        |            (SELECT sum(v4)/count(*) AS lag_ratio FROM quanmin_last_week_lag WHERE isp='教育网' GROUP BY day(time)) t) t2
      """.stripMargin).
      collect().foreach((row: Row) => {
      tmpString += "%f, %f, %s, %f\n".format(row.getDouble(0), row.getDouble(1), row.getString(2), row.getDouble(3))
      htmlRows(1) +=
        """
          |<tr>
          |  <td>%f</td>
          |  <td>%f</td>
          |  <td>%s</td>
          |  <td>%f</td>
          |</tr>
        """.stripMargin.format(row.getDouble(0), row.getDouble(1), row.getString(2), row.getDouble(3))
    })
    attachmentStringsToSend.update("卡顿率-平台教育网总卡顿率（%s）".format(dateString), tmpString)

    tmpString = ", 本周卡顿率, 上周卡顿率, 差异, 趋势\n"
    htmlRows(2) = ""
    sqlContext.sql(
      """
        |SELECT t1.cdn, lag_ratio_this, lag_ratio_last, CASE
        |    WHEN lag_ratio_this>lag_ratio_last THEN '↑'
        |    WHEN lag_ratio_this<lag_ratio_last THEN '↓'
        |    ELSE '' END AS diff,
        |    lag_ratio_this-lag_ratio_last AS trend FROM
        |        (SELECT cdn, avg(lag_ratio) AS lag_ratio_this FROM
        |            (SELECT cdn, sum(v4)/count(*) AS lag_ratio
        |            FROM quanmin_this_week_lag GROUP BY cdn, day(time)) t
        |        GROUP BY cdn) t1
        |        JOIN
        |        (SELECT cdn, avg(lag_ratio) AS lag_ratio_last FROM
        |            (SELECT cdn, sum(v4)/count(*) AS lag_ratio
        |            FROM quanmin_last_week_lag GROUP BY cdn, day(time)) t
        |        GROUP BY cdn) t2
        |        ON t1.cdn=t2.cdn
        |ORDER BY instr('网宿百度腾讯阿里七牛云帆金山未定义', t1.cdn)
      """.stripMargin).
      collect().foreach((row: Row) => {
      tmpString += "%s, %f, %f, %s, %f\n".format(row.getString(0), row.getDouble(1), row.getDouble(2), row.getString(3), row.getDouble(4))
      htmlRows(2) +=
        """
          |<tr>
          |  <td>%s</td>
          |  <td>%f</td>
          |  <td>%f</td>
          |  <td>%s</td>
          |  <td>%f</td>
          |</tr>
        """.stripMargin.format(row.getString(0), row.getDouble(1), row.getDouble(2), row.getString(3), row.getDouble(4))
    })
    attachmentStringsToSend.update("卡顿率-各家卡顿率（%s）".format(dateString), tmpString)

    tmpString = "cdn, 平台, 卡顿率, 平台, 卡顿率, 平台, 卡顿率\n"
    htmlRows(3) = ""
    tmpArray1 = sqlContext.sql(
      """
        |SELECT cdn, avg(lag_ratio) AS lag_ratio FROM
        |    (SELECT cdn, sum(v4)/count(*) AS lag_ratio
        |    FROM quanmin_this_week_lag WHERE platform=5 OR platform=14 GROUP BY cdn, day(time)) t
        |GROUP BY cdn ORDER BY instr('网宿百度腾讯阿里七牛云帆金山未定义', cdn)
      """.stripMargin).collect()
    tmpArray2 = sqlContext.sql(
      """
        |SELECT cdn, avg(lag_ratio) AS lag_ratio FROM
        |    (SELECT cdn, sum(v4)/count(*) AS lag_ratio
        |    FROM quanmin_this_week_lag WHERE platform=1 GROUP BY cdn, day(time)) t
        |GROUP BY cdn ORDER BY instr('网宿百度腾讯阿里七牛云帆金山未定义', cdn)
      """.stripMargin).collect()
    tmpArray3 = sqlContext.sql(
      """
        |SELECT cdn, avg(lag_ratio) AS lag_ratio FROM
        |    (SELECT cdn, sum(v4)/count(*) AS lag_ratio
        |    FROM quanmin_this_week_lag WHERE platform=2 GROUP BY cdn, day(time)) t
        |GROUP BY cdn ORDER BY instr('网宿百度腾讯阿里七牛云帆金山未定义', cdn)
      """.stripMargin).collect()
    for (i <- tmpArray1.indices) {
      tmpString += "%s, %s, %f, %s, %f, %s, %f\n".format(tmpArray1(i).getString(0), "PC端", tmpArray1(i).getDouble(1), "Android端", tmpArray2(i).getDouble(1), "iOS端", tmpArray3(i).getDouble(1))
      htmlRows(3) +=
        """
          |<tr>
          |  <td>%s</td>
          |  <td>%s</td>
          |  <td>%f</td>
          |  <td>%s</td>
          |  <td>%f</td>
          |  <td>%s</td>
          |  <td>%f</td>
          |</tr>
        """.stripMargin.format(tmpArray1(i).getString(0), "PC端", tmpArray1(i).getDouble(1), "Android端", tmpArray2(i).getDouble(1), "iOS端", tmpArray3(i).getDouble(1))
    }
    attachmentStringsToSend.update("卡顿率-各家分平台卡顿率（%s）".format(dateString), tmpString)

    tmpString = "cdn厂商, 运营商, 卡顿次数比率\n"
    htmlRows(4) = ""
    sqlContext.sql(
      """
        |SELECT cdn, isp, avg(lag_ratio) AS lag_ratio FROM
        |    (SELECT cdn, CASE
        |        WHEN isp='电信' OR isp='移动' OR isp='联通' THEN '三大运营商'
        |        WHEN isp='教育网' THEN '教育网'
        |        ELSE '其他' END AS isp, sum(v4)/count(*) AS lag_ratio
        |    FROM quanmin_this_week_lag GROUP BY day(time), cdn, isp) t
        |GROUP BY cdn, isp ORDER BY instr('三大运营商教育网其他', isp), instr('网宿百度腾讯阿里七牛云帆金山未定义', cdn)
      """.stripMargin).
      collect().foreach((row: Row) => {
      tmpString += "%s, %s, %f\n".format(row.getString(0), row.getString(1), row.getDouble(2))
      htmlRows(4) +=
        """
          |<tr>
          |  <td>%s</td>
          |  <td>%s</td>
          |  <td>%f</td>
          |</tr>
        """.stripMargin.format(row.getString(0), row.getString(1), row.getDouble(2))
    })
    attachmentStringsToSend.update("卡顿次数比率-本周各运营商卡顿率(%s)".format(dateString), tmpString)

    tmpString = "cdn, 平台, 首屏时间, 平台, 首屏时间, 平台, 首屏时间\n"
    htmlRows(5) = ""
    tmpArray1 = sqlContext.sql(
      """
        |SELECT cdn, avg(first) AS first FROM
        |    (SELECT cdn, avg(v5) AS first
        |    FROM quanmin_this_week_first WHERE platform=5 OR platform=14 GROUP BY cdn, day(time)) t
        |GROUP BY cdn ORDER BY instr('网宿百度腾讯阿里七牛云帆金山未定义', cdn)
      """.stripMargin).collect()
    tmpArray2 = sqlContext.sql(
      """
        |SELECT cdn, avg(first) AS first FROM
        |    (SELECT cdn, avg(v5) AS first
        |    FROM quanmin_this_week_first WHERE platform=1 GROUP BY cdn, day(time)) t
        |GROUP BY cdn ORDER BY instr('网宿百度腾讯阿里七牛云帆金山未定义', cdn)
      """.stripMargin).collect()
    tmpArray3 = sqlContext.sql(
      """
        |SELECT cdn, avg(first) AS first FROM
        |    (SELECT cdn, avg(v5) AS first
        |    FROM quanmin_this_week_first WHERE platform=2 GROUP BY cdn, day(time)) t
        |GROUP BY cdn ORDER BY instr('网宿百度腾讯阿里七牛云帆金山未定义', cdn)
      """.stripMargin).collect()
    for (i <- tmpArray1.indices) {
      tmpString += "%s, %s, %f, %s, %f, %s, %f\n".format(tmpArray1(i).getString(0), "PC端", tmpArray1(i).getDouble(1), "Android端", tmpArray2(i).getDouble(1), "iOS端", tmpArray3(i).getDouble(1))
      htmlRows(5) +=
        """
          |<tr>
          |  <td>%s</td>
          |  <td>%s</td>
          |  <td>%f</td>
          |  <td>%s</td>
          |  <td>%f</td>
          |  <td>%s</td>
          |  <td>%f</td>
          |</tr>
        """.stripMargin.format(tmpArray1(i).getString(0), "PC端", tmpArray1(i).getDouble(1), "Android端", tmpArray2(i).getDouble(1), "iOS端", tmpArray3(i).getDouble(1))
    }
    attachmentStringsToSend.update("各家分平台首屏（%s）".format(dateString), tmpString)

    tmpString = "省份, 运营商, 卡顿率, 省份, 运营商, 卡顿率\n"
    htmlRows(6) = ""
    tmpArray1 = sqlContext.sql(
      """
        |SELECT province, avg(lag_ratio) AS lag_ratio FROM
        |    (SELECT province, sum(v4)/count(*) AS lag_ratio FROM quanmin_this_week_lag WHERE country='中国' AND province!='中国' GROUP BY province, day(time)) t
        |GROUP BY province ORDER BY lag_ratio DESC LIMIT 5
      """.stripMargin).collect()
    tmpArray2 = sqlContext.sql(
      """
        |SELECT province, avg(lag_ratio) AS lag_ratio FROM
        |    (SELECT province, sum(v4)/count(*) AS lag_ratio FROM quanmin_this_week_lag WHERE country='中国' AND province!='中国' AND isp='教育网' GROUP BY province, day(time)) t
        |GROUP BY province ORDER BY lag_ratio DESC LIMIT 5
      """.stripMargin).collect()
    for (i <- tmpArray1.indices) {
      tmpString += "%s, %s, %f, %s, %s, %f\n".format(tmpArray1(i).getString(0), "ALL", tmpArray1(i).getDouble(1), tmpArray2(i).getString(0), "教育网", tmpArray2(i).getDouble(1))
      htmlRows(6) +=
        """
          |<tr>
          |  <td>%s</td>
          |  <td>%s</td>
          |  <td>%f</td>
          |  <td>%s</td>
          |  <td>%s</td>
          |  <td>%f</td>
          |</tr>
        """.stripMargin.format(tmpArray1(i).getString(0), "ALL", tmpArray1(i).getDouble(1), tmpArray2(i).getString(0), "教育网", tmpArray2(i).getDouble(1))
    }
    attachmentStringsToSend.update("省份卡顿率排名-最差TOP5（%s）".format(dateString), tmpString)

    tmpString = "省份, 运营商, 卡顿率, 省份, 运营商, 卡顿率\n"
    htmlRows(7) = ""
    tmpArray1 = sqlContext.sql(
      """
        |SELECT * FROM
        |    (SELECT province, avg(lag_ratio) AS lag_ratio FROM
        |        (SELECT province, sum(v4)/count(*) AS lag_ratio FROM quanmin_this_week_lag WHERE country='中国' AND province!='中国' GROUP BY province, day(time)) t
        |    GROUP BY province) t1
        |    INNER JOIN
        |    quanmin_valid_province
        |    ON t1.province=quanmin_valid_province.province2
        |ORDER BY lag_ratio LIMIT 5
      """.stripMargin).collect()
    tmpArray2 = sqlContext.sql(
      """
        |SELECT * FROM
        |    (SELECT province, avg(lag_ratio) AS lag_ratio FROM
        |        (SELECT province, sum(v4)/count(*) AS lag_ratio FROM quanmin_this_week_lag WHERE country='中国' AND province!='中国' AND isp='教育网' GROUP BY province, day(time)) t
        |    GROUP BY province) t1
        |    INNER JOIN
        |    quanmin_valid_province
        |    ON t1.province=quanmin_valid_province.province2
        |ORDER BY lag_ratio LIMIT 5
      """.stripMargin).collect()
    for (i <- tmpArray1.indices) {
      tmpString += "%s, %s, %f, %s, %s, %f\n".format(tmpArray1(i).getString(0), "ALL", tmpArray1(i).getDouble(1), tmpArray2(i).getString(0), "教育网", tmpArray2(i).getDouble(1))
      htmlRows(7) +=
        """
          |<tr>
          |  <td>%s</td>
          |  <td>%s</td>
          |  <td>%f</td>
          |  <td>%s</td>
          |  <td>%s</td>
          |  <td>%f</td>
          |</tr>
        """.stripMargin.format(tmpArray1(i).getString(0), "ALL", tmpArray1(i).getDouble(1), tmpArray2(i).getString(0), "教育网", tmpArray2(i).getDouble(1))
    }
    attachmentStringsToSend.update("省份卡顿率排名-最优TOP5（%s）".format(dateString), tmpString)

    tmpString = "排名, cdn厂商, 省份, 本周卡顿率\n"
    htmlRows(8) = ""
    sqlContext.sql(
      """
        |SELECT * FROM
        |    (SELECT row_number() OVER (PARTITION BY cdn ORDER BY lag_ratio) AS row_number, * FROM
        |        (SELECT cdn, province, avg(lag_ratio) AS lag_ratio FROM
        |            (SELECT cdn, province, sum(v4)/count(*) AS lag_ratio
        |            FROM quanmin_this_week_lag WHERE country='中国' AND province!='中国' GROUP BY day(time), cdn, province) t
        |        GROUP BY province, cdn
        |        ) t1
        |        INNER JOIN
        |        quanmin_valid_province
        |        ON t1.province=quanmin_valid_province.province2
        |    ) t
        |WHERE row_number<=5 ORDER BY instr('网宿百度腾讯阿里七牛云帆金山未定义', cdn), lag_ratio
      """.stripMargin).
      collect().foreach((row: Row) => {
      tmpString += "%d, %s, %s, %f\n".format(row.getInt(0), row.getString(1), row.getString(2), row.getDouble(3))
      htmlRows(8) +=
        """
          |<tr>
          |  <td>%d</td>
          |  <td>%s</td>
          |  <td>%s</td>
          |  <td>%f</td>
          |</tr>
        """.stripMargin.format(row.getInt(0), row.getString(1), row.getString(2), row.getDouble(3))
    })
    attachmentStringsToSend.update("各家CDN卡顿率-省份最优top5排名", tmpString)

    tmpString = "排名, cdn厂商, 省份, 本周卡顿率\n"
    htmlRows(9) = ""
    sqlContext.sql(
      """
        |SELECT * FROM
        |    (SELECT row_number() OVER (PARTITION BY cdn ORDER BY lag_ratio DESC) AS row_number, * FROM
        |        (SELECT cdn, province, avg(lag_ratio) AS lag_ratio FROM
        |            (SELECT cdn, province, sum(v4)/count(*) AS lag_ratio
        |            FROM quanmin_this_week_lag WHERE country='中国' AND province!='中国' GROUP BY day(time), cdn, province) t
        |        GROUP BY province, cdn
        |        ) t1
        |        INNER JOIN
        |        quanmin_valid_province
        |        ON t1.province=quanmin_valid_province.province2
        |    ) t
        |WHERE row_number<=5 ORDER BY instr('网宿百度腾讯阿里七牛云帆金山未定义', cdn), lag_ratio DESC
      """.stripMargin).
      collect().foreach((row: Row) => {
      tmpString += "%d, %s, %s, %f\n".format(row.getInt(0), row.getString(1), row.getString(2), row.getDouble(3))
      htmlRows(9) +=
        """
          |<tr>
          |  <td>%d</td>
          |  <td>%s</td>
          |  <td>%s</td>
          |  <td>%f</td>
          |</tr>
        """.stripMargin.format(row.getInt(0), row.getString(1), row.getString(2), row.getDouble(3))
    })
    attachmentStringsToSend.update("各家CDN卡顿率-省份最差top5排名", tmpString)

    val email = new HtmlMultiPartEmail()
    email.setCharset("UTF-8")
    email.setHostName("smtp.sendcloud.net")
    email.setAuthentication("postmaster@apm.mail.qiniu.com", "gW6q6lbbiwFXEoyg")
    email.setFrom("no-reply@apm.mail.qiniu.com", "PILI-APM")
    email.addTo("fangchaochao@qmtv.com")
    email.addTo("dengyarong@qmtv.com")
    email.addTo("huangyisan@qmtv.com")
    email.addCc("zhangyunlong@qmtv.com")
    email.setSubject("周报数据（%s）".format(dateString))
    email.setHtml(htmlTemplateString.format(htmlRows: _*))
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
  }
}
