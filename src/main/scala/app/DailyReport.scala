package app

import java.io.{File, FileOutputStream}
import java.text.SimpleDateFormat
import java.util.Calendar
import mail.HtmlMultiPartEmail
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import udf.MyOrAgg
import scala.collection.mutable

/**
  * Created by WangSiyu on 15/03/2017.
  */
object DailyReport {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().
      setAppName("daily-report").
      setMaster("local[8]")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)

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

    sqlContext.udf.register("myOrAgg", new MyOrAgg)



    var attachmentStringsToSend = scala.collection.mutable.Map[String, String]()
    var tmpString = ""
    var htmlTemplateString =
      """
        |<html>
        |
        |<body>
        |
        |<h4>卡顿次数比率-平台总卡顿率：</h4>
        |<table border="1">
        |<tr>
        |  <td>本日平台总卡顿率</td>
        |  <td>%f</td>
        |</tr>
        |</table>
        |
        |<h4>卡顿次数比率-全天各厂商总卡顿率：</h4>
        |<table border="1">
        |<tr>
        |  <td>cdn运营商</td>
        |  <td>卡顿次数比率</td>
        |</tr>
        |%s
        |</table>
        |
        |<h4>卡顿次数比率-全天各运营商卡顿率：</h4>
        |<table border="1">
        |<tr>
        |  <td>cdn厂商</td>
        |  <td>运营商</td>
        |  <td>卡顿次数比率</td>
        |</tr>
        |%s
        |</table>
        |
        |<h4>卡顿率-各家分平台卡顿率：</h4>
        |<table border="1">
        |<tr>
        |  <td>cdn厂商</td>
        |  <td>终端类型</td>
        |  <td>卡顿率</td>
        |  <td>终端类型</td>
        |  <td>卡顿率</td>
        |  <td>终端类型</td>
        |  <td>卡顿率</td>
        |</tr>
        |%s
        |</table>
        |
        |<h4>首屏数据：</h4>
        |<table border="1">
        |<tr>
        |  <td>cdn厂商</td>
        |  <td>终端类型</td>
        |  <td>首屏时间</td>
        |  <td>终端类型</td>
        |  <td>首屏时间</td>
        |  <td>终端类型</td>
        |  <td>首屏时间</td>
        |</tr>
        |%s
        |</table>
        |
        |</body>
        |</html>
      """.stripMargin

    sc.hadoopConfiguration.set("fs.qiniu.access.key", "YFvDcv7ie2tmSCRjX8aYHwrfqpeXR4M_ef2Az1CK")
    sc.hadoopConfiguration.set("fs.qiniu.secret.key", "MCBFkF6tv55uxavHTnxKEFt8f7uKL5rD0Lv2gL5n")
    sc.hadoopConfiguration.set("fs.qiniu.bucket.domain", "http://oihu9i4fk.bkt.clouddn.com")

    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val cal = Calendar.getInstance
    cal.add(Calendar.DATE, -1)
    val yesterday = dateFormat.format(cal.getTime)

    val dataPath = "qiniu://quanmin2/export_%s-*".format(yesterday)
    val quanmin = sqlContext.read.parquet(dataPath)
    var tmpArray1:Array[Row] = Array()
    var tmpArray2:Array[Row] = Array()
    var tmpArray3:Array[Row] = Array()
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
        |FROM quanmin_raw WHERE tag='first' AND room_id!=-1 AND v5<=10000 AND v5>0
      """.stripMargin).cache().registerTempTable("quanmin_first")



    tmpString = "序号, cdn运营商, 终端类型, 卡顿总次数, 总请求数, 卡顿次数比率\n"
    sqlContext.sql("SELECT row_number() OVER (ORDER BY instr('网宿百度腾讯阿里七牛云帆金山未定义', cdn), platform1) AS row_number, cdn, platform1, sum(v4) AS lag_count, count(v4) AS total_count, sum(v4)/count(v4) AS lag_ratio FROM quanmin_lag GROUP BY cdn, platform1").
      collect().foreach((row: Row) => {
      tmpString += "%d, %s, %s, %d, %d, %f\n".format(row.getInt(0), row.getString(1), getDeviceName(row.getLong(2)), row.getLong(3), row.getLong(4), row.getDouble(5))
    })
    attachmentStringsToSend.update("[%s]卡顿次数比率-全天".format(yesterday), tmpString)

    tmpString = "序号, cdn运营商, 终端类型, 卡顿人数, 观看人数, 卡顿人数比率\n"
    sqlContext.sql(
      """
        |SELECT row_number() OVER (ORDER BY instr('网宿百度腾讯阿里七牛云帆金山未定义', cdn), platform1) AS row_number, cdn, platform1, sum(lag) AS lag_person, count(lag) AS total_person, sum(lag)/count(lag) AS lag_ratio FROM
        |    (SELECT cdn, platform1, device, myOrAgg(v4) AS lag FROM
        |        quanmin_lag
        |    GROUP BY cdn, platform1, device) t
        |GROUP BY cdn, platform1
      """.stripMargin).
      collect().foreach((row: Row) => {
      tmpString += "%d, %s, %s, %d, %d, %f\n".format(row.getInt(0), row.getString(1), getDeviceName(row.getLong(2)), row.getLong(3), row.getLong(4), row.getDouble(5))
    })
    attachmentStringsToSend.update("[%s]卡顿人数比率-全天".format(yesterday), tmpString)

    tmpString = "序号, cdn运营商, 终端类型, 卡顿总次数, 总请求数, 卡顿次数比率\n"
    sqlContext.sql("SELECT row_number() OVER (ORDER BY instr('网宿百度腾讯阿里七牛云帆金山未定义', cdn), platform1) AS row_number, cdn, platform1, sum(v4) AS lag_count, count(v4) AS total_count, sum(v4)/count(v4) AS lag_ratio FROM quanmin_lag WHERE hour(time)>=19 GROUP BY cdn, platform1").
      collect().foreach((row: Row) => {
      tmpString += "%d, %s, %s, %d, %d, %f\n".format(row.getInt(0), row.getString(1), getDeviceName(row.getLong(2)), row.getLong(3), row.getLong(4), row.getDouble(5))
    })
    attachmentStringsToSend.update("[%s]卡顿次数比率-晚高峰".format(yesterday), tmpString)

    tmpString = "序号, cdn运营商, 终端类型, 卡顿人数, 观看人数, 卡顿人数比率\n"
    sqlContext.sql(
      """
        |SELECT row_number() OVER (ORDER BY instr('网宿百度腾讯阿里七牛云帆金山未定义', cdn), platform1) AS row_number, cdn, platform1, sum(lag) AS lag_person, count(lag) AS total_person, sum(lag)/count(lag) AS lag_ratio FROM
        |    (SELECT cdn, platform1, device, myOrAgg(v4) AS lag FROM quanmin_lag
        |    WHERE hour(time)>=19 GROUP BY cdn, platform1, device) t
        |GROUP BY cdn, platform1
      """.stripMargin).
      collect().foreach((row: Row) => {
      tmpString += "%d, %s, %s, %d, %d, %f\n".format(row.getInt(0), row.getString(1), getDeviceName(row.getLong(2)), row.getLong(3), row.getLong(4), row.getDouble(5))
    })
    attachmentStringsToSend.update("[%s]卡顿人数比率-晚高峰".format(yesterday), tmpString)

    val tmpList = mutable.MutableList[mutable.MutableList[String]]()
    var previousCdn = ""
    var maxLength = 0
    tmpString = ""
    sqlContext.sql(
      """
        |SELECT * FROM
        |    (SELECT row_number() OVER (PARTITION BY cdn, isp ORDER BY lag_ratio DESC) AS row_number, * FROM
        |        (SELECT cdn, isp, province, sum(v4) AS lag_count, count(v4) AS total_count, sum(v4)/count(v4) AS lag_ratio FROM quanmin_lag WHERE country='中国' AND (isp='联通' OR isp='电信' OR isp='移动' OR isp='教育网') GROUP BY province, cdn, isp) t
        |    WHERE total_count>500) t
        |WHERE row_number<=5 ORDER BY instr('网宿百度腾讯阿里七牛云帆金山未定义', cdn), instr('电信移动联通教育网', isp), lag_ratio DESC
      """.stripMargin).
      collect().foreach((row: Row) => {
      val singleRow = "%d, %s, %s, %s, %d, %d, %f".format(row.getInt(0), row.getString(1), row.getString(2), row.getString(3), row.getLong(4), row.getLong(5), row.getDouble(6))
      if (previousCdn != row.getString(1)) {
        tmpList += mutable.MutableList[String]("序号, cdn运营商, isp, 省份, 卡顿总次数, 总请求数, 卡顿次数比率",
          singleRow)
        previousCdn = row.getString(1)
      } else {
        tmpList.last += singleRow
      }
      if (tmpList.last.length > maxLength) {
        maxLength = tmpList.last.length
      }
    })
    tmpList.map((x: mutable.MutableList[String]) => {
      for (_ <- x.length until maxLength) {
        x += ",,,,,,"
      }
      x
    })
    tmpString += tmpList.reduce((a: mutable.MutableList[String], b: mutable.MutableList[String]) => {
      val l = mutable.MutableList[String]()
      for (i <- 0 until maxLength) {
        l += (a(i) + ",," + b(i))
      }
      l
    }).reduce((a: String, b: String) => {
      a + "\n" + b
    })
    attachmentStringsToSend.update("[%s]省份卡顿次数top5".format(yesterday), tmpString)

    tmpString = "序号, cdn运营商, cdn_ip, 卡顿总次数, 总请求数, 卡顿次数比率\n"
    sqlContext.sql(
      """
        |SELECT * FROM
        |    (SELECT row_number() OVER (PARTITION BY cdn ORDER BY lag_ratio DESC) AS row_number, * FROM
        |        (SELECT cdn, v2 AS cdn_ip, sum(v4) AS lag_count, count(v4) AS total_count, sum(v4)/count(v4) AS lag_ratio FROM quanmin_lag
        |        WHERE v2!='' GROUP BY cdn, v2) t
        |    WHERE total_count>3000) t
        |WHERE row_number<=10 ORDER BY instr('网宿百度腾讯阿里七牛云帆金山未定义', cdn), lag_ratio DESC
      """.stripMargin).
      collect().foreach((row: Row) => {
      tmpString += "%d, %s, %s, %d, %d, %f\n".format(row.getInt(0), row.getString(1), row.getString(2), row.getLong(3), row.getLong(4), row.getDouble(5))
    })
    attachmentStringsToSend.update("[%s]卡顿cdn_ip下行节点卡顿top10\n".format(yesterday), tmpString)

    var totalRatio: Double = 0
    tmpString = "本日平台总卡顿率,"
    sqlContext.sql("SELECT sum(v4)/count(*) AS lag_ratio FROM quanmin_lag WHERE tag='monitor' AND room_id!=-1 AND v5>1").
      collect().foreach((row: Row) => {
      tmpString += row.getDouble(0)
      totalRatio = row.getDouble(0)
    })
    attachmentStringsToSend.update("[%s]卡顿次数比率-平台总卡顿率\n".format(yesterday), tmpString)

    var htmlRows1 = ""
    tmpString = "cdn运营商, 卡顿次数比率\n"
    sqlContext.sql("SELECT cdn, sum(v4)/count(*) AS lag_ratio FROM quanmin_lag GROUP BY cdn ORDER BY instr('网宿百度腾讯阿里七牛云帆金山未定义', cdn)").
      collect().foreach((row: Row) => {
      tmpString += "%s, %f\n".format(row.getString(0), row.getDouble(1))
      htmlRows1 +=
        """
          |<tr>
          |  <td>%s</td>
          |  <td>%f</td>
          |</tr>
        """.stripMargin.format(row.getString(0), row.getDouble(1))
    })
    attachmentStringsToSend.update("[%s]卡顿次数比率-全天各厂商总卡顿率\n".format(yesterday), tmpString)

    var htmlRows2 = ""
    tmpString = "cdn厂商, 运营商, 卡顿次数比率\n"
    sqlContext.sql(
      """
        |SELECT cdn, isp, sum(v4)/count(*) AS lag_ratio FROM
        |    (SELECT cdn, CASE
        |        WHEN isp='电信' OR isp='移动' OR isp='联通' THEN '三大运营商'
        |        WHEN isp='教育网' THEN '教育网'
        |        ELSE '其他' END AS isp, v4
        |    FROM quanmin_lag) t
        |GROUP BY cdn, isp ORDER BY instr('三大运营商教育网其他', isp), instr('网宿百度腾讯阿里七牛云帆金山未定义', cdn)
      """.stripMargin).
      collect().foreach((row: Row) => {
      tmpString += "%s, %s, %f\n".format(row.getString(0), row.getString(1), row.getDouble(2))
      htmlRows2 +=
        """
          |<tr>
          |  <td>%s</td>
          |  <td>%s</td>
          |  <td>%f</td>
          |</tr>
        """.stripMargin.format(row.getString(0), row.getString(1), row.getDouble(2))
    })
    attachmentStringsToSend.update("[%s]卡顿次数比率-全天各运营商卡顿率".format(yesterday), tmpString)

    tmpString = "cdn, 平台, 卡顿率, 平台, 卡顿率, 平台, 卡顿率\n"
    var htmlRows3 = ""
    tmpArray1 = sqlContext.sql(
      """
        |SELECT cdn, sum(v4)/count(*) AS lag_ratio FROM
        |    (SELECT cdn, v4 FROM quanmin_lag WHERE platform=5 OR platform=14) t
        |GROUP BY cdn ORDER BY instr('网宿百度腾讯阿里七牛云帆金山未定义', cdn)
      """.stripMargin).collect()
    tmpArray2 = sqlContext.sql(
      """
        |SELECT cdn, sum(v4)/count(*) AS lag_ratio FROM
        |    (SELECT cdn, v4 FROM quanmin_lag WHERE platform=1) t
        |GROUP BY cdn ORDER BY instr('网宿百度腾讯阿里七牛云帆金山未定义', cdn)
      """.stripMargin).collect()
    tmpArray3 = sqlContext.sql(
      """
        |SELECT cdn, sum(v4)/count(*) AS lag_ratio FROM
        |    (SELECT cdn, v4 FROM quanmin_lag WHERE platform=2) t
        |GROUP BY cdn ORDER BY instr('网宿百度腾讯阿里七牛云帆金山未定义', cdn)
      """.stripMargin).collect()
    for(i <- tmpArray1.indices) {
      tmpString += "%s, %s, %f, %s, %f, %s, %f\n".format(tmpArray1(i).getString(0), "PC端", tmpArray1(i).getDouble(1), "Android端", tmpArray2(i).getDouble(1), "iOS端", tmpArray3(i).getDouble(1))
      htmlRows3 +=
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
//    attachmentStringsToSend.update("[%s]卡顿率-各家分平台卡顿率".format(yesterday), tmpString)

    tmpString = "cdn, 平台, 首屏时间, 平台, 首屏时间, 平台, 首屏时间\n"
    var htmlRows4 = ""
    tmpArray1 = sqlContext.sql(
      """
        |SELECT cdn, avg(v5) AS first FROM
        |    (SELECT cdn, v5 FROM quanmin_first WHERE platform=5 OR platform=14) t
        |GROUP BY cdn ORDER BY instr('网宿百度腾讯阿里七牛云帆金山未定义', cdn)
      """.stripMargin).collect()
    tmpArray2 = sqlContext.sql(
      """
        |SELECT cdn, avg(v5) AS first FROM
        |    (SELECT cdn, v5 FROM quanmin_first WHERE platform=1) t
        |GROUP BY cdn ORDER BY instr('网宿百度腾讯阿里七牛云帆金山未定义', cdn)
      """.stripMargin).collect()
    tmpArray3 = sqlContext.sql(
      """
        |SELECT cdn, avg(v5) AS first FROM
        |    (SELECT cdn, v5 FROM quanmin_first WHERE platform=2) t
        |GROUP BY cdn ORDER BY instr('网宿百度腾讯阿里七牛云帆金山未定义', cdn)
      """.stripMargin).collect()
    for(i <- tmpArray1.indices) {
      tmpString += "%s, %s, %f, %s, %f, %s, %f\n".format(tmpArray1(i).getString(0), "PC端", tmpArray1(i).getDouble(1), "Android端", tmpArray2(i).getDouble(1), "iOS端", tmpArray3(i).getDouble(1))
      htmlRows4 +=
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
//    attachmentStringsToSend.update("[%s]首屏数据".format(yesterday), tmpString)

    htmlTemplateString = htmlTemplateString.format(totalRatio, htmlRows1, htmlRows2, htmlRows3, htmlRows4)

    try {
      val email = new HtmlMultiPartEmail()
      email.setCharset("UTF-8")
      email.setHostName("smtp.sendcloud.net")
      email.setAuthentication("postmaster@apm.mail.qiniu.com", "gW6q6lbbiwFXEoyg")
      email.setFrom("no-reply@apm.mail.qiniu.com", "PILI-APM")
      email.addTo("fangchaochao@qmtv.com")
      email.addTo("dengyarong@qmtv.com")
      email.addTo("huangyisan@qmtv.com")
      email.addCc("zhangyunlong@qmtv.com")
      email.setSubject("[%s][全民TV]CDN质量数据日报".format(yesterday))
      email.setHtml(htmlTemplateString)
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
