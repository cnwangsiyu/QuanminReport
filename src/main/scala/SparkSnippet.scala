/**
  * Created by WangSiyu on 15/03/2017.
  */

import java.io.{File, FileOutputStream}
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.commons.mail.MultiPartEmail
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object SparkSnippet {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().
      setAppName("quanmin-report").
      setMaster("local[8]")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)

    sqlContext.udf.register("myOrAgg", new MyOrAgg)

    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    val yesterday = dateFormat.format(cal.getTime())
    val dataPath = "/Users/WangSiyu/Desktop/quanmin2/export_%s-*".format("2017-03-02-00")
    val quanminDataFrame = sqlContext.read.parquet(dataPath)
    quanminDataFrame.registerTempTable("quanmin")

    var attachmentStringsToSend: scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map[String, String]()
    var tmpString: String = ""

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

    tmpString = "序号, cdn运营商, 终端类型, 卡顿总次数, 总请求数, 卡顿次数比率\n"
    sqlContext.sql(
      """
        |SELECT row_number() OVER (ORDER BY v1, platform) AS row_number, v1 AS cdn, platform, sum(v4) AS lag_count, count(v4) AS total_count, sum(v4)/count(v4) AS lag_ratio FROM
        |    (SELECT tag, device, v4,
        |    CASE
        |        WHEN v1='bd' OR v1='baidu' THEN 'baidu'
        |        ELSE v1 END AS v1,
        |    CASE
        |        WHEN platform=14 THEN 5
        |        ELSE platform END AS platform
        |    FROM quanmin) t
        |WHERE (tag='monitor' AND v1!='') GROUP BY v1, platform
      """.stripMargin).
      collect().foreach((row: Row) => {
      tmpString += "%d, %s, %s, %d, %d, %f\n".format(row.getInt(0), row.getString(1), getDeviceName(row.getLong(2)), row.getLong(3), row.getLong(4), row.getDouble(5))
    })
    attachmentStringsToSend.update("[%s]全天卡顿次数比率".format(yesterday), tmpString)

    tmpString = "序号, cdn运营商, 终端类型, 卡顿人数, 观看人数, 卡顿人数比率\n"
    sqlContext.sql(
      """
        |SELECT row_number() OVER (ORDER BY cdn, platform) AS row_number, cdn, platform, sum(lag) AS lag_person, count(lag) AS total_person, sum(lag)/count(lag) AS lag_ratio FROM
        |    (SELECT v1 AS cdn, platform, device, myOrAgg(v4) AS lag FROM
        |        (SELECT tag, device, v4,
        |        CASE
        |            WHEN v1='bd' OR v1='baidu' THEN 'baidu'
        |            ELSE v1 END AS v1,
        |        CASE
        |            WHEN platform=14 THEN 5
        |            ELSE platform END AS platform
        |        FROM quanmin) t
        |    WHERE (tag='monitor' AND v1!='') GROUP BY v1, platform, device) t
        |GROUP BY cdn, platform
      """.stripMargin).
      collect().foreach((row: Row) => {
      tmpString += "%d, %s, %s, %d, %d, %f\n".format(row.getInt(0), row.getString(1), getDeviceName(row.getLong(2)), row.getLong(3), row.getLong(4), row.getDouble(5))
    })
    attachmentStringsToSend.update("[%s]全天卡顿人数比率".format(yesterday), tmpString)

    tmpString = "序号, cdn运营商, 终端类型, 卡顿总次数, 总请求数, 卡顿次数比率\n"
    sqlContext.sql(
      """
        |SELECT row_number() OVER (ORDER BY v1, platform) AS row_number, v1 AS cdn, platform, sum(v4) AS lag_count, count(v4) AS total_count, sum(v4)/count(v4) AS lag_ratio FROM
        |    (SELECT time, tag, device, v4,
        |    CASE
        |        WHEN v1='bd' OR v1='baidu' THEN 'baidu'
        |        ELSE v1 END AS v1,
        |    CASE
        |        WHEN platform=14 THEN 5
        |        ELSE platform END AS platform
        |    FROM quanmin) t
        |WHERE (tag='monitor' AND v1!='' AND hour(time)>=19) GROUP BY v1, platform
      """.stripMargin).
      collect().foreach((row: Row) => {
      tmpString += "%d, %s, %s, %d, %d, %f\n".format(row.getInt(0), row.getString(1), getDeviceName(row.getLong(2)), row.getLong(3), row.getLong(4), row.getDouble(5))
    })
    attachmentStringsToSend.update("[%s]晚高峰卡顿次数比率".format(yesterday), tmpString)

    tmpString = "序号, cdn运营商, 终端类型, 卡顿人数, 观看人数, 卡顿人数比率\n"
    sqlContext.sql(
      """
        |SELECT row_number() OVER (ORDER BY cdn, platform) AS row_number, cdn, platform, sum(lag) AS lag_person, count(lag) AS total_person, sum(lag)/count(lag) AS lag_ratio FROM
        |    (SELECT v1 AS cdn, platform, device, myOrAgg(v4) AS lag FROM
        |        (SELECT time, tag, device, v4,
        |        CASE
        |            WHEN v1='bd' OR v1='baidu' THEN 'baidu'
        |            ELSE v1 END AS v1,
        |        CASE
        |            WHEN platform=14 THEN 5
        |            ELSE platform END AS platform
        |        FROM quanmin) t
        |    WHERE (tag='monitor' AND v1!='' AND hour(time)>=19) GROUP BY v1, platform, device) t
        |GROUP BY cdn, platform
      """.stripMargin).
      collect().foreach((row: Row) => {
      tmpString += "%d, %s, %s, %d, %d, %f\n".format(row.getInt(0), row.getString(1), getDeviceName(row.getLong(2)), row.getLong(3), row.getLong(4), row.getDouble(5))
    })
    attachmentStringsToSend.update("[%s]晚高峰卡顿人数比率".format(yesterday), tmpString)

    tmpString = "序号, cdn运营商, isp, 省份, 卡顿人数, 观看人数, 卡顿人数比率\n"
    sqlContext.sql(
      """
        |SELECT * FROM
        |    (SELECT row_number() OVER (PARTITION BY cdn, isp ORDER BY lag_ratio DESC) AS row_number, * FROM
        |        (SELECT v1 AS cdn, isp, province, sum(v4) AS lag_count, count(v4) AS total_count, sum(v4)/count(v4) AS lag_ratio FROM
        |            (SELECT tag, province, country, device, v4,
        |            CASE
        |                WHEN v1='bd' OR v1='baidu' THEN 'baidu'
        |                ELSE v1 END AS v1,
        |            CASE
        |                WHEN isp='联通' OR isp='电信' OR isp='移动' OR isp='教育网' THEN isp
        |                ELSE '其他' END AS isp
        |            FROM quanmin) t
        |        WHERE (tag='monitor' AND v1!='' AND country='中国') GROUP BY province, v1, isp) t
        |    WHERE total_count>500) t
        |WHERE row_number<=5
      """.stripMargin).
      collect().foreach((row: Row) => {
      tmpString += "%d, %s, %s, %s, %d, %d, %f\n".format(row.getInt(0), row.getString(1), row.getString(2), row.getString(3), row.getLong(4), row.getLong(5), row.getDouble(6))
    })
    attachmentStringsToSend.update("[%s]省份卡顿次数top5".format(yesterday), tmpString)

    tmpString = "序号, cdn运营商, cdn_ip, 卡顿总次数, 总请求数, 卡顿次数比率\n"
    sqlContext.sql(
      """
        |SELECT * FROM
        |    (SELECT row_number() OVER (PARTITION BY cdn ORDER BY lag_ratio DESC) AS row_number, * FROM
        |        (SELECT v1 AS cdn, v2 AS cdn_ip, sum(v4) AS lag_count, count(v4) AS total_count, sum(v4)/count(v4) AS lag_ratio FROM
        |            (SELECT tag, v4, v2,
        |            CASE
        |                WHEN v1='bd' OR v1='baidu' THEN 'baidu'
        |                ELSE v1 END AS v1
        |            FROM quanmin) t
        |        WHERE (tag='monitor' AND v1!='' AND v2!='') GROUP BY v1, v2) t
        |    WHERE total_count>3000) t
        |WHERE row_number<=10
      """.stripMargin).
      collect().foreach((row: Row) => {
      tmpString += "%d, %s, %s, %d, %d, %f\n".format(row.getInt(0), row.getString(1), row.getString(2), row.getLong(3), row.getLong(4), row.getDouble(5))
    })
    attachmentStringsToSend.update("[%s]卡顿cdn_ip下行节点卡顿top10\n".format(yesterday), tmpString)

    try {
      val email = new MultiPartEmail()
      email.setCharset("UTF-8")
      email.setHostName("smtp.sendcloud.net")
      email.setAuthentication("postmaster@apm.mail.qiniu.com", "gW6q6lbbiwFXEoyg")
      email.setFrom("no-reply@apm.mail.qiniu.com", "PILI-APM")
      email.addTo("wangsiyu@qiniu.com")
      email.addTo("hzwangsiyu@163.com")
      email.setSubject("[%s][全民TV] PILI-APM 报表".format(yesterday))
      email.setMsg("全民直播报表")
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
