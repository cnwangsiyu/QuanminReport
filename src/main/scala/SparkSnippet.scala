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
        case 5 => "Web"
        case 14 => "iOS"
        case _ => "其他"
      }
    }

    tmpString = "cdn运营商, 终端类型, 卡顿总次数, 总请求数, 卡顿次数比率\n"
    sqlContext.sql("SELECT v1 AS cdn, platform, sum(v4) AS lag_count, count(v4) AS total_count, sum(v4)/count(v4) AS lag_ratio FROM quanmin WHERE (tag='monitor' AND v1!='') GROUP BY v1, platform ORDER BY cdn, platform").
      collect().foreach((row: Row) => {
      tmpString += "%s, %s, %d, %d, %f\n".format(row.getString(0), getDeviceName(row.getLong(1)), row.getLong(2),row.getLong(3), row.getDouble(4))
    })
    attachmentStringsToSend.update("[%s]全天卡顿次数比率".format(yesterday), tmpString)

    tmpString = "cdn运营商, 终端类型, 卡顿人数, 观看人数, 卡顿人数比率\n"
    sqlContext.sql("SELECT cdn, platform, sum(lag) AS lag_person, count(lag) AS total_person, sum(lag)/count(lag) AS lag_ratio FROM (SELECT v1 AS cdn, platform, uid, myOrAgg(v4) AS lag FROM quanmin WHERE (tag='monitor' AND v1!='') GROUP BY v1, platform, uid) lag_by_uid GROUP BY cdn, platform ORDER BY cdn, platform").
      collect().foreach((row: Row) => {
      tmpString += "%s, %s, %d, %d, %f\n".format(row.getString(0), getDeviceName(row.getLong(1)), row.getLong(2),row.getLong(3), row.getDouble(4))
    })
    attachmentStringsToSend.update("[%s]全天卡顿人数比率".format(yesterday), tmpString)

    tmpString = "cdn运营商, 终端类型, 卡顿总次数, 总请求数, 卡顿次数比率\n"
    sqlContext.sql("SELECT cdn, platform, sum(lag) AS lag_person, count(lag) AS total_person, sum(lag)/count(lag) AS lag_ratio FROM (SELECT v1 AS cdn, platform, uid, myOrAgg(v4) AS lag FROM quanmin WHERE (tag='monitor' AND v1!='' AND hour(time)>=19) GROUP BY v1, platform, uid) lag_by_uid GROUP BY cdn, platform ORDER BY cdn, platform").
      collect().foreach((row: Row) => {
      tmpString += "%s, %s, %d, %d, %f\n".format(row.getString(0), getDeviceName(row.getLong(1)), row.getLong(2),row.getLong(3), row.getDouble(4))
    })
    attachmentStringsToSend.update("[%s]晚高峰卡顿次数比率".format(yesterday), tmpString)

    tmpString = "cdn运营商, 终端类型, 卡顿人数, 观看人数, 卡顿人数比率\n"
    sqlContext.sql("SELECT cdn, platform, sum(lag) AS lag_person, count(lag) AS total_person, sum(lag)/count(lag) AS lag_ratio FROM (SELECT v1 AS cdn, platform, uid, myOrAgg(v4) AS lag FROM quanmin WHERE (tag='monitor' AND v1!='' AND hour(time)>=19) GROUP BY v1, platform, uid) lag_by_uid GROUP BY cdn, platform ORDER BY cdn, platform").
      collect().foreach((row: Row) => {
      tmpString += "%s, %s, %d, %d, %f\n".format(row.getString(0), getDeviceName(row.getLong(1)), row.getLong(2),row.getLong(3), row.getDouble(4))
    })
    attachmentStringsToSend.update("[%s]晚高峰卡顿人数比率".format(yesterday), tmpString)

    tmpString = "省份, cdn运营商, isp, 卡顿人数, 观看人数, 卡顿人数比率\n"
    sqlContext.sql("SELECT province, cdn, isp, sum(lag) AS lag_person, count(lag) AS total_person, sum(lag)/count(lag) AS lag_ratio FROM (SELECT province, v1 AS cdn, isp, uid, myOrAgg(v4) AS lag FROM quanmin WHERE (tag='monitor' AND v1!='') GROUP BY province, v1, isp, uid) lag_by_uid GROUP BY province, cdn, isp ORDER BY lag_person DESC LIMIT 5").
      collect().foreach((row: Row) => {
      tmpString += "%s, %s, %s, %d, %d, %f\n".format(row.getString(0), row.getString(1), row.getString(2), row.getLong(3),row.getLong(4), row.getDouble(5))
    })
    attachmentStringsToSend.update("[%s]省份卡顿用户数top5".format(yesterday), tmpString)

    tmpString = "cdn运营商, cdn_ip, 卡顿总次数, 总请求数, 卡顿次数比率\n"
    sqlContext.sql("SELECT v1 AS cdn, v2 AS cdn_ip, sum(v4) AS lag_count, count(v4) AS total_count, sum(v4)/count(v4) AS lag_ratio FROM quanmin WHERE (tag='monitor' AND v1!='' AND v2!='') GROUP BY v1, v2 ORDER BY lag_count DESC LIMIT 10").
      collect().foreach((row: Row) => {
      tmpString += "%s, %s, %d, %d, %f\n".format(row.getString(0), row.getString(1), row.getLong(2),row.getLong(3), row.getDouble(4))
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
