import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.commons.mail.HtmlEmail
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.{SparkConf, SparkContext}

class MyOrAgg extends UserDefinedAggregateFunction {
  override def inputSchema = new StructType().
    add("nums", IntegerType)

  override def bufferSchema = new StructType().
    add("result", IntegerType)

  override def dataType = IntegerType

  override def deterministic = true

  override def initialize(buffer: MutableAggregationBuffer) = {
    buffer(0) = 0
  }

  override def update(buffer: MutableAggregationBuffer, input: Row) = {
    buffer(0) = myOr(buffer.getInt(0), input.getInt(0))
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row) = {
    buffer1(0) = myOr(buffer1.getInt(0), buffer2.getInt(0))
  }

  override def evaluate(buffer: Row) = {
    buffer.getInt(0)
  }

  def myOr(num1: Integer, num2: Integer) = {
    if (num1 == 0 && num2 == 0) {
      0
    } else {
      1
    }
  }
}

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
    val dataPath = "/Users/WangSiyu/Desktop/quanmin2/export_%s-*".format(yesterday)
    val quanminDataFrame = sqlContext.read.parquet(dataPath)
    quanminDataFrame.registerTempTable("quanmin")

    val reportTemplate =
      """
        |<html>
        |   <body>
        |     %s
        |   </body>
        |</html>
      """.stripMargin

    val tableTemplate =
      """
        |<h4>%s</h4>
        |<table border="1">
        |   %s
        |</table>
      """.stripMargin
    var table = ""

    table += String.format(tableTemplate,
      "全天卡顿次数比率：",
      "<tr><td>cdn运营商</td><td>卡顿总次数</td><td>总请求数</td><td>卡顿次数比率</td></tr>%s")
    val lag_count = sqlContext.sql("SELECT v1 AS cdn, sum(v4) AS lag_count, count(v4) AS total_count, sum(v4)/count(v4) AS lag_ratio FROM quanmin WHERE tag = 'monitor' GROUP BY v1")
    var tableContent1 = ""
    lag_count.collect().foreach((row: Row) => {
      tableContent1 += "<tr><td>%s</td><td>%d</td><td>%d</td><td>%f</td></tr>".format(row.getString(0), row.getLong(1), row.getLong(2), row.getDouble(3))
    })
    table = table.format(tableContent1)

    table += tableTemplate.format("全天卡顿人数比率：",
      "<tr><td>cdn运营商</td><td>卡顿人数</td><td>观看人数</td><td>卡顿人数比率</td></tr>%s")
    val lag_person_count = sqlContext.sql("SELECT cdn, sum(lag) AS lag_person, count(lag) AS total_person, sum(lag)/count(lag) AS lag_ratio FROM (SELECT v1 AS cdn, uid, myOrAgg(v4) AS lag FROM quanmin WHERE tag='monitor' GROUP BY v1, uid) lag_by_uid GROUP BY cdn")
    var tableContent2 = ""
    lag_person_count.collect().foreach((row: Row) => {
      tableContent2 += "<tr><td>%s</td><td>%d</td><td>%d</td><td>%f</td></tr>".format(row.getString(0), row.getLong(1), row.getLong(2), row.getDouble(3))
    })
    table = table.format(tableContent2)

    try {
      val emailContent = reportTemplate.format(table)
      val email = new HtmlEmail()
      email.setCharset("UTF-8")
      email.setHostName("smtp.sendcloud.net")
      email.setAuthentication("postmaster@apm.mail.qiniu.com", "gW6q6lbbiwFXEoyg")
      email.setFrom("no-reply@apm.mail.qiniu.com", "PILI-APM")
      email.addTo("wangsiyu@qiniu.com")
      email.addTo("hzwangsiyu@163.com")
      email.setSubject("PILI-APM 报表[全民TV][%s]".format(yesterday))
      email.setHtmlMsg(emailContent)
      email.send()
    } catch {
      case e: Exception => println(e)
    }
  }
}
