import java.io.File
import java.util.Properties

import com.databricks.spark.csv.CsvParser
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FunSuite

/**
 * Created by Administrator on 2015/12/16.
 */
class Spec extends FunSuite {

  test("csv") {
    val sparkConf = new SparkConf().setMaster("local").setAppName("wanxiangdata_app_etl")
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sparkContext)
    val path = "./reflect.csv"
    val file = new File(path)
    val abs = file.getAbsolutePath
    var cleanedAT = new CsvParser().withCharset("utf-8").withUseHeader(true).withDelimiter('\t').csvFile(sqlContext, abs).drop("zzid")
    cleanedAT.show()
  }

  test("mansong") {
    val current = 5998f
    val info = "满3000元，省300元"
    val rex1 = ".*\\d+.*\\d+.*"
    val rex2 = ".*\\d+.*"
    val rex3 = "\\d+"
    var real = current
    val matches = rex3.r.findAllIn(info).matchData.map(m => {
      m.group(0).toInt
    }).toSeq.toList
    matches.length match {
      case 0 =>
      case 1 => real = real - matches(0)
      case 2 => real = real - Math.round(real) / matches(0) * matches(1)
    }
    assert(real == 5698)
  }

  test("readhive") {
    val driverName: String = "org.apache.hive.jdbc.HiveDriver"
    Class.forName(driverName)
    val sparkConf = new SparkConf().setMaster("local").setAppName("aaa")
    val sparkContext = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sparkContext)
    val url = s"jdbc:hive2://nn.avcdata.com:26010"
    val properties = new Properties
    properties.put("url", url)
    properties.put("databasename", "wanxiangdata_competition")
    properties.put("username", "")
    properties.put("password", "")
    properties.put("useunicode", "true")
    properties.put("characterEncoding", "utf8")
    properties.put("hive.enable.spark.execution.engine","true")
    //    hiveContext.setConf(properties)
    hiveContext.read.jdbc(url,"datereflect",properties).registerTempTable("datereflect")
    hiveContext.sql("select * from datereflect").show()
    println("readhive ok")
    System.exit(0)
  }

}