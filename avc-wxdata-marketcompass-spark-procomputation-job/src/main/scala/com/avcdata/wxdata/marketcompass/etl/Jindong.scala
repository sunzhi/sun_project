package com.avcdata.wxdata.marketcompass.etl

import com.databricks.spark.csv._
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by dev on 16-1-7.
 */
object Jindong {
//  def main(args: Array[String]) {
      def start (args: Array[String]){
    print("start-------------------")
    //    sparksql read csv

  var master="local"
   if(args.length>0)
     master = args(0)

    val conf = new SparkConf().setAppName("scalateste").setMaster(master)
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    //        val file = new File("/user/hdfs/JD/sunzhize/JD.csv")
    //    val salesrecords = sqlContext.csvFile("file://"+file.getAbsolutePath) /user/hdfs/JD/sunzhize/JD.csv
        val salesrecords = sqlContext.csvFile("/user/hdfs/JD/sunzhize/suntest.csv")
//    val salesrecords = sqlContext.csvFile("/mnt/shared/JDnew.csv")
    //    val hiveContext = new HiveContext(sc)
    //    split two table
    salesrecords.registerTempTable("tablea")
    var num = 0
    def addnum(): Int = {
      num = num + 1
      num
    }
    var numb = 0
    def addnumb(): Int = {
      numb = numb + 1
      numb
    }

    def rmnuma(numc: Int): Int = {
      num = num -1
      var n = 0
      n = numc - 1
      n
    }
    def rmnumb(numc: Int): Int = {
      numb = numb -1
      var n = 0
      n = numc - 1
      n
    }
    def computer(a1: Int, a2: String, a3: String, a4: String, a5: String, a6: String, a7: String,
                 b1: Int, b2: String, b3: String, b4: String, b5: String, b6: String, b7: String): Boolean = {
      //      println("a1=======" +a1)
      //      println("b1=======" +b1)
      if (a1 == b1) {
        return false
      } else {
        if (a2 != b2 || a3 != b3 || a4 != b4 || a5 != b5 || a6 != b6 || a7 != b7) {
          return false
        } else {
          if (a1 > b1) {
            return true
          } else {
            return false
          }
        }
      }
    }
    def mark(id1: Int, id2: Int, boolean: Boolean): Int = {
      if (boolean == true) {
        return id2
      } else {
        return id1
      }
    }
    def deal(id1: Int, id2: Int): String = {

      if (id2 == 0) {
        return "aowei" + id1
      } else {
        return "aowei" + id2
      }
    }
    def to_int(a: String): Int={
      var b = a.toInt
      b
    }
    def metch(a1: String, a2: String, a3: String, a4: String, a5: String, a6: String,
              b1: String, b2: String, b3: String, b4: String, b5: String, b6: String): Boolean={
      var count = 0
      if (a1 == b1 || b1 == "NA"){
        count = count + 1
      }

      if (a2 == b2 || b2 == "NA"){
        count = count + 1
      }
      if (a3 == b3 || b3 == "NA"){
        count = count + 1
      }
      if (a4 == b4 || b4 == "NA"){
        count = count + 1
      }
      if (a5 == b5 || b5 == "NA"){
        count = count + 1
      }
      if (a6 == b6 || b6 == "NA"){
        count = count + 1
      }

      if (count/6.0 >= 0.8){
        //        println("yes-----------------")
        return true
      }else {
        return false
      }
    }
    //    def
    sqlContext.udf.register("addnum", () => addnum())
    sqlContext.udf.register("to_int", (a: String) => to_int(a))
    sqlContext.udf.register("addnumb", () => addnumb())
    sqlContext.udf.register("rmnuma", (numc: Int) => rmnuma(numc))
    sqlContext.udf.register("rmnumb", (numc: Int) => rmnumb(numc))
    sqlContext.udf.register("computer", (a1: Int, a2: String, a3: String, a4: String, a5: String, a6: String, a7: String,
                                         b1: Int, b2: String, b3: String, b4: String, b5: String, b6: String, b7: String
                                          ) => computer(a1, a2, a3, a4, a5, a6, a7, b1, b2, b3, b4, b5, b6, b7))
    sqlContext.udf.register("mark", (id1: Int, id2: Int, boolean: Boolean) => mark(id1, id2, boolean))
    sqlContext.udf.register("deal", (id1: Int, id2: Int) => deal(id1, id2))
    sqlContext.udf.register("metch", (a1: String, a2: String, a3: String, a4: String, a5: String, a6: String,
                                      b1: String, b2: String, b3: String, b4: String, b5: String, b6: String)
    => metch(a1, a2, a3, a4, a5, a6,
        b1, b2, b3, b4, b5, b6))


    val tableb = sqlContext.sql("SELECT  a.ID id, a.品牌, a.衣长, a.袖长, a.图案, a.标牌价, " +
      "a.里料材质, a.货号, a.厚薄, a.领子, a.风格, a.促销价, a.衣门襟, a.品类2,a.urlID," +
      "a.URL,a.skuId,a.平台,a.商品名称,a.品牌ID,a.店铺,a.店铺ID,a.促销信息,a.返券,a.买赠,a.换购," +
      "a.让利,a.团购,a.积分,a.打折,a.累积评论数,a.商品评分,a.月成交记录,a.采集时间,a.材质成分," +
      "a.适用年龄,a.年份季节,a.颜色分类,a.尺码,a.销售渠道类型,a.服装版型,a.袖型,a.流行元素工艺," +
      "a.面料,a.通勤,a.里料图案,a.组合形式, '天猫商城' 来源平台 from tablea a ")
    tableb.registerTempTable("tablec")


    println("sun--------------------")
    //      sun.collect().foreach(println)
    //      sun.saveAsTextFile()

    val tabled = sqlContext.sql("SELECT c.aID, c.a品牌, c.a衣长, c.a袖长, c.a图案, c.a标牌价," +
      "c.a里料材质, c.a货号, c.a厚薄, c.a领子, c.a风格, c.a促销价, c.a衣门襟, c.bID, c.b品牌, c.b衣长, c.b袖长," +
      "c.b图案, c.b标牌价, c.b里料材质, c.b货号, c.b厚薄, c.b领子, c.b风格, c.b促销价, c.b衣门襟, c.boolean, mark(c.aID,c.bID,c.boolean) mark FROM " +
      "(SELECT a.id aID, a.品牌 a品牌, a.衣长 a衣长, a.袖长 a袖长, a.图案 a图案, a.标牌价 a标牌价," +
      "a.里料材质 a里料材质, a.货号 a货号, a.厚薄 a厚薄, a.领子 a领子, a.风格 a风格, a.促销价 a促销价, a.衣门襟 a衣门襟, b.id bID, b.品牌 b品牌, b.衣长 b衣长, b.袖长 b袖长," +
      "b.图案 b图案, b.标牌价 b标牌价, b.里料材质 b里料材质, b.货号 b货号, b.厚薄 b厚薄, b.领子 b领子, b.风格 b风格, b.促销价 b促销价, b.衣门襟 b衣门襟, " +
      "computer(a.id,a.品牌, a.衣长, a.袖长, a.图案, a.标牌价, a.衣门襟, b.id, b.品牌, b.衣长, b.袖长,b.图案, b.标牌价, b.衣门襟) boolean " +
      "FROM tablec a join tablec b) c  where boolean = true ")
    //    tabled.collect().foreach(println)
    println("OK---------------------------------")
    tabled.registerTempTable("tablee")
    val tablef = sqlContext.sql("SELECT k.aID, min(to_int(k.bID)) bID FROM (SELECT d.aID, d.bID, d.booleantype FROM " +
      "(SELECT c.aID, c.bID, metch(c.a里料材质, c.a货号, c.a厚薄, c.a领子, c.a风格, c.a促销价, " +
      "c.b里料材质, c.b货号, c.b厚薄, c.b领子, c.b风格, c.b促销价) booleantype FROM tablee c) d  where d.booleantype = true order by to_int(d.bID) ) k group by k.aID  ")

    println("sfsdf------------------------")
    //    tablef.rdd.repartition(1).saveAsTextFile("/home/dev/ttttt1")
    //    tablef.collect().foreach(println)
    println("bbbbbbbkkkkkkkk")
    tablef.registerTempTable("tablek")
    val all = sqlContext.sql("SELECT  a.id, a.品牌, a.衣长, a.袖长, a.图案, a.标牌价, " +
      "a.里料材质, a.货号, a.厚薄, a.领子, a.风格, a.促销价, a.衣门襟, a.品类2,a.urlID," +
      "a.URL,a.skuId,a.平台,a.商品名称,a.品牌ID,a.店铺,a.店铺ID,a.促销信息,a.返券,a.买赠," +
      "a.换购,a.让利,a.团购,a.积分,a.打折,a.累积评论数,a.商品评分,a.月成交记录,a.采集时间," +
      "a.材质成分,a.适用年龄,a.年份季节,a.颜色分类,a.尺码,a.销售渠道类型,a.服装版型,a.袖型," +
      "a.流行元素工艺,a.面料,a.通勤,a.里料图案,a.组合形式, deal(a.id, k.bID) aowei from tablec a LEFT JOIN " +
      " tablek k ON a.id = k.aID order by to_int(a.id)")
    //    all.collect().foreach(println)
    println("wait-------------------------")
    all.rdd.repartition(1).saveAsTextFile("/user/hdfs/JD/sunzhize/suntest")
    println("end---------------------------")

  }
}

