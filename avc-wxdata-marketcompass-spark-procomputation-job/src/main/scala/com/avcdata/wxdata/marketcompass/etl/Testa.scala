//package com.avcdata.wxdata.marketcompass.etl
//
//import org.apache.spark.{SparkConf, SparkContext}
////import org.apache.spark.sql.SQLContext
////import org.apache.spark.{SparkContext, SparkConf}
//import org.apache.spark.sql.hive.HiveContext
//
///**
// * Created by root on 15-11-16.
// */
//object Testa {
//
//  def start () {
//
//
//
//    val conf = new SparkConf().setAppName("192.168.100.201")
//    val sc = new SparkContext(conf)
//    val hiveContext = new HiveContext(sc)
//    import hiveContext.sql
//    val salesrecords = sql("FROM wanxiangdata_app.salesrecords SELECT * ")
//
//    print("start-------------------")
//    //    val conf = new SparkConf().setAppName("scalateste").setMaster("local")
//    //    val sc = new SparkContext(conf)
//    //
//    //    val sqlContext = new SQLContext(sc)
//    //
//    //
//    //    val salesrecords = sqlContext.csvFile("/mnt/shared/newresult.csv")
//
//
//    salesrecords.registerTempTable("salesrecords")
//
//
//    def ds_change(ec: String): String= {
//      var rt= "其他电商"
//      if (ec == "京东商城"){
//        rt = "专业电商"
//      }else if (ec == "天猫商城"){
//        rt = "天猫商城"
//      }else if (ec == "苏宁易购" || ec == "国美在线"){
//        rt = "垂直电商"
//      }
//      rt
//    }
//
//    hiveContext.udf.register("ds_change", (x:String) => ds_change(x))
//
//
//
//    val week_child = sql("select a.week from salesrecords a order by a.week desc limit 1")
//    val week_four_child = sql("select distinct(week) from salesrecords order by week desc limit 4")
//    val year_child = sql("SELECT b.subweek from (SELECT a.week, substring(a.week,1,3) subweek from salesrecords a order by week desc limit 1) b")
//    val week_child_list = week_child.collect()
//    val week_four_child_list = week_four_child.collect()
//    val year_child_list = year_child.collect()
//    def child_in(): String= {
//      var rt = ""
//      for(temp <- week_child_list){
//        rt = rt + temp(0)
//      }
//      rt
//    }
//    def four_in(): String= {
//      var str = ""
//      var rt = ""
//      for(temp <- week_four_child_list){
//        str = str + ",'" + temp(0) + "'"
//      }
//      rt = str.substring(1)
//      rt
//    }
//    def four_start(): String= {
//      var rt = ""
//      rt = week_four_child_list(3)(0).toString
////      println(rt)
//      rt
//    }
//    def four_end(): String= {
//      var rt = ""
//      rt = week_four_child_list(0)(0).toString
////      println(rt)
//      rt
//    }
//    def year_in(): String= {
//      var rt = ""
//      for(temp <- year_child_list){
//        rt = rt + temp(0)
//      }
//      rt
//    }
//
//
//    hiveContext.udf.register("child_in", () => child_in())
////    hiveContext.udf.register("four_in", () => four_in())
//    hiveContext.udf.register("year_in", () => year_in())
//
//    val year_start = sql("SELECT week FROM salesrecords WHERE substring(week,1,3) in (year_in()) order by week limit 1")
//    val year_end = sql("SELECT week FROM salesrecords WHERE substring(week,1,3) in (year_in()) order by week desc limit 1")
////    year_start.collect().foreach(println)
////    year_end.collect().foreach(println)
//    val year_start_list = year_start.collect()
//    val year_end_list = year_end.collect()
//
//    def year_startf(): String= {
//      var rt = ""
//      rt = year_start_list(0)(0).toString
//      rt
//    }
////    println(year_startf())
//    def year_endf(): String= {
//      var rt = ""
//      rt = year_end_list(0)(0).toString
//      rt
//    }
//
//    hiveContext.udf.register("four_start", () => four_start())
//    hiveContext.udf.register("four_end", () => four_end())
//    hiveContext.udf.register("year_startf", () => year_startf())
//    hiveContext.udf.register("year_endf", () => year_endf())
//
//    val week = sql("SELECT aa.category, '本周' weektype, child_in() startweek, child_in() endweek, aa.newec, aa.brand, aa.model, sum(aa.volume) volume_all, sum(aa.amount) amount_all from  " +
//                  "(SELECT ab.category, ab.brand, ab.model, ds_change(ab.ec) newec, ab.volume, ab.amount " +
//                  "from  salesrecords ab where ab.week in (child_in()) ) aa " +
//                  "group by aa.category,aa.newec,aa.brand,aa.model union all " +
//                  "SELECT bb.category, '本周' weektype, child_in() startweek, child_in() endweek, '全部电商' newec, bb.brand, bb.model, sum(bb.volume) volume_all, sum(bb.amount) amount_all from " +
//                  "(SELECT ac.category, ac.brand, ac.model, ac.volume, ac.amount from  salesrecords ac where ac.week in (child_in()) ) bb " +
//                  "group by bb.category,bb.brand,bb.model")
//////    week.collect().foreach(println)
//
//    val week_four = sql("SELECT aa.category, '近四周' weektype, four_start() startweek, four_end() endweek, aa.newec, aa.brand, aa.model, sum(aa.volume) volume_all, sum(aa.amount) amount_all from  " +
//                        "(SELECT ab.category, ab.brand, ab.model, ds_change(ab.ec) newec, ab.volume, ab.amount " +
//                        "from  salesrecords ab where ab.week in (" + four_in() + ") ) aa " +
//                        "group by aa.category,aa.newec,aa.brand,aa.model union all " +
//                        "SELECT bb.category, '近四周' weektype, four_start() startweek, four_end() endweek, '全部电商' newec, bb.brand, bb.model, sum(bb.volume) volume_all, sum(bb.amount) amount_all from " +
//                        "(SELECT ac.category, ac.brand, ac.model, ac.volume, ac.amount from  salesrecords ac where ac.week in ("+ four_in() + ") ) bb " +
//                        "group by bb.category,bb.brand,bb.model")
////    week_four.collect().foreach(println)
//
//    val year = sql("SELECT aa.category, '本年' weektype, year_startf() startweek, year_endf() endweek, aa.newec, aa.brand, aa.model, sum(aa.volume) volume_all, sum(aa.amount) amount_all from  " +
//                    "(SELECT ab.category, ab.brand, ab.model, ds_change(ab.ec) newec, ab.volume, ab.amount " +
//                    "from  salesrecords ab where substring(ab.week,1,3) in (year_in()) ) aa " +
//                    "group by aa.category,aa.newec,aa.brand,aa.model union all " +
//                    "SELECT bb.category, '本年' weektype, year_startf() startweek, year_endf() endweek, '全部电商' newec, bb.brand, bb.model, sum(bb.volume) volume_all, sum(bb.amount) amount_all from " +
//                    "(SELECT ac.category, ac.brand, ac.model, ac.volume, ac.amount from  salesrecords ac where substring(ac.week,1,3) in (year_in()) ) bb " +
//                    "group by bb.category,bb.brand,bb.model")
////    year.collect().foreach(println)
//
//
//
//
//    week.registerTempTable("week")
//
//    week_four.registerTempTable("week_four")
//    year.registerTempTable("year")
//    val all = sql("SELECT * from week union all SELECT * from week_four union all SELECT * from year")
//    all.registerTempTable("all")
//
//
//    sql("DROP TABLE wanxiangdata.marketcompass_popularmodel")
//    sql("create table wanxiangdata.marketcompass_popularmodel(category string, weektype string, startweek string, endweek string, ec string, brand string, model string,  volume Int, amount float,  avg Int)")
//    sql("INSERT INTO TABLE wanxiangdata.marketcompass_popularmodel SELECT category,  weektype, startweek, endweek, newec ec, brand, model, volume_all volume, (int(amount_all/10000 * 10)/10) amount, round(amount_all/volume_all) age FROM all")
//  }
//}
