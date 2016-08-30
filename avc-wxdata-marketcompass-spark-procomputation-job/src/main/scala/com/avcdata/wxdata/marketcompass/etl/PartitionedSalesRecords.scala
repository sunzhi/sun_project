package com.avcdata.wxdata.marketcompass.etl

import java.io.File
import java.util.UUID

import com.avcdata.config.config
import com.avcdata.core.{HDFSHelper, HiveHelper, Transformer, WritingMode}
import com.avcdata.wxdata.marketcompass.JobStarter
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

/**
 * Created by Stuart Alex on 2015/12/24.
 */
object PartitionedSalesRecords {
  val logger = Logger.getLogger(this.getClass)
  val db = config.getString("data.destination.hive.dbname")
  val destination = config.getString("data.destination.hive.salesrecords")

  def start(sparkContext: SparkContext): Unit = {
    val sqlContext = new SQLContext(sparkContext)
    val condition = "name like '___线上周度%永久表____年'"
    //pts indicates "permanent tables"
    var pts = config.readFromMSSQLServer(sqlContext, config.getString("data.source.salesrecord.url"),
      config.getString("data.source.salesrecord.username"), config.getString("data.source.salesrecord.password"),
      config.getString("data.source.salesrecord.database"), "SYSOBJECTS").filter(condition).select("name", "xtype")
    val regex = "[A-Z]{2}_线上周度.+永久表\\d{3}[4-9]年"
    HiveHelper.truncate(config.getString("data.destination.hive.dbname"), config.getString("data.destination.hive.salesrecords"))
    val hiveContext = new HiveContext(sparkContext)
    var sum = 0
    pts.collect.foreach(row => {
      val name = row.getString(row.fieldIndex("name"))
      val xtype = row.getString(row.fieldIndex("xtype")).toLowerCase.trim
      if (name.matches(regex) && xtype.equals("u")) {
        sum = sum + 1
      }
    })
    var present = 1;
    pts.collect.foreach(row => {
      val name = row.getString(row.fieldIndex("name"))
      val category = name.substring(name.indexOf('度') + 1, name.indexOf('永'))
      val xtype = row.getString(row.fieldIndex("xtype")).toLowerCase.trim
      if (name.matches(regex) && xtype.equals("u")) {
        config.printStage(sum, present, logger)
        present = present + 1
        //        extract_byupload(sparkContext, name, category)
        extract_byinsert(hiveContext, name, category)
      }
    })
  }

  def extract_byupload(sparkContext: SparkContext, sourceTable: String, category: String): Unit = {
    val sqlContext = new SQLContext(sparkContext)
    val salesRecords = config.readFromMSSQLServer(sqlContext, config.getString("data.source.salesrecord.url"),
      config.getString("data.source.salesrecord.username"), config.getString("data.source.salesrecord.password"),
      config.getString("data.source.salesrecord.database"), sourceTable).filter("dataflag=2")
      .select("品牌", "机型", "机型编码", "电商", "周度", "单价", "销量", "销额")
    salesRecords.registerTempTable("salesrecords")
    var records = sqlContext.sql(s"select upper(品牌) 品牌,upper(机型) 机型,upper(机型编码) 机型编码,电商,周度,单价,sum(销量) 销量,sum(销额) 销额 from $destination group by upper(品牌),upper(机型),upper(机型编码),电商,周度,单价")
    val fileName = s"output/__user__hive__warehouse__${db}.db__${destination}__category=${category}__${UUID.randomUUID()}.csv"
    val file = new File(fileName)
    if (HDFSHelper.writeToFile(records, file, true, WritingMode.ONCE_FOR_ALL, 0)) {
      HDFSHelper.upload(db, destination, file, true)
    }
  }

  def extract_byinsert(hiveContext: HiveContext, sourceTable: String, category: String): Unit = {
    val salesRecords = config.readFromMSSQLServer(hiveContext, config.getString("data.source.salesrecord.url"),
      config.getString("data.source.salesrecord.username"), config.getString("data.source.salesrecord.password"),
      config.getString("data.source.salesrecord.database"), sourceTable).filter("dataflag=2")
      .select("品牌", "机型", "机型编码", "电商", "周度", "单价", "销量", "销额")
    salesRecords.registerTempTable(destination)
    if (JobStarter.env == "prd") {
      val fileName = s"output/__user__hive__warehouse__${db}.${destination}=${category}__${UUID.randomUUID()}.csv"
      val file = new File(fileName)
      HDFSHelper.writeToFile(salesRecords, file, true, WritingMode.ONCE_FOR_ALL, 0)
      HiveHelper.postForCDT(config.getString("services.queryservice"),
        s"alter table ${db}.${destination} add if not exists partition (category='${Transformer.hanyuToPinyin(category)}')")
    }
    val result = hiveContext.sql(s"insert into ${db}.${destination} partition(category='${Transformer.hanyuToPinyin(category)}') select * from ${destination}")
    //    var records = hiveContext.sql("select upper(品牌) 品牌,upper(机型) 机型,upper(机型编码) 机型编码,电商,周度,单价,sum(销量) 销量,sum(销额) 销额 " +
    //      "from salesrecords group by upper(品牌),upper(机型),upper(机型编码),电商,周度,单价")
    //    records.registerTempTable("records")
    //    val result = hiveContext.sql(s"insert into ${db}.${destination} partition(category='${Transformer.hanyuToPinyin(category)}') select * from records")
    hiveContext.dropTempTable("salesrecords")
  }

}