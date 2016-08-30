package com.avcdata.wxdata.marketcompass.etl

import java.io.File
import java.util.{ArrayList, UUID}

import com.avcdata.config.config
import com.avcdata.core._
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext

/**
 * Created by Stuart Alex on 2015/11/11.
 */
object SementicAnalysisData {
  val logger = Logger.getLogger(this.getClass)
  val microsoftSQLServerHelper: MicrosoftSQLServerHelper = new MicrosoftSQLServerHelper(config.getString("data.source.semantic.url"),
    config.getString("data.source.semantic.database"), config.getString("data.source.semantic.username"),
    config.getString("data.source.semantic.password"))
  val db = config.getString("data.destination.hive.dbname")
  val destination = config.getString("data.destination.hive.sar")

  def start(sparkContext: SparkContext): Unit = {
    logger.warn("start extract-transform-load progress of SemanticAnalysing data")
    val hiveContext = new HiveContext(sparkContext)
    val existedDF = config.readFromHive(hiveContext, s"select week from $db.$destination group by week order by week")
    val existedList = new ArrayList[String]()
    existedDF.collect.foreach(row => existedList.add(row.getString(row.fieldIndex("week")).toUpperCase.trim))
    val havingRS = microsoftSQLServerHelper.getResultSet("select 周度 from 语义分析结果表 group by 周度 order by 周度", null)
    val meta = havingRS.getMetaData
    val havingList = new ArrayList[String]()
    while (havingRS.next)
      havingList.add(havingRS.getString("周度").toUpperCase.trim)
    logger.warn(s"start loading data from database $db ......")
    var sum = 0
    for (i <- 1 to havingList.size) {
      val week = havingList.get(i - 1)
      week match {
        case _ if existedList.contains(week) =>
        case _ => sum = sum + 1
      }
    }
    var present = 1;
    for (i <- 1 to havingList.size) {
      val week = havingList.get(i - 1)
      week match {
        case _ if existedList.contains(week) =>
        case _ =>
          config.printStage(sum, present, logger)
          present = present + 1
          val statement = s"select 周度,词语,指标,评论库位置,情感 from 语义分析结果表 where 周度='$week'"
          val resultSet = microsoftSQLServerHelper.getResultSet(statement, null)
          val name = UUID.randomUUID()
          val fileName = s"output/__user__hive__warehouse__${db}.db__${destination}__${UUID.randomUUID()}.csv"
          val file = new File(fileName)
          if (HDFSHelper.writeToFile(resultSet, file, false, WritingMode.ONCE_FOR_ALL, 0)) {
            HDFSHelper.upload(db, destination, file, true)
          }
      }
    }
    logger.warn("SemanticAnalysing data uploaded")
  }

}