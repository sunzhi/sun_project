package com.avcdata.wxdata.marketcompass.etl

import java.io.File
import java.util.{ArrayList, UUID}

import com.avcdata.config.config
import com.avcdata.core.{HDFSHelper, MicrosoftSQLServerHelper, WritingMode}
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext

/**
 * Created by Administrator on 2015/12/24.
 */
object CommentAndRating {
  val logger = Logger.getLogger(this.getClass)
  val microsoftSQLServerHelper: MicrosoftSQLServerHelper = new MicrosoftSQLServerHelper(config.getString("data.source.semantic.url"),
    config.getString("data.source.semantic.database"), config.getString("data.source.semantic.username"),
    config.getString("data.source.semantic.password"))
  val db = config.getString("data.destination.hive.dbname")
  val destination = config.getString("data.destination.hive.c")

  def start(sparkContext: SparkContext): Unit = {
    logger.warn("start extract-transform-load progress of OriginalComment & ComprihensiveRating")
    val hiveContext = new HiveContext(sparkContext)
    val existedDF = config.readFromHive(hiveContext, s"select week from $db.$destination group by week order by week")
    val existedList = new ArrayList[String]()
    existedDF.collect.foreach(row => existedList.add(row.getString(row.fieldIndex("week")).toUpperCase.trim))
    val havingRS = microsoftSQLServerHelper.getResultSet("select 周度 from 原始表 group by 周度 order by 周度", null)
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
          val statement =
            s"""select zzid,url,月度,周度,品类,品牌,机型,产品类型,电商,店铺名称,评论内容,评论人,评论日期,
               |case when 综合得分>=1 then 5 when 综合得分>0 then 4 when 综合得分=0 then 3 when 综合得分>=-1 then 2 else 1 end 综合评级 from
               |(select 评论库位置,sum(情感得分)/count(情感得分) 综合得分 from 语义分析结果表 where 周度='$week' group by 评论库位置) a,
                                                                                       |(select zzid,url,月度,upper(周度) 周度,品类,upper(品牌) 品牌,upper(机型) 机型,产品类型,电商,店铺名称,评论内容,评论人,评论日期 from 原始表 where 周度='$week') b
                                                                                                                                                                                                           |where a.评论库位置=b.zzid""".stripMargin
          val resultSet = microsoftSQLServerHelper.getResultSet(statement, null)
          val name = UUID.randomUUID()
          val fileName = s"output/__user__hive__warehouse__${db}.db__${destination}__${UUID.randomUUID()}.csv"
          val file = new File(fileName)
          if (HDFSHelper.writeToFile(resultSet, file, false, WritingMode.ONCE_FOR_ALL, 0)) {
            HDFSHelper.upload(db, destination, file, true)
          }
      }
    }
    logger.warn("OriginalComment & ComprehensiveRatingResult uploaded")
  }

}
