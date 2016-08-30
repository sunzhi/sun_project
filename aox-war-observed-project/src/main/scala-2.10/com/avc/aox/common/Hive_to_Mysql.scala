package com.avc.aox.common

import java.util.Properties

import org.apache.spark.sql.DataFrame


/**
 * Created by dev on 16-4-21.
 */
object Hive_to_Mysql {

//  def hivecontext_sql(sql: String): DataFrame ={
//
//    val hiveContext = new HiveContext(sc)
//    val data = hiveContext.sql(sql)
//    data
//  }


  def hive_to_mysql(data: DataFrame,tableName: String):Unit= {
    try {
      //创建Properties存储数据库相关属性
      val prop = new Properties()
      prop.put("user", "root")
      prop.put("password", "new.1234")

      data.write.mode("append").jdbc("jdbc:mysql://192.168.100.200:3306/AVC_aox_war_observed", tableName, prop)
    } catch {
      case e: Exception => println("The Exception: " + e.printStackTrace())
    }






  }
}
