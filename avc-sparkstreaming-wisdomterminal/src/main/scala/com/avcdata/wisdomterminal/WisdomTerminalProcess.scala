package com.avcdata.wisdomterminal


import com.avcdata.etl.streaming.template.StreamProcessTemplate
import org.apache.hadoop.hbase.client.{Put, Connection}
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.json.JSONObject

/**
 * Created by dev on 16-8-8.
 */
case class EvaluationButlerDetailsa(sun: String)
class WisdomTerminalProcess(batchDurationSeconds: Int, otherConfigrations: Map[String, String]) extends StreamProcessTemplate(batchDurationSeconds: Int, otherConfigrations: Map[String, String]){
  override def process(ssc: StreamingContext): Unit = {
    val parms = Map(
      "zookeeper.connect" -> "192.168.100.202:2181",
      "group.id" -> "wisdom-terminal",
      "zookeeper.connect.timeout" -> "30000"
    )

//    val stream = KafkaUtils.createStream(ssc,parms,Map("test" -> 1),StorageLevel.MEMORY_AND_DISK_SER)

    val hbasecontext = new HBaseContext(ssc.sparkContext,HBaseConfiguration.create())
//    读取的数据
    val stream = KafkaUtils.createStream(ssc, "192.168.100.202:2181","0", Map("test" -> 1), StorageLevel.MEMORY_AND_DISK).map(_._2)
    stream.foreachRDD(rdd=> {
      println("sun--------->")


//        rdd.foreach { record =>
//          val connection = createNewConnection()
//          connection.send(record)
//          connection.close()
//        }

      saveToHbase(hbasecontext,rdd)
//      x.collect().foreach(println)
    }
    )

    ssc.start()

    ssc.awaitTermination()
  }

  private def saveToHbase(hbasecontext: HBaseContext,rdd: RDD[String]): Unit ={

    hbasecontext.foreachPartition(rdd,(it: Iterator[String],conn: Connection) =>{

      val bufferedMutator = conn.getBufferedMutator(TableName.valueOf("test"))

      it.foreach(info=>
      {
        val infoData = new JSONObject(info)
        println("--------->" + infoData.get("sun"))
        val put = new Put(Bytes.toBytes(infoData.get("sun").toString))
        put.add(Bytes.toBytes("f"),Bytes.toBytes(infoData.get("sun").toString),Bytes.toBytes(infoData.get("sun").toString))
        bufferedMutator.mutate(put)
      }
      )
      bufferedMutator.flush()
      bufferedMutator.close()
    })
  }

}
