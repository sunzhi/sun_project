package com.avcdata.wisdom.terminal

import com.avcdata.etl.streaming.template.StreamProcessTemplate
import org.apache.spark.streaming.StreamingContext

/**
 * Created by dev on 16-8-4.
 */
class WisdomTerminalProcess(batchDurationSeconds: Int, otherConfigrations: Map[String, String])
  extends StreamProcessTemplate(batchDurationSeconds: Int, otherConfigrations: Map[String, String])
{
  protected override def process(ssc: StreamingContext): Unit =
  {
    
  }
}
