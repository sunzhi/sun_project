package com.avcdata.wisdomterminal

import com.avcdata.etl.streaming.launcher.StreamProcessLauncher
import org.junit.Assert._
import org.junit._

@Test
class AppTest {

    @Test
    def testOK() = assertTrue(true)

//    @Test
//    def testKO() = assertTrue(false)

    val args = Array[String]("--process-class"
      , "com.avcdata.wisdomterminal.WisdomTerminalProcess"
      , "--batch-duration-seconds"
      , "5"
      , "--param"
      , "redis.host=192.168.100.202"
      , "--param"
      , "redis.port=16379"
      , "--param"
      , "redis.listen.keys=comment_spider:test"
      , "--param"
      , "es.index.auto.create=true"
      , "--param"
      , "es.resource=evaluation/butler"
      , "--param"
      , "es.nodes=192.168.100.202"
      , "--param"
      , "es.port=9200"
      , "--param"
      , "es.input.json=false"
      , "--param"
      , "es.write.operation=upsert"
      , "--param"
      , "es.output.json=false"

//      , "--param"
//      , "es.nodes.discovery=false"
//      , "--param"
//      , "es.nodes.client.only=true"
    )

    StreamProcessLauncher.main(args)



}


