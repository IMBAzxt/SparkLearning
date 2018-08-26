package com.zhengxuetao

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

class SparkStreamingTest {
    def testSparkStreaming(): Unit = {
        val conf = new SparkConf().setAppName("sparkstreaming").setMaster("local")
        val ssc = new StreamingContext(conf, Seconds(5))
        val lines = ssc.socketTextStream("localhost",9999)
        val words = lines.flatMap(_.split(" "))

        val pairs = words.map(word => (word,1))
        val wordCounts = pairs.reduceByKey(_ + _)
        wordCounts.print()

        ssc.start() //启动运行
        ssc.awaitTermination()  //等待计算结束
        ssc.stop()
    }

    def testKafka2Spark(): Unit = {
        val conf = new SparkConf().setAppName("Kafka2Spark").setMaster("local[2]")
        val ssc = new StreamingContext(conf, Seconds(5))
        //smallest : 自动把offset设为最小的offset；largest : 自动把offset设为最大的offset；
        val kafkaParams = scala.collection.immutable.Map[String, String](
            "metadata.broker.list" -> "dn1:9092,dn2:9092",
            "auto.offset.reset" -> "smallest",
            "group.id" -> "test")
        val topicsSet = Set("flume")
        val km = new KafkaManager(kafkaParams)
        val kafkaStream = km.createDirectStream[String, String, StringDecoder, StringDecoder](ssc,kafkaParams,topicsSet)
                .map(line => {
                    val arr = line.toString().split(" ")
                    (arr(0),arr(1))
                }).foreachRDD(rdd => {
                    if (!rdd.isEmpty)
                        km.updateZKOffsets(rdd)
                })

        ssc.start() //启动运行
        ssc.awaitTermination()  //等待计算结束
        ssc.stop()
    }

}
