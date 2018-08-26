package com.zhengxuetao

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

class SparkStreamingTest {
	def testSparkStreaming(): Unit = {
		val conf = new SparkConf().setAppName("sparkstreaming").setMaster("local")
		val ssc = new StreamingContext(conf, Seconds(5))
		val lines = ssc.socketTextStream("localhost", 9999)
		val words = lines.flatMap(_.split(" "))

		val pairs = words.map(word => (word, 1))
		val wordCounts = pairs.reduceByKey(_ + _)
		wordCounts.print()

		ssc.start() //启动运行
		ssc.awaitTermination() //等待计算结束
		ssc.stop()
	}

}
