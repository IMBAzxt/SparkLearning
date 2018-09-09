package com.zhengxuetao

import com.zhengxuetao.KafkaCluster.Err
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

class SparkStreamingTest {
	def testSparkStreaming(): Unit = {
		val conf = new SparkConf().setAppName("sparkstreaming").setMaster("local[2]")
		val ssc = new StreamingContext(conf, Seconds(5))
		val lines = ssc.socketTextStream("192.168.1.212", 9999)
		val words = lines.flatMap(_.split(" "))

		val pairs = words.map(word => (word, 1))
		val wordCounts = pairs.reduceByKey(_ + _)
		wordCounts.print()

		ssc.start() //启动运行
		ssc.awaitTermination() //等待计算结束
		ssc.stop()
	}

	def testKafka2Spark(group: String, topic: String): Unit = {
		val conf = new SparkConf().setAppName("Kafka2Spark").setMaster("local[*]")
		val ssc = new StreamingContext(conf, Seconds(5))
		//smallest : 自动把offset设为最小的offset；largest : 自动把offset设为最大的offset；

		val kafkaParams = scala.collection.immutable.Map[String, String](
			"metadata.broker.list" -> "dn1:9092,dn2:9092",
			"auto.offset.reset" -> "smallest",
			"group.id" -> group)
		val topicsSet = Set(topic)
		val km = new KafkaManager(kafkaParams)
		val kafkaStream = km.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
		kafkaStream.foreachRDD(rdd => {
			rdd.map(x => (x, 1)).saveAsTextFile("/data/result/")
			km.updateZKOffsets(rdd)
		})
		ssc.start() //启动运行
		ssc.awaitTermination() //等待计算结束
		ssc.stop()
	}

	def testZkUtilSaveOffset(group: String, topic: String): Unit = {
		val conf = new SparkConf()
		//				.setAppName("Kafka2Spark").setMaster("local[4]")
		val ssc = new StreamingContext(conf, Seconds(5))
		ssc.sparkContext.setLogLevel("DEBUG")
		val kafkaParam = scala.collection.immutable.Map[String, String](
			"metadata.broker.list" -> "dn1:9092,dn2:9092",
			"enable.auto.commit" -> "(false: java.lang.Boolean)",
			"auto.offset.reset" -> "smallest",
			"group.id" -> group)

		val topics: Set[String] = Set(topic) //创建 stream 时使用的 topic 名字集合
		val topicDirs = new ZKGroupTopicDirs(group, topic) //创建一个 ZKGroupTopicDirs 对象，对保存
		val zkClient = new ZkClient("dn1:2181")

		val kc = new KafkaCluster(kafkaParam)
		val partitions: Either[Err, Set[TopicAndPartition]] = kc.getPartitions(topics)
		if (partitions.isRight) {
			val seq = partitions.right.get

			//如果 zookeeper 中有保存 offset，我们会利用这个 offset 作为 kafkaStream 的起始位置
			var fromOffsets: Map[TopicAndPartition, Long] = Map()
			//这个会将 kafka 的消息进行 transform，最终 kafka 的数据都会变成 (topic_name, message) 这样的 tuple
			val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())
			seq.foreach(p => {
				//查询该路径下是否字节点（默认有字节点为我们自己保存不同 partition 时生成的）
				val children = zkClient.countChildren(s"${topicDirs.consumerOffsetDir}")
				val tp = TopicAndPartition(topic, p.partition)
				if (children > 0) {
					try {
						val partitionOffset = zkClient.readData[String](s"${topicDirs.consumerOffsetDir}/${p.partition}")
						fromOffsets += (tp -> partitionOffset.toLong)
					} catch {
						case _: Exception => fromOffsets += (tp -> 0L)
					}
				} else {
					fromOffsets += (tp -> 0L)
				}
			})
			val kafkaStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](
				ssc,
				kafkaParam,
				fromOffsets,
				messageHandler
			)
			var offsetRanges = Array[OffsetRange]()
			kafkaStream
					.transform { rdd =>
						//得到该 rdd 对应 kafka 的消息的 offset
						offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
						rdd
					}
					.foreachRDD { rdd =>
						rdd.map(s => s).saveAsTextFile("hdfs://nn:8020/data/result")
						//								.saveAsHadoopFile("hdfs://nn:8020/data/result", classOf[String], classOf[String], classOf[TextOutputFormat[String, String]])
						for (o <- offsetRanges) {
							val zkPath = s"${topicDirs.consumerOffsetDir}/${o.partition}"
							//将该 partition 的 offset 保存到 zookeeper
							ZkUtils.updatePersistentPath(zkClient, zkPath, o.fromOffset.toString)
							println(s"topic:  ${o.topic}  partition: ${o.partition}  fromOffset: ${o.fromOffset}  untilOffset: ${o.untilOffset}")
						}
					}
			ssc.start() //启动运行
			ssc.awaitTermination() //等待计算结束
			ssc.stop()

		} else {
			println("topic not have any partition error")
		}
	}


}
