package com.zhengxuetao

import java.text.SimpleDateFormat
import java.util.Date

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
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
		val conf = new SparkConf().setAppName("Kafka2Spark").setMaster("local[2]")
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
			rdd.map(x => (x, 1)).saveAsTextFile("/data/result/" + NowDate())
			km.updateZKOffsets(rdd)
		})
		ssc.start() //启动运行
		ssc.awaitTermination() //等待计算结束
		ssc.stop()
	}

	/**
	  * 只支持0.8及以下的版本
	  *
	  */
	//	def testZkUtilSaveOffset(group: String, topic: String): Unit = {
	//		val conf = new SparkConf().setAppName("Kafka2Spark").setMaster("local[4]")
	//		val ssc = new StreamingContext(conf, Seconds(5))
	//		val kafkaParam = scala.collection.immutable.Map[String, String](
	//			"metadata.broker.list" -> "dn1:9092,dn2:9092",
	//			//			"enable.auto.commit" -> "(false: java.lang.Boolean)",
	//			"auto.offset.reset" -> "smallest",
	//			"group.id" -> group)
	//
	//		//		val topic: String = "flumetest" //消费的 topic 名字
	//		val topics: Set[String] = Set(topic) //创建 stream 时使用的 topic 名字集合
	//		val topicDirs = new ZKGroupTopicDirs(group, topic) //创建一个 ZKGroupTopicDirs 对象，对保存
	//		//  val zkClient = new ZkClient(zkServers)
	//		// 必须要使用带有ZkSerializer参数的构造函数来构造，否则在之后使用ZkUtils的一些方法时会出错，而且在向zookeeper中写入数据时会导致乱码
	//		// org.I0Itec.zkclient.exception.ZkMarshallingError: java.io.StreamCorruptedException: invalid stream header: 7B227665
	//		val zkClient = new ZkClient("dn1:2181", Integer.MAX_VALUE, 10000, new MySerializer) //zookeeper 的host 和 ip，创建一个 client
	//		val children = zkClient.countChildren(s"${topicDirs.consumerOffsetDir}") //查询该路径下是否字节点（默认有字节点为我们自己保存不同 partition 时生成的）
	//		var kafkaStream: InputDStream[(String, String)] = null
	//		var fromOffsets: Map[TopicAndPartition, Long] = Map() //如果 zookeeper 中有保存 offset，我们会利用这个 offset 作为 kafkaStream 的起始位置
	//		if (children > 0) { //如果保存过 offset，这里更好的做法，还应该和  kafka 上最小的 offset 做对比，不然会报 OutOfRange 的错误
	//			for (i <- 0 until children) {
	//				val partitionOffset = zkClient.readData[String](s"${topicDirs.consumerOffsetDir}/${i}")
	//				val tp = TopicAndPartition(topic, i)
	//				fromOffsets += (tp -> partitionOffset.toLong) //将不同 partition 对应的 offset 增加到 fromOffsets 中
	//				println("@@@@@@ topic[" + topic + "] partition[" + i + "] offset[" + partitionOffset + "] @@@@@@")
	//			}
	//			val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message()) //这个会将 kafka 的消息进行 transform，最终 kafka 的数据都会变成 (topic_name, message) 这样的 tuple
	//			kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParam, fromOffsets, messageHandler)
	//		}
	//		else {
	//			kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParam, topics) //如果未保存，根据 kafkaParam 的配置使用最新或者最旧的 offset
	//		}
	//		var offsetRanges = Array[OffsetRange]()
	//		kafkaStream
	//				.transform { rdd =>
	//					offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges //得到该 rdd 对应 kafka 的消息的 offset
	//					rdd
	//				}
	//				.map(msg => msg._2)
	//				.foreachRDD { rdd =>
	//					for (o <- offsetRanges) {
	//						val zkPath = s"${topicDirs.consumerOffsetDir}/${o.partition}"
	//						//						val zkUtils = new ZkUtils(zkClient, new ZkConnection("dn1:2181"), JaasUtils.isZkSecurityEnabled)
	//						val zkUtils = ZkUtils.apply(zkClient, false)
	//						zkUtils.updatePersistentPath(zkPath, o.toString()) //将该 partition 的 offset 保存到 zookeeper
	//						println(s"@@@@@@ topic  ${o.topic}  partition ${o.partition}  fromoffset ${o.fromOffset}  untiloffset ${o.untilOffset} #######")
	//					}
	//					//sparkstreaming 写hdfs如果指定同一个文件，不同批次内容会覆盖。因此这里设置每个批次都是不一样的文件。
	//					rdd.map(x => (x, 1)).saveAsTextFile("hdfs://nn:8020/data/result/" + NowDate())
	//				}
	//		ssc.start() //启动运行
	//		ssc.awaitTermination() //等待计算结束
	//		ssc.stop()
	//	}

	def NowDate(): String = {
		val now: Date = new Date()
		val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
		val date = dateFormat.format(now)
		return date
	}

}
