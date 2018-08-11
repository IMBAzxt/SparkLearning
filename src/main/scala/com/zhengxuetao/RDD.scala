package com.zhengxuetao

import org.apache.spark.{SparkConf, SparkContext}

class RDD {
	System.setProperty("hadoop.home.dir", "D:\\tools\\hadoop-common-2.2.0")

	def testFilter(): Long = {
		val conf = new SparkConf().setAppName("test").setMaster("local")
		val sc = new SparkContext(conf)
		val file = sc.textFile("src\\main\\resources\\test")
		val count = file.filter(line => line.contains("a")).count()
		return count
	}

	def testMap(): Array[Array[String]] = {
		val conf = new SparkConf().setAppName("test").setMaster("local")
		val sc = new SparkContext(conf)
		val file = sc.textFile("src\\main\\resources\\testmap")
		return file.map(line => line.split(",")).collect()
	}

	def testFlatMap(): Unit = {
		val conf = new SparkConf().setAppName("test").setMaster("local")
		val sc = new SparkContext(conf)
		val file = sc.textFile("src\\main\\resources\\testmap")
		val rdd = file.flatMap(_.split(",")) collect()
		for (e <- rdd) {
			println(e)
		}
	}

	def testMapPartition(): Unit = {
		val conf = new SparkConf().setAppName("test").setMaster("local")
		val sc = new SparkContext(conf)
		val rdd = sc.makeRDD(1 to 20, 3)
		val rdd1 = rdd.mapPartitions(x => {
			var rs = List[Int]()
			var i = 0
			while (x.hasNext) {
				rs.::(x.next()).iterator
			}
			return rs
		})
	}
}
