package com.zhengxuetao

import org.apache.spark.{SparkConf, SparkContext}

class RDDLearning {
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

	def testMapReduce(): Unit = {
		val conf = new SparkConf().setAppName("test").setMaster("local")
		val sc = new SparkContext(conf)
		val file = sc.textFile("src\\main\\resources\\testmap")
		val rdd1 = file.map(line => line.split(",")(1).toInt)
		val rdd2 = rdd1.reduce((x,y) => {
			println(x + "---" + y )
			x + y
		})
		println(rdd2)
		sc.stop()
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
		//		val data = Array(1, 2, 3, 4, 5)
		//		val rdd = sc.parallelize(data)
		val rdd = sc.makeRDD(1 to 20, 3)
		val rdd1 = rdd.mapPartitions(x => {
			var rs = List[Int]()
			var i = 0
			while (x.hasNext) {
				//				i += x.next()
				rs = rs :+ x.next
			}
			rs.iterator
		})
		for (e <- rdd1) {
			println(e)
		}
	}

	def testMapPartitionsWithIndex(): Unit = {
		val conf = new SparkConf().setAppName("test").setMaster("local")
		val sc = new SparkContext(conf)
		//		val data = Array(1, 2, 3, 4, 5)
		//		val rdd = sc.parallelize(data)
		val rdd = sc.makeRDD(1 to 20, 3)
		val rdd1 = rdd.mapPartitionsWithIndex((a,x) => {
			var rs = List[String]()
			var i = 0
			while (x.hasNext) {
				//				i += x.next()
				val data : String = "partitions:" + a + " values:" + x.next
				rs = rs :+ data
			}
			rs.iterator
		})
		for (e <- rdd1) {
			println(e)
		}
	}

	def testSample(): Unit = {
		val conf = new SparkConf().setAppName("test").setMaster("local")
		val sc = new SparkContext(conf)
		val rdd = sc.makeRDD(1 to 20, 3)
		val rdd1 = rdd.sample(false,0.1,20)
		rdd1.foreach(println)
	}

	/**
	  * 两个数据集并集
	  */
	def testUnion(): Unit = {
		val conf = new SparkConf().setAppName("test").setMaster("local")
		val sc = new SparkContext(conf)
		val rdd1 = sc.makeRDD(1 to 20, 3)
		val rdd2 = sc.makeRDD(30 to 40,2)
		val rdd3= rdd1.union(rdd2)
		rdd3.foreach(println)
	}

	/**
	  * 求两个数据集交集
	  */
	def testIntersection(): Unit = {
		val conf = new SparkConf().setAppName("test").setMaster("local")
		val sc = new SparkContext(conf)
		val rdd1 = sc.makeRDD(1 to 20, 3)
		val rdd2 = sc.makeRDD(10 to 30,2)
		val rdd3= rdd1.intersection(rdd2)
		rdd3.foreach(println)
	}

	/**
	  * 去重，并按照key排序
	  */
	def testDistinct(): Unit = {
		val conf = new SparkConf().setAppName("test").setMaster("local")
		val sc = new SparkContext(conf)
		val rdd1 = sc.makeRDD(1 to 20, 3)
		val rdd2 = sc.makeRDD(10 to 30,2)
		val rdd3= rdd1.union(rdd2)
		rdd3.distinct().sortBy(x => x,true).foreach(println)
	}

	/**
	  * 汇聚
	  */
	def testGroupBy(): Unit = {
		val conf = new SparkConf().setAppName("test").setMaster("local")
		val sc = new SparkContext(conf)
		val rdd1 = sc.makeRDD(20 to 10, 3)
		rdd1.groupBy(x => x).foreach(println)
	}
}
