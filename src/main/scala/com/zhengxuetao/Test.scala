package com.zhengxuetao

import org.apache.spark.{SparkConf, SparkContext}

object Test {
	def main(args: Array[String]): Unit = {
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
}
