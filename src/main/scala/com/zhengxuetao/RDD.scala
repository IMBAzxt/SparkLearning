package com.zhengxuetao

import org.apache.spark.{SparkConf, SparkContext}

class RDD {
	System.setProperty("hadoop.home.dir", "D:\\tools\\hadoop-common-2.2.0")
	val conf = new SparkConf().setAppName("test").setMaster("local")
	val sc = new SparkContext(conf)

	def testFilter(): Long = {
		val file = sc.textFile("src\\main\\resources\\test")
		val count = file.filter(line => line.contains("a")).count()
		return count
	}

	def testMap(): Array[Array[String]] = {
		val file = sc.textFile("src\\main\\resources\\testmap")
		return file.map(line => line.split(",")).collect()
	}
}
