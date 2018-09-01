package com.zhengxuetao

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Hello world!
 *
 */
object App {
  def main(args: Array[String]): Unit = {
	System.setProperty("hadoop.home.dir", "D:\\tools\\hadoop-common-2.2.0")
	val conf = new SparkConf().setAppName("test").setMaster("local")
	val sc = new SparkContext(conf)
	sc.setLogLevel("Warn")
	val file = sc.textFile("d:\\test.txt")
	val rdd = file.map(line => line.split(","))
	  .map(x => ((x(0), x(1)), x(3))).groupByKey().sortByKey(false)
	  .map(x => (x._1._1 + "-" + x._1._2, x._2.toList.sortWith(_ > _)))
	rdd.foreach(
	  x => {
		val buf = new StringBuilder()
		for (a <- x._2) {
		  buf.append(a)
		  buf.append(",")
		}
		buf.deleteCharAt(buf.length() - 1)
		println(x._1 + " " + buf.toString())
	  })
	sc.stop()
  }
}
