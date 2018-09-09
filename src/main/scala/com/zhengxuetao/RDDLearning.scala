package com.zhengxuetao

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

class RDDLearning {
	System.setProperty("hadoop.home.dir", "D:\\tools\\hadoop-common-2.2.0")

	def testFilter(): Long = {
		val conf = new SparkConf().setAppName("test").setMaster("local")
		val sc = new SparkContext(conf)
		val file = sc.textFile("src\\main\\resources\\test")
		val count = file.filter(line => line.contains("a")).count()
		return count
	}

	def testMap(): Unit = {
		val conf = new SparkConf().setAppName("test").setMaster("local")
		val sc = new SparkContext(conf)
		val file = sc.textFile("src\\main\\resources\\testmap")
		file.map(line => {
            val arr = line.split(",")
            (arr(0), arr(1))
        }).foreach(println)
    }

    /**
      * 将相同key的v值合并，如：(k,v1) (k,v2) => (k,v1v2)
      */
    def testReduceByKey(): Unit = {
        val conf = new SparkConf().setAppName("test").setMaster("local")
        val sc = new SparkContext(conf)
        val file = sc.textFile("src\\main\\resources\\testmap")
        file.map(line => {
            val arr = line.split(",")
            (arr(0), Integer.parseInt(arr(1)))
        })
                .reduceByKey((x, y) => {
                    x + y
                })
                //                .reduceByKey(_ + _)   //与上面方法的简写。
                .foreach(println)
    }

    /**
      * 将相同key的(k,v)分组到一个集合中，结果：(hadoop,CompactBuffer(1, 2))
      */
    def testGroupByKey(): Unit = {
        val conf = new SparkConf().setAppName("test").setMaster("local")
        val sc = new SparkContext(conf)
        val file = sc.textFile("src\\main\\resources\\testmap")
        file
                .map(line => {
                    val arr = line.split(",")
                    (arr(0), Integer.parseInt(arr(1)))
                })
                .groupByKey()
                .foreach(println)
    }

    /**
      * 将每个分区的数据进行聚合，然后再将所有分区的数据进行聚合。
      * zeroValue为初始结果，
      * seqOp将一个分区的数据T按照自定义规则合并后，加入到U中，
      * combOp拿到所有分区的U，再进行一次合并，并返回U
      */
    def testAggregate(): Unit = {
        val conf = new SparkConf().setAppName("test").setMaster("local")
        val sc = new SparkContext(conf)
        val file = sc.textFile("src\\main\\resources\\testmap")
        val v = file
                .map(line => {
                    val arr = line.split(",")
                    (arr(0), Integer.parseInt(arr(1)))
                })
                .aggregate(mutable.HashMap[String, Int]())((agg: mutable.HashMap[String, Int], data) => {
                    val temp = agg.get(data._1);
                    if (temp == None) {
                        agg.put(data._1, data._2)
                    } else {
                        agg.put(data._1, (data._2 + temp.get))
                    }
                    agg
                }, (agg1: mutable.HashMap[String, Int], agg2: mutable.HashMap[String, Int]) => {
                    for ((word, count) <- agg1) {
                        val temp = agg2.get(word);
                        if (temp == None) {
                            agg2.put(word, count)
                        } else {
                            agg2.put(word, temp.get + count)
                        }
                    }
                    agg2
                })
        println(v.toList)

    }

    /**
      * 跟reduceByKey一样的效果。性能上有什么区别？
      */
    def testAggregateByKey(): Unit = {
        val conf = new SparkConf().setAppName("test").setMaster("local")
        val sc = new SparkContext(conf)
        val file = sc.textFile("src\\main\\resources\\testmap")
        val v = file
                .map(line => {
                    val arr = line.split(",")
                    (arr(0), Integer.parseInt(arr(1)))
                })
                .aggregateByKey(0)((agg, data) => {
                    agg + data
                }, (_ + _)).foreach(println)
        //        println(v)

    }

	def testMapReduce(): Unit = {
		val conf = new SparkConf().setAppName("test").setMaster("local")
		val sc = new SparkContext(conf)
		val file = sc.textFile("src\\main\\resources\\testmap")
        file
                .map(line => {
                    val arr = line.split(",")
                    (arr(0), Integer.parseInt(arr(1)))
                })
        //                .reduce((x, y) => {
        //                    (x + y)
        //                })
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
