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
		sc.setLogLevel("ERROR")
		val file = sc.textFile("src\\main\\resources\\testmap")
		val a = file.map(line => {
            val arr = line.split(",")
            (arr(0), arr(1))
		}).collect()
		println(a)
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
	  * 跟reduceByKey一样的效果。增加了自定义的merge和combine
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

	/**
	  * 比aggregateByKey更低一层的接口，可以自定义输入的(k,v)值。其他的跟aggregateByKey一样
	  */
	def testCombineByKey(): Unit = {
		val conf = new SparkConf().setAppName("test").setMaster("local")
		val sc = new SparkContext(conf)
		val file = sc.textFile("src\\main\\resources\\testmap")

		type MVType = (Int, Int) //定义一个元组类型(科目计数器,分数)
		val v = file
				.map(line => {
					val arr = line.split(",")
					(arr(0), Integer.parseInt(arr(1)))
				})
				.combineByKey(
					score => (score, 1),
					(c1: MVType, v1) => (c1._1 + v1, c1._2 + 1),
					(c2: MVType, c3: MVType) => (c2._1 + c3._1, c2._2 + c3._2)
				).map(x => (x._1, x._2._1 / x._2._2)).foreach(println)
	}

	/**
	  * 排序
	  */
	def testSortByOrByKey(): Unit = {
		val conf = new SparkConf().setAppName("test").setMaster("local")
		val sc = new SparkContext(conf)
		val file = sc.textFile("src\\main\\resources\\testmap")
		file.map(line => {
			val arr = line.split(",")
			(arr(0), Integer.parseInt(arr(1)))
		})
				//				.sortBy(f => f._2, false)
				.sortByKey(false)
				.foreach(println)
	}

	/**
	  * 将RDD中元素前两个传给输入函数，产生一个新的return值，
	  * 新产生的return值与RDD中下一个元素（第三个元素）组成两个元素，
	  * 再被传给输入函数，直到最后只有一个值为止。
	  * mutable.Map.put如果key相同，值将被覆盖。
	  */
	def testMapReduce(): Unit = {
		val conf = new SparkConf().setAppName("test").setMaster("local")
		val sc = new SparkContext(conf)
		val file = sc.textFile("src\\main\\resources\\testmap")
		val result = file
                .map(line => {
                    val arr = line.split(",")
					(mutable.Map[String, Integer](), (arr(0), Integer.parseInt(arr(1))))
                })
				.reduce((x, y) => {
					var t1: mutable.Map[String, Integer] = x._1
					if (t1.isEmpty) {
						if (x._2._1 == y._2._1) {
							x._1.put(x._2._1, x._2._2 + y._2._2)
						} else {
							x._1.put(x._2._1, x._2._2)
							x._1.put(y._2._1, y._2._2)
						}
					} else {
						var t2 = t1.get(y._2._1);
						if (t2 == None) {
							x._1.put(y._2._1, y._2._2)
						} else {
							x._1.put(y._2._1, (y._2._2 + t2.get))
						}
					}
					x
				})
		result._1.foreach(println)
		sc.stop()
	}

	/**
	  * 将内容转化成list返回。
	  */
	def testFlatMap(): Unit = {
		val conf = new SparkConf().setAppName("test").setMaster("local")
		val sc = new SparkContext(conf)
		val file = sc.textFile("src\\main\\resources\\testmap")
		val rdd = file.flatMap(_.split(",")) collect()
		for (e <- rdd) {
			println(e)
		}
	}

	/**
	  * 遍历每一个partition,传入的值是一个partition的集合。
	  */
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

	/**
	  * 遍历每一个partition,传入的值是一个（partitionKey,Iterator[T]）。
	  */
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

	/**
	  * 采样
	  */
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

}
