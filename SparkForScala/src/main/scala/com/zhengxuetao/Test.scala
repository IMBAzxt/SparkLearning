package com.zhengxuetao


object Test {
	def main(args: Array[String]): Unit = {
		if (args.length < 2) {
			System.err.println(
				"Params: \n" + "group topic"
			)
			System.exit(1)
		}
		val ss = new SparkStreamingTest()
		//		ss.testZkUtilSaveOffset(args(0).toString, args(1).toString)
		ss.testKafka2Spark(args(0).toString, args(1).toString);
	}
}
