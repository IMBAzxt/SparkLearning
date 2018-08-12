package com.zhengxuetao

import org.apache.spark.sql.SparkSession
class SparkSqlTest {

    def testSparkSql(): Unit = {
        val sc = SparkSession
        .builder()
        .appName("SparkSql")
        .master("local")
        .config("spark.some.config.option", "some-value")
        .getOrCreate()

        //导入implicits，才能使用$
        import sc.implicits._
        val df = sc.read.json("src\\main\\resources\\testsql")
        //显示表
        df.show()
        //打印表字段
        df.printSchema()
        //显示name列
        df.select("name").show()
        //显示年龄大于6的数据
        df.filter($"age".gt(6)).show()
        //显示hadoop数据
        df.filter($"name".equalTo("hadoop")).show()
        //计算财产
        df.groupBy("name").sum("money").show()
        //计算age+10
        df.select(df.col("name"),df.col("age").plus(10)).show()
        //使用sql来查询
        df.createOrReplaceTempView("testsql")
        sc.sql("select * from testsql where id=1").show()
    }
}
