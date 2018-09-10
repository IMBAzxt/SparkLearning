package com.zhengxuetao


/**
  * Hello world!
  *
  */
object App {

	def main(args: Array[String]): Unit = {
		val p1 = new Phone(1, "小米")
		var p2 = new Phone(id = 2)
		var p3 = new Phone(3, "华为", "国产")
		var seq = Seq[Phone](p1, p2);

		val l1 = p3 +: seq
        //		l1.foreach(p => println(p.phone_id))

		//		val l2 = l1 ::: list //合并列表，往list头部加入l1
		//		l2.foreach(p => println(p.phone_id))
		//		val l3 = list :+ p3 //往列表末尾加入数据
		//		l3.foreach(p => println(p.phone_id))
		//        var a = Array("a", "w", "c", "d", "b")
		//        a.foreach(print)
	}
}
