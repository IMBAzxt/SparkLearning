package com.zhengxuetao


/**
 * Hello world!
 *
 */
object App {

  def main(args: Array[String]): Unit = {
      var p1 = new Phone(1, "小米")
      var p2 = new Phone(id = 2)
      var p3 = new Phone(3, "华为", "国产")
      var list = List[Phone](p1, p2);
      list.foreach(p => {
          if (p.phone_id == 1) {
              p.phone_name = "苹果"
              p.show()
          }
      })
      val l = p3 :: list
      l.foreach(p => print(p.phone_id))
  }
}
