package com.edward.spark.core.test

import java.io.ObjectOutputStream
import java.net.Socket

object Driver {

  def main(args: Array[String]): Unit = {

    //客户端，连接服务器

    val client = new Socket("localhost", 9999)

    val out = client.getOutputStream()
    val outObj = new ObjectOutputStream(out)

    val rdd = new RDD()
    val task = new Task()
    task.data = rdd.data
    task.logic = rdd.logic

    outObj.writeObject(task)

    outObj.flush()
    outObj.close()

    client.close()

  }

}
