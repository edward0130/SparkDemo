package com.edward.spark.core.test

import java.io.ObjectInputStream
import java.net.ServerSocket

object Executor {

  def main(args: Array[String]): Unit = {

    //启动服务端

    val server = new ServerSocket(9999)
    val socket = server.accept()
    val input = socket.getInputStream
    val inputStream = new ObjectInputStream(input)

    val i = inputStream.readObject().asInstanceOf[Task]

    println("接收客户端发送过来的数据：" + i.compute())

    inputStream.close()
    input.close()
    socket.close()
    server.close()

  }

}
