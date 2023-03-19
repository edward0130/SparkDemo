package com.edward.spark.core.wc

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {


    // 建立spark链接
    val conf = new SparkConf().setAppName("word count").setMaster("local[1]")
    val sc = new SparkContext(conf)

    val spark = SparkSession.builder().config(conf).getOrCreate()


    // 业务逻辑

    // 1 读取文件，一行一行读取
    //  "hello world"
    // 读取文件的时候要明确文件路径，否则会找hadoop路径导致报错，依赖winutils.exe
    val lines = sc.textFile("datas/data1.txt")

//    val lines = spark.read.json("datas/data4.json").rdd
//    val lines = spark.read.text("datas/data1.txt").rdd

    // 2 把一行文件的内容进行拆分
    //  "hello world" = hello, world
    val words = lines.flatMap(_.split(" "))

    words.collect.foreach(println)

    // 3 拆分后对数据进行分组
    // (hello, hello), (world, world)
    val wordGroup = words.groupBy(word=>word)

    // 4 统计分组数据
    val value = wordGroup.map(a => (a._1, a._2.size))

    // 4 打印输出
    value.collect().foreach(println)

    // 关闭链接
    sc.stop()
  }
}
