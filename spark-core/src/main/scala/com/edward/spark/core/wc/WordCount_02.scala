package com.edward.spark.core.wc

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object WordCount_02 {

  def main(args: Array[String]): Unit = {


    // 建立spark链接
    val conf = new SparkConf().setAppName("word count").setMaster("local[1]")
    val sc = new SparkContext(conf)

    val spark = SparkSession.builder().config(conf).getOrCreate()


    // 业务逻辑

    // 1 读取文件，一行一行读取

    val lines = sc.textFile("datas/data1.txt")

    // 2 把一行文件的内容进行拆分
    val words = lines.flatMap(_.split(" ")).map(a => (a, 1))

    // 3 拆分后对数据进行分组
    val wordGroup = words.groupBy(word=>word._1)

    // 4 统计分组数据
    val value = wordGroup.map(a => a._2.reduce((x,y)=>(x._1 ,x._2 + y._2)))

    // 4 打印输出
    value.collect().foreach(println)

    // 关闭链接
    sc.stop()
  }
}
