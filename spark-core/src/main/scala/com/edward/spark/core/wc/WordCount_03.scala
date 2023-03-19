package com.edward.spark.core.wc

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object WordCount_03 {

  def main(args: Array[String]): Unit = {


    // 建立spark链接
    val conf = new SparkConf().setAppName("word count").setMaster("local[1]")
    val sc = new SparkContext(conf)

    val spark = SparkSession.builder().config(conf).getOrCreate()


    // 业务逻辑

    // 1 读取文件，一行一行读取

    val lines = sc.textFile("datas/data1.txt,datas/data2.txt")

    // 2 把一行文件的内容进行拆分
    val words = lines.flatMap(_.split(" ")).map(a => (a, 1))

    // 3 进行分组聚合
    val wordGroup = words.reduceByKey((x,y)=> x+y)

    // 4 打印输出
    wordGroup.collect().foreach(println)

    // 关闭链接
    sc.stop()
  }
}
