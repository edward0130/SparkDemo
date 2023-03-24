package com.edward.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object RDD_FlatMap {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("MapPartition")

    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(List(1,2), List(3,4)),2)

    val rdd1 = sc.makeRDD(List("hello world","hello scala"))

    val rdd2 = sc.makeRDD(List(List(1,2), List(3,4),5,6))



    //扁平化处理
    val flatRdd = rdd.flatMap(list => list)

    //字符串拆分
    val flatRdd1 = rdd1.flatMap(str => {
      val result: Array[String] = str.split(" ")
      result
    })

    //不同类型拆分
    val flatRdd2 = rdd2.flatMap(data => {
      data match {
        case list: List[_] => list
        case dat: Int => List(dat)
      }
    })

    flatRdd.collect().foreach(println)
    flatRdd1.collect().foreach(println)
    flatRdd2.collect().foreach(println)

    sc.stop()
  }
}
