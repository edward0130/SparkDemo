package com.edward.spark.core.rdd.create

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_File {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("RDD")

    val sc = new SparkContext(conf)

    val rdd : RDD[String] = sc.textFile("datas/data1.txt", 3)

    //使用mapmapPartitionsWithIndex方法，查看分区存储的数据
    rdd.mapPartitionsWithIndex( (i, r) => {
      r.map((i,_))
    }).collect().foreach(println)
    rdd.collect().foreach(println)

    sc.stop()
  }

}
