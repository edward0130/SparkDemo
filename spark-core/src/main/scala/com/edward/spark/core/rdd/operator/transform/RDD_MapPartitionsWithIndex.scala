package com.edward.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object RDD_MapPartitionsWithIndex {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("MapPartition")

    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(Seq(1, 2, 3, 4, 5),2)

    //把数据加上分区序号
    val mapRdd = rdd.mapPartitionsWithIndex((index, iter) => {
      iter.map((index, _))
    })

    //过滤分区，返回空迭代器
    val filterRdd = rdd.mapPartitionsWithIndex((index, iter) => {
      if( index == 1)
        iter
      else
        Nil.iterator //空
    })
    mapRdd.collect().foreach(println)

    sc.stop()
  }
}
