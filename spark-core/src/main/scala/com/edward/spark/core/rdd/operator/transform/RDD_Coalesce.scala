package com.edward.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object RDD_Coalesce {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("Repartition")

    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5,6), 3)

    //重新分区, 减小分区格式，对分区的数据进行合并，分区内的数据不变
    //如果参数为shuffle=true,数据会打散重新分配
    val repRdd = rdd.coalesce(2)

    repRdd.glom().collect().foreach(data => println(data.mkString(",")))

    sc.stop()

  }

}
