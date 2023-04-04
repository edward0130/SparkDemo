package com.edward.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object RDD_GroupBy {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("groupby")
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List("Hello", "World", "Hi", "Scala"), 2)

    val rdd1 = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2)

    //分组，按照指定值进行分组处理，根据单词的首字母进行分组
    val groupRdd = rdd.groupBy(_.charAt(0))

    //分组，根据奇偶数进行分组
    val groupRdd1 = rdd1.groupBy(num => num % 2)

    groupRdd.collect().foreach(println)
    groupRdd1.collect().foreach(println)

    sc.stop()

  }

}
