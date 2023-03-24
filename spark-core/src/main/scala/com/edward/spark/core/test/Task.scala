package com.edward.spark.core.test

class Task extends Serializable {

  var data:List[Int] = _

  var logic: Int=> Int = _

  def compute() ={
    data.map(logic)
  }
}
