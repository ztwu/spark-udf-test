package com.iflytek.edcc.util

import org.apache.spark.Partitioner

/**
  * Created with Intellij IDEA.
  * User: ztwu2
  * Date: 2018/1/8
  * Time: 15:56
  * Description 自定义分区
  */
class MyPartitioner(numParts : Int) extends Partitioner {

  //变量,分区数
  override def numPartitions: Int = numParts

  /**
    * 可以自定义分区算法
    *
    * @param key
    * @return
    * 这个函数需要对输入的key做计算，然后返回该key的分区ID，范围一定是0到numPartitions-1；
    * HashPartitioner分区的原理：对于给定的key，计算其hashCode，并除于分区的个数取余，如果余数小于0，则用余数+分区的个数，最后返回的值就是这个key所属的分区ID
    */
  override def getPartition(key: Any): Int = {
    val code = (key.hashCode % numPartitions)
    println("返回的分区号："+code)
    if (code < 0) {
      code + numPartitions
    } else {
      code
    }
  }

  //这个是Java标准的判断相等的函数，
  // 之所以要求用户实现这个函数是因为Spark内部会比较两个RDD的分区是否一样。
  override def equals(other: Any): Boolean = other match {
    case mypartition: MyPartitioner =>
      mypartition.numPartitions == numPartitions
    case _ =>
      false
  }

  override def hashCode(): Int = numPartitions

}

