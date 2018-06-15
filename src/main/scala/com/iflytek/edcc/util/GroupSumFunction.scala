package com.iflytek.edcc.util

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
  * Created with Intellij IDEA.
  * User: ztwu2
  * Date: 2018/1/8
  * Time: 11:39
  * Description 自定义聚合函数
  */
class GroupSumFunction extends UserDefinedAggregateFunction {

  /**
    * 该方法指定具体输入数据的类型
    * @return
    */
  override def inputSchema: StructType = {
    StructType(Array(StructField("key",StringType),StructField("value",IntegerType)))
  }

  /**
    * 在进行聚合操作的时候所要处理的数据的结果的类型
    * @return
    */
  override def bufferSchema: StructType = {
    StructType(Array(StructField("map",MapType(StringType,IntegerType)),StructField("value",IntegerType)))
  }

  /**
    * 指定UDAF函数计算后返回的结果类型
    * @return
    */
  override def dataType: DataType = {
    IntegerType
  }

  //用以标记针对给定的一组输入,UDAF是否总是生成相同的结果
  override def deterministic: Boolean = {
    true
  }

  /**
    * 在Aggregate之前每组数据的初始化结果
    * @param buffer
    */
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = null
    buffer(1) = 0
  }

  /**
    * 在进行聚合的时候，每当有新的值进来，对分组后的聚合如何进行计算
    * 本地的聚合操作，相当于Hadoop MapReduce模型中的Combiner
    * @param buffer
    * @param input
    */
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val key = input.getAs[String](0)
    val value = input.getAs[Int](1)
    println(key+"-------:-------"+value)

    val last = buffer.getMap[String,Int](0)
    println("Last : ---------------------------------------"+buffer(0))
    if(last != null){
      buffer(0) = last.+(key->value)
    }else {
      buffer(0) = Map(key->value)
      println("---------------------------------------"+buffer(0))
    }
  }

  /**
    * 最后在分布式节点进行Local Reduce完成后需要进行全局级别的Merge操作
    * @param buffer1
    * @param buffer2
    */
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val last = buffer1.getMap[String,Int](0)
    val curr = buffer2.getMap[String,Int](0)
    println("merge-last : ---------------------------------------"+last)
    println("merge-curr : ---------------------------------------"+curr)
    if(last!=null){
      buffer1(0) = last.++(curr)
    }else {
      buffer1(0) = curr
    }
  }

  /**
    * 返回UDAF最后的计算结果
    * @param buffer
    * @return
    */
  override def evaluate(buffer: Row): Any = {
    val data = buffer.getMap[String,Int](0)
    var result = buffer.getAs[Int](1)
    println("eval : ---------------------------------------"+data)
    if(data!=null){
      data.foreach(x=>{
        result += x._2
      })
    }
    result
  }
}
