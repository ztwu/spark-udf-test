package com.iflytek.edcc

import com.iflytek.edcc.util.{GroupSumFunction, MyPartitioner, MySort}
import org.apache.spark.sql.{Row, SQLContext, SaveMode}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object GroupSum {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local[4]")
    sparkConf.setAppName(this.getClass.getName)
    sparkConf.set("spark.hadoop.validateOutputSpecs", "false")

    //在spark2.0后hash-base shuffle废弃

    //其实 bucket 是一个广义的概念，代表 ShuffleMapTask 输出结果经过 partition 后要存放的地方，这里为了细化数据存放位置和数据名称，仅仅用 bucket 表示缓冲区。
    //ShuffleMapTask 的执行过程很简单：先利用 pipeline 计算得到 finalRDD 中对应 partition 的 records。每得到一个 record 就将其送到对应的 bucket 里，具体是哪个 bucket 由partitioner.partition(record.getKey()))决定。每个 bucket 里面的数据会不断被写到本地磁盘上，形成一个 ShuffleBlockFile，或者简称 FileSegment。之后的 reducer 会去 fetch 属于自己的 FileSegment，进入 shuffle read 阶段。

    //类似初始化mr环形缓冲区(maptask.MapOutputBuffer):hadoopd的环形缓冲区其实只是一个简单的buffer()

    //sort-base shuffle参考mapreduce的shuffle过程，map-spli-sort-combine-merge-reduce-sort-merge

    //Tungsten（钨丝计划）的第一方面：内存的优化
    //Tungsten是一个更底层的机制，可以认为它不存在，有点相当于JVM中的JIT功能。JVM的数据放在堆内或者堆外，把数据放在堆外的好处是
    //1：可以避免GC，
    //2：使用原生数据。磁盘上1G的数据，如果使用JVM加载进来的话要用比1G大的存储空间，可能是3G，总之会膨胀。

    //Tungsten的第二方面：CPU的优化
//    Tungsten有翻译方法的功能而且是面向Stage的翻译, Stage里面有很多的方法，如果Stage里面有5000个方法，并不会有5000次的方法调用，因为是函数式编程，会合并成为一个普通的代码块，如for循环。
//    寄存器：把数据放在寄存器里面会比放在内存里面读取更快，因为寄存器离CPU更近。

    //有三个可选项：hash、sort和tungsten-sort（优化内存）
//    sparkConf.set("spark.shuffle.manager","sort") //默认

    val sc = new SparkContext(sparkConf)

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val df = sc.textFile("D:\\project\\edu_edcc\\ztwu2\\temp\\spark-udf\\source\\")
      .map(x=>{
        val line = x.split("\t")
        val cityId = line(0)
        val schoolId = line(1)
        val userId = line(2)
        val cnt = line(3)
        (cityId,schoolId,userId,cnt)
      }).toDF("city_id","school_id","user_id","cnt")

    df.registerTempTable("test")
    sqlContext.udf.register("GroupSumFunction",new GroupSumFunction)

    val result = sqlContext.sql("select city_id,GroupSumFunction(user_id,cnt) as cnt from test group by city_id")

    implicit val st = new Ordering[(String,Int)](){
      override def compare(x: (String, Int), y: (String, Int)): Int = {
        var comp = 0
        if(x._1.hashCode != y._1.hashCode){
          comp = x._1.hashCode - y._1.hashCode
        }else {
          comp = y._2 - x._2
        }
        println("排序比较结果："+comp)
        comp
      }
    }

    val data = result
      .map(x=>{
        val cityId = x.getAs[String](0)
        val cnt = x.getAs[Int]("cnt")
        val random = new Random()
        val prefix = random.nextInt(10)
        (prefix+"###"+cityId,cnt)
      })
      .reduceByKey((x,y)=>{
        x+y
      }).map(x=>{
      val key = x._1.split("###")(1)
      val value = x._2
      (key,value)
    }).reduceByKey((a,b)=>{
      a+b
    })
      .partitionBy(new MyPartitioner(2))

        .flatMap(x=>{
          ArrayBuffer((x._1,70),(x._1,x._2))
        })

      //通过RangePartitioner来进行排序
      //Stage 0：Sample。创建 RangePartitioner，先对输入的数据的key做sampling来估算key的分布情况，
      // 然后按指定的排序切分出range，尽可能让每个partition对应的range里的key的数量均匀。
      // 计算出来的 rangeBounds 是一个长为 numPartitions - 1 的Array[TKey]，记录头 numPartitions - 1 个partition对应的range的上界；
      // 最后一个partition的边界就隐含在“剩余”当中。

      // Stage 1：Shuffle Write。开始shuffle，在map side做shuffle write，
      // 根据前面计算出的rangeBounds来重新partition。Shuffle write出的数据中，
      // 每个partition内的数据虽然尚未排序，但partition之间已经可以保证数据是按照partition index排序的了。

      //Stage 2：Shuffle Read。然后到reduce side，每个reducer再对拿到的本partition内的数据做排序。

      //shuffle的map和reduce阶段都存在排序
//      .map(x=>{
//        (new MySort(x._1,x._2),x)
//      }).sortByKey(true)
//      .map(x=>{
//        x._2
//      })

      //通过隐私变量implicit
      .sortBy(x=>x)

    //第二个shuffle参数传递一个true，这样会在重新分区过程中多一步shuffle，
    // 这意味着上游的分区可以并行运行
    //第二个参数shuffle=true，将会产生多于之前的分区数目
    //不经过shuffle，是无法将RDD的partition数变多的
//    println("coalesce分区：------------------------------------------------------------------------------------------")
//    val data1 = data.coalesce(5,true)
//    println(data1.getNumPartitions)

    //repartition 内部实现调用的 coalesce 且为coalesce中  shuffle = true的实现
//    println("repartition分区：---------------------------------------------------------------------------------------")
//    val data2 = data.repartition(2)
//    println(data2.getNumPartitions)

//    data2.saveAsTextFile("D:\\project\\edu_edcc\\ztwu2\\temp\\spark-udf\\result")

    data.foreach(println)
    println(data.getNumPartitions)

  }
}
