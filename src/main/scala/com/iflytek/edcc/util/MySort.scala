package com.iflytek.edcc.util

/**
  * Created with Intellij IDEA.
  * User: ztwu2
  * Date: 2018/1/11
  * Time: 13:41
  * Description 自定义排序
  */
//Tuple2[String,Int]，scala语法，类似List<String>的泛型，scala泛型=》类型参数=》类型作为参数传递
//KVOrdering(first:String,second:Int),scala语法，主构造函数
//with trait 表示也具备两种特征,类似实现java接口，trait功能更像java的抽象类，有部分自己实现的方法
//extends 类似java类的继承
class MySort(val first:String, val second:Int) extends Ordered[MySort] with Serializable{

  /**
  *  自定义比较器,这种方式对于代码侵入性较强
    */
  override def compare(that: MySort): Int = {
    var comp = 0;
    if(this.first.hashCode - that.first.hashCode != 0)
    {
      ////按第一个比较字段升序
      comp = this.first.hashCode - that.first.hashCode
    }else {
      //按第二个比较字段降序
      comp = that.second - this.second
    }
    println("参数1 : "+this.first+" - "+this.second)
//    println("参数2 : "+that.first+" - "+that.second)
    println("排序比较结果："+comp)
    comp
  }

}