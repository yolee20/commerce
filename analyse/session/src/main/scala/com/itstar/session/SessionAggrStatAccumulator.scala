package com.itstar.session

import org.apache.spark.util.AccumulatorV2
import scala.collection.mutable

/**
  * 自定义累加器
  */
class SessionAggrStatAccumulator extends AccumulatorV2[String, mutable.HashMap[String, Int]] {

  // 保存所有聚合数据
  private val aggrStatMap = mutable.HashMap[String, Int]()

  override def isZero: Boolean = {
    aggrStatMap.isEmpty
  }

  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Int]] = {
    val newAcc = new SessionAggrStatAccumulator
    aggrStatMap.synchronized{
      /*   https://www.cnblogs.com/harvey888/p/6246471.html
       *  ::: 该方法只能用于连接两个List类型的集合
       *  ++ 该方法用于连接两个集合，list1++list2
       *  :: 该方法被称为cons，意为构造，向队列的头部追加数据，创造新的列表。
       *  :+方法用于在尾部追加元素
       *  +:方法用于在头部追加元素，和::很类似，但是::可以用于pattern match。
       *          +:和:+,只要记住冒号永远靠近集合类型就OK了。
       *
        * */
      newAcc.aggrStatMap ++= this.aggrStatMap
    }
    newAcc
  }

  override def reset(): Unit = {
    aggrStatMap.clear()
  }

  override def add(v: String): Unit = {
    if (!aggrStatMap.contains(v))
      aggrStatMap += (v -> 0)
    aggrStatMap.update(v, aggrStatMap(v) + 1)
  }

  /**
    * TODO：aggrStatMap中有，则+1；无，则设置0
    * @param other
    */
  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Int]]): Unit = {
    other match {
      case acc:SessionAggrStatAccumulator => {
        (this.aggrStatMap /: acc.value){
          case (map, (k,v)) => map += ( k -> (v + map.getOrElse(k, 0)) )
        }
      }
    }
  }

  override def value: mutable.HashMap[String, Int] = {
    this.aggrStatMap
  }
}
