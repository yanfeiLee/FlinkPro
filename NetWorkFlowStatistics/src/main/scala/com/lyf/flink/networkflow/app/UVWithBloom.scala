package com.lyf.flink.networkflow.app

import java.text.SimpleDateFormat
import java.util.Date

import com.lyf.flink.networkflow.beans.UserBehavior
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

/**
  * Project: FlinkPro
  * Create by lyf3312 on 20/03/22 21:35
  * Version: 1.0
  */
object UVWithBloom {
  def main(args: Array[String]): Unit = {
    //创建环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val resource = getClass.getResource("/UserBehavior.csv")
    val source = env.readTextFile(resource.getPath)
    val userBehaviorStream = source.map(
      line => {
        val fields = line.split(",")
        UserBehavior(fields(0).trim.toLong, fields(1).trim.toLong, fields(2).trim.toInt, fields(3).trim, fields(4).trim.toLong)
      }
    ).assignTimestampsAndWatermarks(new AscendingTimestampExtractor[UserBehavior] {
      override def extractAscendingTimestamp(element: UserBehavior): Long = element.timestamp * 1000L
    })

    val res = userBehaviorStream.filter(_.behavior == "pv")
      .map(record => ("dummykey", record.userId))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .trigger(new MyTrigger())
      .process(new UVCountPF())


    res.print().setParallelism(1)

    //启动
    env.execute("UVWithBloom")
  }
}

//布隆过滤器
class Bloom(size: Long) {
  // 内部属性，位图的大小，必须是 2^N
  private val cap = size

  // 定义hash函数，用于将一个String转化成一个offset
  def hash(value: String, seed: Int): Long = {
    var res: Long = 0L
    for (i <- 0 until  value.length) {
      res = res * seed + value.charAt(i)
    }

    // 返回offset值，需要让它在cap范围内
    res & (cap - 1)
  }
}

class MyTrigger() extends Trigger[(String, Long), TimeWindow] {
  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {}

  // 每来一条数据，就触发窗口计算，并且清空窗口状态
  override def onElement(element: (String, Long), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.FIRE_AND_PURGE
}

class UVCountPF() extends ProcessWindowFunction[(String, Long), (String, Long), String, TimeWindow] {
  lazy val jedis = new Jedis("hadoop104", 6379)
  //初始化位图大小 位图大小为约 10^9，由于需要2的整次幂，所以转换成了 2^30
  lazy val bloom = new Bloom(1 >> 32)

  override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[(String, Long)]): Unit = {
    // 存储方式：一个窗口对应一个位图，所以以windowEnd为 key，(windowEnd, bitmap)
    val field = context.window.getEnd.toString
    //由于使用了FIRE_AND_PURGE 触发器，将窗口的统计结果存入redis
    val hashName = "uvCount"
    var count = 0L
    //取出已存的值
    if (jedis.hget(hashName, field) != null) {
      count = jedis.hget(hashName, field).toLong
    }
    //更新值
    //判断当前uid是否出现在bitma中
    val uid = elements.last._2.toString
    val offset = bloom.hash(uid,61)
    val isExists = jedis.getbit(field,offset)
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    if (!isExists){
        //不存在，对应位置置为true，count+1
      jedis.setbit(field,offset,true)
      jedis.hset(hashName,field,(count+1).toString)
      //输出统计结果
      out.collect((format.format(new Date(field.toLong)),count+1))
    }

  }
}

