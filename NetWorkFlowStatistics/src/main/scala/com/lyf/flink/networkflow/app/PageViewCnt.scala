package com.lyf.flink.networkflow.app

import java.text.SimpleDateFormat
import java.time.{LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter
import java.util.Date

import com.lyf.flink.networkflow.beans.UserBehavior
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * Project: FlinkPro
  * Create by lyf3312 on 20/03/22 19:58
  * Version: 1.0
  */
object PageViewCnt {
  def main(args: Array[String]): Unit = {
    //创建环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//    val source = env.socketTextStream("hadoop104", 3333)
    val source = env.readTextFile("E:\\Dev\\Code\\idea\\ScalaCode\\FlinkPro\\NetWorkFlowStatistics\\src\\main\\resources\\UserBehavior.csv")
    val userBehaviorStream = source.map(
      line => {
        val fields = line.split(",")
        UserBehavior(fields(0).trim.toLong, fields(1).trim.toLong, fields(2).trim.toInt, fields(3).trim, fields(4).trim.toLong)
      }
    ).assignTimestampsAndWatermarks(new AscendingTimestampExtractor[UserBehavior] {
      override def extractAscendingTimestamp(element: UserBehavior): Long = element.timestamp*1000L
    })
    val res = userBehaviorStream.filter(_.behavior == "pv")
      .map(data => (data.behavior, 1))
      .keyBy(_._1) //按照 dummy key进行分组，全部分到一个组内聚合
      .timeWindow(Time.hours(1))
//      .sum(1)
        .aggregate(new PVAgger(),new CountPVAgger())


    res.print().setParallelism(1)

     //启动
     env.execute("PageViewCount")
  }
}

class PVAgger() extends AggregateFunction[(String,Int),Long,Long]{
  override def add(value: (String, Int), accumulator: Long): Long = accumulator+1

  override def createAccumulator(): Long = 0L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a+b
}

class CountPVAgger() extends WindowFunction[Long,(String,Long),String,TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[(String,Long)]): Unit = {
//    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//    val ts = format.format(new Date(window.getEnd))
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    val time = LocalDateTime.ofEpochSecond(window.getEnd/1000,0,ZoneOffset.ofHours(8))
    val ts = formatter.format(time)
    out.collect((ts,input.iterator.next()))
  }
}

