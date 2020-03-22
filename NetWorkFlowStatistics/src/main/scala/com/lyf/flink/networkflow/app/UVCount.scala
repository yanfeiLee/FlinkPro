package com.lyf.flink.networkflow.app

import java.time.{LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter

import com.lyf.flink.networkflow.beans.UserBehavior
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * Project: FlinkPro
  * Create by lyf3312 on 20/03/22 20:52
  * Version: 1.0
  */
object UVCount {
  def main(args: Array[String]): Unit = {
    //创建环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val source = env.readTextFile("E:\\Dev\\Code\\idea\\ScalaCode\\FlinkPro\\NetWorkFlowStatistics\\src\\main\\resources\\UserBehavior.csv")
    val userBehaviorStream = source.map(
      line => {
        val fields = line.split(",")
        UserBehavior(fields(0).trim.toLong, fields(1).trim.toLong, fields(2).trim.toInt, fields(3).trim, fields(4).trim.toLong)
      }
    ).assignAscendingTimestamps(_.timestamp*1000L)

    val res = userBehaviorStream.filter(_.behavior == "pv")
      .timeWindowAll(Time.hours(1))
      .apply(new UVWindowCount())


    res.print().setParallelism(1)

    //启动
    env.execute("UVCount")
  }
}

class UVWindowCount() extends AllWindowFunction[UserBehavior,(String,Long),TimeWindow]{
  override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[(String, Long)]): Unit = {
     val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
     val time = LocalDateTime.ofEpochSecond(window.getEnd/1000,0,ZoneOffset.ofHours(8))
     val ts = formatter.format(time)
     val iterator = input.iterator
    //利用set进行去重
     var userSet = Set[Long]()
     while (iterator.hasNext){
       val uid = iterator.next().userId
       userSet += uid
     }

    out.collect((ts,userSet.size))
  }
}
