package com.lyf.flink.marketing.app

import java.sql.Timestamp
import java.util.UUID

import com.lyf.flink.marketing.beans.{MarketingUserBehavior, MarketingViewCount}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

/**
  * Project: FlinkPro
  * Create by lyf3312 on 20/03/23 21:15
  * Version: 1.0
  */
object MarketingCountByChannel {
  def main(args: Array[String]): Unit = {
    //创建环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val source = env.addSource(new SimulateEventSource()).assignAscendingTimestamps(_.timestamp)

    val res = source.filter(_.behavior != "UNINSTALL")
      .keyBy(data => (data.channel, data.behavior))
      .timeWindow(Time.hours(1), Time.seconds(5))
      .process(new MarketingCountRes())

    res.print().setParallelism(1)
    //启动
    env.execute("MarketingCountByChannel")
  }
}

//自定义数据源
class SimulateEventSource() extends RichParallelSourceFunction[MarketingUserBehavior] {
  var running = true
  val rand = Random

  val channelSet: Seq[String] = Seq("AppStore", "XiaomiStore", "HuaweiStore", "weibo", "wechat", "tieba")
  val behaviorTypes: Seq[String] = Seq("BROWSE", "CLICK", "PURCHASE", "INSTALL", "UNINSTALL")

  override def cancel(): Unit = running = false

  override def run(ctx: SourceFunction.SourceContext[MarketingUserBehavior]): Unit = {
    val maxElements = Long.MaxValue
    var count = 1L
    while (running && count < maxElements) {
      val uid = UUID.randomUUID().toString
      val channel = channelSet(rand.nextInt(channelSet.size))
      val behavior = behaviorTypes(rand.nextInt(behaviorTypes.size))
      ctx.collect(MarketingUserBehavior(uid, behavior, channel, System.currentTimeMillis()))
      count += 1L
      Thread.sleep(500)
    }
  }
}

//自定义processWindowFunction
class MarketingCountRes() extends ProcessWindowFunction[MarketingUserBehavior, MarketingViewCount, (String, String), TimeWindow] {
  override def process(key: (String, String), context: Context, elements: Iterable[MarketingUserBehavior], out: Collector[MarketingViewCount]): Unit = {
    val window = context.window
    out.collect(MarketingViewCount(
      key._1,
      key._2,
      elements.size,
      new Timestamp(window.getStart).toString,
      new Timestamp(window.getEnd).toString
    ))
  }
}
