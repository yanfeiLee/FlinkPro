package com.lyf.flink.marketing.app

import java.sql.Timestamp

import com.lyf.flink.marketing.beans.{AdClickBehavior, AdCountByProvince, BlackListWarning}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * Project: FlinkPro
  * Create by lyf3312 on 20/03/23 21:59
  * Version: 1.0
  */
object AdCount {
  def main(args: Array[String]): Unit = {
    //创建环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val sourceFile = getClass.getResource("/AdClickLog.csv")
    val source = env.readTextFile(sourceFile.getPath)
    val adSource = source.map(
      data => {
        val dataArray = data.split(",")
        AdClickBehavior(dataArray(0).toLong, dataArray(1).toLong, dataArray(2), dataArray(3), dataArray(4).toLong)
      }
    ).assignAscendingTimestamps(_.ts * 1000L)

    //过滤刷点击量用户，加入blackList
    val filterBlackListUserStream = adSource
      .keyBy(data => (data.userId, data.adId))
      .process(new BlackListUser(100)) //单日单用户，对某ad点击超过100，则加入blackList



    val res = filterBlackListUserStream.keyBy(_.province)
      .timeWindow(Time.hours(1), Time.seconds(5))
      .aggregate(new AdAgg(), new AdCountRes())

    //输出blackListUser
    val sideStream = filterBlackListUserStream.getSideOutput(new OutputTag[BlackListWarning]("blackList"))

    sideStream.print("warning").setParallelism(1)
    res.print().setParallelism(1)



    //启动
    env.execute("AdCountByProvince")
  }
}

//自定义增量聚合函数
class AdAgg() extends AggregateFunction[AdClickBehavior, Long, Long] {
  override def add(value: AdClickBehavior, accumulator: Long): Long = accumulator + 1

  override def createAccumulator(): Long = 0L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

//自定义窗口聚合函数
class AdCountRes() extends WindowFunction[Long, AdCountByProvince, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[AdCountByProvince]): Unit = {
    out.collect(AdCountByProvince(new Timestamp(window.getEnd).toString, key, input.last))
  }

}

class BlackListUser(maxClick: Int) extends KeyedProcessFunction[(Long, Long), AdClickBehavior, AdClickBehavior] {
  //定义状态
  //记录点击量
  private lazy val adCount: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("click-count", classOf[Long]))
  //记录是否加入blackList,避免重复添加
  private lazy val isAddBlackList: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("isAddBlackList", classOf[Boolean], false))
  //记录隔日重置黑名单定时器
  private lazy val timer: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer", classOf[Long]))

  override def processElement(value: AdClickBehavior, ctx: KeyedProcessFunction[(Long, Long), AdClickBehavior, AdClickBehavior]#Context, out: Collector[AdClickBehavior]): Unit = {
    //判断是否刷点击量
    val count = adCount.value()
    if (count == 0) {
      //第一次点击，注册定时器   设置次日零点清除定时器
      val ts = (ctx.timerService().currentProcessingTime() / (24 * 60 * 60 * 1000) + 1) * (24 * 60 * 60 * 1000)
      ctx.timerService().registerProcessingTimeTimer(ts)
      timer.update(ts)
    }
    if (count > maxClick) {
      //刷点击量
      //输出报警到侧输出流
      if (!isAddBlackList.value()) {
        //之前未输出
        ctx.output(new OutputTag[BlackListWarning]("blackList"), BlackListWarning(value.userId, value.adId, " warning: flush click and more than 100"))
        isAddBlackList.update(true)
      }
      return
    }
    //正常输出
    out.collect(value)
    adCount.update(count + 1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickBehavior, AdClickBehavior]#OnTimerContext, out: Collector[AdClickBehavior]): Unit = {
    if(timestamp == timer.value()){
      //清空所有状态
      adCount.clear()
      isAddBlackList.clear()
      timer.clear()
    }
  }
}

