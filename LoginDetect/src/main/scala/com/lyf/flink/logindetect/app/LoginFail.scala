package com.lyf.flink.logindetect.app

import com.lyf.flink.logindetect.beans.{LoginEvent, LoginWarning}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
  * Project: FlinkPro
  * Create by lyf3312 on 20/03/23 23:29
  * Version: 1.0
  * 同一用户2s内连续失败2次则输出报警信息
  */
object LoginFail {
  def main(args: Array[String]): Unit = {
    //创建环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val resource = getClass.getResource("/LoginLog.csv")
    val loginStream = env.readTextFile(resource.getPath)
      .map(
        line => {
          val fields = line.split(",")
          LoginEvent(fields(0).toLong, fields(1), fields(2), fields(3).toLong)
        }
      ).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(3)) {
      override def extractTimestamp(element: LoginEvent): Long = element.ts * 1000L
    })
    val res = loginStream.keyBy(_.userId)
      .process(new LoginFailPF(2))

    res.print().setParallelism(1)

    //启动
    env.execute("LoginFail")
  }
}

class LoginFailPF(maxFailTimes: Int) extends KeyedProcessFunction[Long, LoginEvent, LoginWarning] {

  //记录2s内连续登录失败的状态
  private lazy val failState: ListState[LoginEvent] = getRuntimeContext.getListState[LoginEvent](new ListStateDescriptor[LoginEvent]("fail-login", classOf[LoginEvent]))
  //2s后触发的定时器
  private lazy val timeState: ValueState[Long] = getRuntimeContext.getState[Long](new ValueStateDescriptor[Long]("timer", classOf[Long]))

  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, LoginWarning]#Context, out: Collector[LoginWarning]): Unit = {
    if (value.state == "fail") {
      //更新状态
      failState.add(value)
      //注册定时器
      if (timeState.value() == 0) {
        //注册一个2s后的定时器
        ctx.timerService().registerEventTimeTimer(value.ts * 1000L + 2000L)
      }
    } else {
      //清空状态
      failState.clear()
      ctx.timerService().deleteEventTimeTimer(timeState.value())
      timeState.clear()
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, LoginWarning]#OnTimerContext, out: Collector[LoginWarning]): Unit = {
    //查看2s内失败登录的次数
    import scala.collection.JavaConversions._
    val failList = failState.get().toList
    if(failList.length >= maxFailTimes){
      out.collect(LoginWarning(failList.head.userId,failList.head.ts,failList.last.ts,"warning:fail "+failList.size+" times in 2 seconds"))
    }

    //清空状态
    failState.clear()
    timeState.clear()
  }
}

