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
object LoginFailPlus {
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
      .process(new LoginFailPFPlus(2))

    res.print().setParallelism(1)

    //启动
    env.execute("LoginFail")
  }
}

class LoginFailPFPlus(maxFailTimes: Int) extends KeyedProcessFunction[Long, LoginEvent, LoginWarning] {

  //记录2s内连续登录失败的状态
  private lazy val failState: ListState[LoginEvent] = getRuntimeContext.getListState[LoginEvent](new ListStateDescriptor[LoginEvent]("fail-login", classOf[LoginEvent]))

  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, LoginWarning]#Context, out: Collector[LoginWarning]): Unit = {
    if (value.state == "fail") {

      //判断失败次数
      import scala.collection.JavaConversions._
      val failList = failState.get().iterator()
      if (failList.hasNext){
        val firstFail = failList.next()
        //判断时间差
        if (value.ts - firstFail.ts <= 2){
          out.collect(LoginWarning(value.userId,firstFail.ts,value.ts,"warning: fail times more than "+maxFailTimes+" in 2 seconds"))
        }
        //清空状态
        failState.clear()
        failState.add(value)
      }else{
        //清空状态
        failState.add(value)
      }
    }else{
      //清空状态
      failState.clear()
    }
  }


}

