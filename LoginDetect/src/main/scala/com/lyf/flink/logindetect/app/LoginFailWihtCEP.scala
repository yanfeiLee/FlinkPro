package com.lyf.flink.logindetect.app

import java.util

import com.lyf.flink.logindetect.beans.{LoginEvent, LoginWarning}
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Project: FlinkPro
  * Create by lyf3312 on 20/03/25 8:39
  * Version: 1.0
  */
object LoginFailWihtCEP {
  def main(args: Array[String]): Unit = {
    //创建环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val url = getClass.getResource("/LoginLog.csv")
    val source = env
      .readTextFile(url.getPath)
      .map(
        line => {
          val fields = line.split(",")
          LoginEvent(fields(0).toLong, fields(1), fields(2), fields(3).toLong)
        }
      ).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(3)) {
      override def extractTimestamp(element: LoginEvent): Long = {
        element.ts * 1000L
      }
    })

    //1.设置匹配规则
    val pattern = Pattern
      .begin[LoginEvent]("firstFail").where(_.state == "fail") // 第一次登录失败
      .next("secondFail").where(_.state == "fail") //连续两次登录失败
      .within(Time.seconds(2)) //2s之内
    //2.在输入流上应用pattern,得到patternStream
    val patternStream = CEP.pattern(source, pattern)
    //3.从PatternStream中取出匹配的事件流，
    val res = patternStream.select(new MatchStream())

    res.print()

    //启动
    env.execute("LoginFail1WihtCEP")
  }
}

class MatchStream() extends PatternSelectFunction[LoginEvent, LoginWarning] {
  override def select(map: util.Map[String, util.List[LoginEvent]]): LoginWarning = {
    //从map中获取各个体模式的匹配结果
    val firstFail = map.get("firstFail").get(0)
    val secondFail = map.get("secondFail").get(0)
    LoginWarning(firstFail.userId, firstFail.ts, secondFail.ts, "fail 2 times in 2 seconds")
  }
}
