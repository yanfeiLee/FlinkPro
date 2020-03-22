package com.lyf.flink.networkflow.app

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.{LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter

import com.lyf.flink.networkflow.beans.{ApacheLog, PageViewCount}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * Project: FlinkPro
  * Create by lyf3312 on 20/03/21 9:46
  * Version: 1.0
  */
object PageViewTopN {
  def main(args: Array[String]): Unit = {
    //创建环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //val source = env.readTextFile("E:\\Dev\\Code\\idea\\ScalaCode\\FlinkPro\\NetWorkFlowStatistics\\src\\main\\resources\\apache.log")
    val source = env.socketTextStream("hadoop104", 3333)
    val LogStream = source.map(
      line => {
        val fields = line.split(" ")
        //格式化时间
        //利用java8 time 来格式化
        val formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy:HH:mm:ss")
        val ts = LocalDateTime.parse(fields(3), formatter).toInstant(ZoneOffset.ofHours(8)).toEpochMilli
        //利用simpleDateFormat格式化
        //        val format = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        //        val ts = format.parse(fields(3)).getTime

        ApacheLog(fields(0), fields(1), ts, fields(5), fields(6))
      }
    ).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLog](Time.seconds(1)) {
      override def extractTimestamp(element: ApacheLog): Long = {
        element.eventTime
      }
    })
    //过滤日志中的css,js，ico等请求
    val aggStream = LogStream
      //.filter(_.url.matches(".*(?<!css|ico|txt|js|jar|jpg|png|jpeg|gif|ttf|svg)$"))
      //另外一种正则过滤方式
      .filter(data => {
      val pattern = "^((?!\\.(css|js)$).)*$".r
      val maybeString = pattern findFirstIn data.url
      maybeString.nonEmpty
    })
      .keyBy(_.url)
      .timeWindow(Time.minutes(10), Time.seconds(10))
      //窗口延迟关闭
      .allowedLateness(Time.minutes(1))
      //窗口关闭后，迟到的数据输出到测输出流
      .sideOutputLateData(new OutputTag[ApacheLog]("late-pv"))
      .aggregate(new PVAgg(), new PVCountRes())

    //获取测输出流数据
    val sideStream = aggStream.getSideOutput(new OutputTag[ApacheLog]("late-pv"))

    //排名，取topN
    val res = aggStream.keyBy(_.windowEndTs)
      .process(new MyPVTopNFunction(3))


    //输出pageViewTopN排名
    LogStream.print("input").setParallelism(1)
    aggStream.print("agg").setParallelism(1)
    res.print("res").setParallelism(1)
    sideStream.print("side").setParallelism(1)

    //启动
    env.execute("PageViewTopN")
  }
}

class PVAgg() extends AggregateFunction[ApacheLog, Long, Long] {
  override def add(value: ApacheLog, accumulator: Long): Long = accumulator + 1

  override def createAccumulator(): Long = 0L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

class PVCountRes() extends WindowFunction[Long, PageViewCount, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[PageViewCount]): Unit = {
    val url = key
    val windowEndTs = window.getEnd
    val pvCount = input.iterator.next()
    out.collect(PageViewCount(url, pvCount, windowEndTs))
  }
}

//自定义processFunction
class MyPVTopNFunction(TopSize: Int) extends KeyedProcessFunction[Long, PageViewCount, String] {
  //  private lazy val pvCountListState: ListState[PageViewCount] = getRuntimeContext.getListState(new ListStateDescriptor[PageViewCount]("pvCount-list", classOf[PageViewCount]))

  //为避免迟到数据，多次加入list，造成重复，改用mapState
  private lazy val pvCountMapState: MapState[String, Long] = getRuntimeContext.getMapState(new MapStateDescriptor[String, Long]("pvCount-map", classOf[String], classOf[Long]))

  //记录窗口触发时间点用于清空状态
  private lazy val windowEndTimeState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("windowEnd-time", classOf[Long]))

  override def processElement(value: PageViewCount, ctx: KeyedProcessFunction[Long, PageViewCount, String]#Context, out: Collector[String]): Unit = {
    //触发关窗时间戳
    val windowCloseTs = value.windowEndTs + 1
    //更新状态list
    pvCountMapState.put(value.url, value.pvCount)
    //记录关窗事件点
    windowEndTimeState.update(windowCloseTs)
    //设定定时器
    ctx.timerService().registerEventTimeTimer(windowCloseTs)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PageViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    //排序取TopN
    //触发窗口关闭时的操作
    val pvCountListBuffer = new ListBuffer[(String, Long)]()
    val pvCountList = pvCountMapState.entries().iterator()
    while (pvCountList.hasNext) {
      val entry = pvCountList.next()
      pvCountListBuffer += ((entry.getKey, entry.getValue))
    }
    val topNList = pvCountListBuffer.sortBy(_._2)(Ordering.Long.reverse).take(TopSize)
    //循环输出topN
    val res = new StringBuilder
    res.append("关闭窗口：").append(new Timestamp(timestamp - 1)).append("\n")
    for (elem <- topNList.indices) {
      res.append("Top").append(elem + 1).append(": ")
        .append(topNList(elem)._2).append(" ").append(topNList(elem)._1)
        .append("\n")
    }
    res.append("----------------------------------")

    out.collect(res.toString())
    if (timestamp != windowEndTimeState.value()) {
      //窗口在延迟时间到达时，真正关闭窗口时的操作
      //清空状态
      pvCountMapState.clear()
      windowEndTimeState.clear()
    }
  }
}