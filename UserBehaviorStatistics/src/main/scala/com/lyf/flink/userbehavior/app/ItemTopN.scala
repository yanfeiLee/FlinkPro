package com.lyf.flink.userbehavior.app

import java.lang
import java.sql.Timestamp
import java.util.Properties

import com.lyf.flink.userbehavior.beans.{ItemPVCount, UserBehavior}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

/**
  * Project: FlinkPro
  * Create by lyf3312 on 20/03/20 17:13
  * Version: 1.0
  * 商品热度TopN
  */
object ItemTopN {
  def main(args: Array[String]): Unit = {
    //创建环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //1.读取数据源
    //从csv读取文件
//    val source = env.readTextFile("E:\\Dev\\Code\\idea\\ScalaCode\\FlinkPro\\UserBehaviorStatistics\\src\\main\\resources\\UserBehavior.csv")
    //从kafka消费数据
    val props = new Properties()
    props.setProperty("bootstrap.servers","hadoop104:9092")
    props.setProperty("group.id","flink-pro")
    props.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("auto.offset.reset","latest")

    val source = env.addSource(new FlinkKafkaConsumer[String]("userBehavior",new SimpleStringSchema(),props))
    //转为文本数据为样例类UserBehavior，并设置watermark
    val userBehaviorStream = source.map(
      line => {
        val fields = line.split(",")
        UserBehavior(fields(0).trim.toLong, fields(1).trim.toLong, fields(2).trim.toInt, fields(3), fields(4).trim.toLong)
      }
    ).assignAscendingTimestamps(_.timestamp * 1000L)
    //2.对数据流进行transform，并进行开窗聚合
    val aggStream = userBehaviorStream.filter(_.behavior == "pv")
      .keyBy("itemId")
      .timeWindow(Time.hours(1), Time.minutes(5)) //窗口长度为1h,步长为5min的滑动窗口
      .aggregate(new ItemsAgg(), new CountWindowResult())

    //3.对聚合的结果按windowEndTs进行分组，排序获取TopN
    val res = aggStream.keyBy(_.windowEndTs)
      .process(new ResProcessFunction(5)) //自定义处理函数，到windowEndTs进行一次排序输出

    res.print().setParallelism(1)
    //启动
    env.execute("ItemTopNApp")
  }
}

//增量累加器
class ItemsAgg extends AggregateFunction[UserBehavior, Long, Long] {
  //聚合操作
  override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1L

  //初始化聚合值
  override def createAccumulator(): Long = 0L

  //返回聚合值
  override def getResult(accumulator: Long): Long = accumulator

  //合并各分区的聚合值
  override def merge(a: Long, b: Long): Long = a + b
}

//聚合累加器结果
class CountWindowResult extends WindowFunction[Long, ItemPVCount, Tuple, TimeWindow] {
  override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemPVCount]): Unit = {
    //输出聚合结果
    //将key的Tuple类型转为Long
    val itemId = key.asInstanceOf[Tuple1[Long]].f0
    val windowEndTs = window.getEnd
    val count = input.iterator.next()
    out.collect(ItemPVCount(itemId, count, windowEndTs))
  }
}

//自定义keyedProcessFunction
class ResProcessFunction(topSize:Int) extends KeyedProcessFunction[Long, ItemPVCount, String] {

  //定义状态保存数据结构
  private lazy val itemCountState: ListState[ItemPVCount] = getRuntimeContext.getListState(new ListStateDescriptor[ItemPVCount]("items-count",classOf[ItemPVCount]))

  override def processElement(value: ItemPVCount, ctx: KeyedProcessFunction[Long, ItemPVCount, String]#Context, out: Collector[String]): Unit = {

    //更新listState
    itemCountState.add(value)
    //注册定时器
    ctx.timerService().registerEventTimeTimer(value.windowEndTs+1) //设定在窗口关闭后下一毫秒触发定时器
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemPVCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    //对itemCount进行排序
    //itemCountState 转为list 进行排序
    import scala.collection.JavaConversions._
    val itemPVCountList = itemCountState.get().toList
    //清空状态
    itemCountState.clear()
    //取topN
    val topNList = itemPVCountList.sortBy(_.pvCount)(Ordering.Long.reverse).take(topSize)

    //输出结果
    val res = new StringBuffer()
    res.append("窗口关闭时间：").append(new Timestamp(timestamp-1)).append("\n")
    for (elem <- topNList.indices) {
      res.append("Top").append(elem+1).append(": ")
        .append("商品id=")
        .append(topNList(elem).itemId)
        .append(",PV值=")
        .append(topNList(elem).pvCount)
        .append("\n")
    }
    res.append("--------------------------------")
    out.collect(res.toString)


    Thread.sleep(1000L) //当从文件中读取时，为测试，设置隔1s输出一次

  }
}