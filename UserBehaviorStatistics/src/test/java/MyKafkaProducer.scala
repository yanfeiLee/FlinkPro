import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
  * Project: FlinkPro
  * Create by lyf3312 on 20/03/20 21:20
  * Version: 1.0
  */
object MyKafkaProducer {
  def main(args: Array[String]): Unit = {
    kafkaProducer("userBehavior")
  }
  def kafkaProducer(topic:String): Unit ={
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop104:9092")
    properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String,String](properties)

    //读取文件，逐条发送
    val txt = io.Source.fromFile("E:\\Dev\\Code\\idea\\ScalaCode\\FlinkPro\\UserBehaviorStatistics\\src\\main\\resources\\UserBehavior.csv")
    for (elem <- txt.getLines()) {

      val userBehavior = new ProducerRecord[String,String](topic,elem)
      producer.send(userBehavior)
    }

    //关闭资源
    producer.close()
  }
}
