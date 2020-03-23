import java.util.UUID

/**
  * Project: FlinkPro
  * Create by lyf3312 on 20/03/23 21:25
  * Version: 1.0
  */
object uuidTest {
  def main(args: Array[String]): Unit = {
    val uuid = UUID.randomUUID().toString
    println(uuid)
  }
}
