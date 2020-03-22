import java.time.{LocalDate, LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter

/**
  * Project: FlinkPro
  * Create by lyf3312 on 20/03/21 18:09
  * Version: 1.0
  */
object TimeFormat {
  def main(args: Array[String]): Unit = {
    val accessor = DateTimeFormatter.ofPattern("dd/MM/yyyy:HH:mm:ss")
    val time = LocalDateTime.parse("17/05/2015:22:05:38",accessor)
    val milli = time.toInstant(ZoneOffset.of("+0")).toEpochMilli
    println(milli)
  }
}
