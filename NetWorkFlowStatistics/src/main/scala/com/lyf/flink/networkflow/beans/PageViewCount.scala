package com.lyf.flink.networkflow.beans

/**
  * Project: FlinkPro
  * Create by lyf3312 on 20/03/21 9:48
  * Version: 1.0
  */
case class PageViewCount (
                         url:String,
                         pvCount:Long,
                         windowEndTs:Long
                         ){

}
