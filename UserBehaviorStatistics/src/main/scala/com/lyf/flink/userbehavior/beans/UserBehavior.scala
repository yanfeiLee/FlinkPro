package com.lyf.flink.userbehavior.beans

/**
  * Project: FlinkPro
  * Create by lyf3312 on 20/03/20 17:13
  * Version: 1.0
  */
case class UserBehavior(
                       userId:Long,
                       itemId:Long,
                       categoryId:Int,
                       behavior: String,
                       timestamp:Long
                       ) {

}
