package com.lyf.flink.marketing.beans

/**
  * Project: FlinkPro
  * Create by lyf3312 on 20/03/23 21:17
  * Version: 1.0
  */
case class MarketingUserBehavior(userId:String,
                                 behavior:String,
                                 channel:String,
                                 timestamp:Long
                                ) {

}
