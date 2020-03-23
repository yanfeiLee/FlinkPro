package com.lyf.flink.marketing.beans

/**
  * Project: FlinkPro
  * Create by lyf3312 on 20/03/23 21:36
  * Version: 1.0
  */
case class MarketingViewCount (
                              channel:String,
                              behavior:String,
                              count:Long,
                              windowStart:String,
                              windowEnd:String
                              ){

}
