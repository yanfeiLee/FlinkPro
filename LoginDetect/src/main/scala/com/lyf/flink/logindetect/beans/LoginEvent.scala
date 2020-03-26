package com.lyf.flink.logindetect.beans

/**
  * Project: FlinkPro
  * Create by lyf3312 on 20/03/23 23:32
  * Version: 1.0
  */
case class LoginEvent (
                      userId:Long,
                      ip:String,
                      state:String,
                      ts:Long
                      ){

}
