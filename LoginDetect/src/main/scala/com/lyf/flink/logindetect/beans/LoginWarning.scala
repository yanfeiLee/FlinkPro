package com.lyf.flink.logindetect.beans

/**
  * Project: FlinkPro
  * Create by lyf3312 on 20/03/23 23:42
  * Version: 1.0
  */
case class LoginWarning (
                          userId: Long,
                          firstFailTime: Long,
                          lastFailTime: Long,
                          warningMsg: String){

}
