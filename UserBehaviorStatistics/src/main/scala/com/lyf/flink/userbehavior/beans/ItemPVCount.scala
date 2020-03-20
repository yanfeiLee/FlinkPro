package com.lyf.flink.userbehavior.beans

/**
  * Project: FlinkPro
  * Create by lyf3312 on 20/03/20 17:17
  * Version: 1.0
  * 商品pv统计
  */
case class ItemPVCount(
                      itemId:Long,
                      pvCount:Long,
                      windowEndTs:Long  //窗口关闭时间戳
                      ) {

}
