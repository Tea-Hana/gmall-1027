package com.atguigu.bean

/**
 * @Author Hana
 * @Date 2022-03-28-10:02
 * @Description :
 */
case class CouponAlertInfo(mid:String,
                           uids:java.util.HashSet[String],
                           itemIds:java.util.HashSet[String],
                           events:java.util.List[String],
                           ts:Long)
