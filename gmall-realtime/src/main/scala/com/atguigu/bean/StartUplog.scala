package com.atguigu.bean

/**
 * @Author Hana
 * @Date 2022-03-16-20:48
 * @Description :
 */
case class StartUpLog(mid:String,
                      uid:String,
                      appid:String,
                      area:String,
                      os:String,
                      ch:String,
                      `type`:String,
                      vs:String,
                      var logDate:String,
                      var logHour:String,
                      var ts:Long)
