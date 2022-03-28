package com.atguigu.utils


import java.io.InputStreamReader
import java.util.Properties



/**
 * @Author Hana
 * @Date 2022-03-16-15:50
 * @Description :读取配置文件
 */
object PropertiesUtil {

  def load(propertieName:String): Properties ={
    val prop:Properties = new Properties()
    prop.load(new InputStreamReader(Thread.currentThread()
      .getContextClassLoader.getResourceAsStream(propertieName) , "UTF-8"))
    prop
  }
}
