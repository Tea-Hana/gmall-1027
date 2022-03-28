package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.UserInfo
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Author Hana
 * @Date 2022-03-28-15:21
 * @Description :
 */
object UserInfoApp {
  def main(args: Array[String]): Unit = {
    //1. 创建sparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("UserInfoApp")

    //2. 创建streamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    //3. 消费kafka的数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_USER_INFO, ssc)


    //4.将数据转换为数据类
    val userInfoDStream: DStream[UserInfo] = kafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        val userInfo: UserInfo = JSON.parseObject(record.value(), classOf[UserInfo])
        userInfo
      })
    })

    userInfoDStream.print()

    //关闭并阻塞流
    ssc.start()
    ssc.awaitTermination()
  }
}
