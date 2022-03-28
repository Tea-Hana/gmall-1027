package com.atguigu.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bean.StartUpLog
import com.atguigu.constants.GmallConstants
import com.atguigu.handle.DauHandle
import com.atguigu.utils.MyKafkaUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._


object DauApp {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DauApp")
    //2.创建StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    //3.消费kafka数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP, ssc)

    //4. 将kafka消费过来的json字符串数据转为样例类，方便我们操作，并且补充两个时间字段
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
    val startUpLogDStream: DStream[StartUpLog] = kafkaDStream.mapPartitions(partitions => {
      partitions.map(record => {
        //利用fastjson将json字符串转为样例类
        val startUpLog: StartUpLog = JSON.parseObject(record.value(), classOf[StartUpLog])
        //补全时间段
        //times : yyyy-MM-dd HH
        val times: String = sdf.format(new Date(startUpLog.ts))
        startUpLog.logDate = times.split(" ")(0)
        startUpLog.logHour = times.split(" ")(1)
        startUpLog
      })
    })

   //打印原始数据的个数
    //当一个流多次使用时，可以通过缓存提高效率
    startUpLogDStream.cache()
    startUpLogDStream.count().print()

    //5. 批次间去重
    val filterByReidsDStream: DStream[StartUpLog] = DauHandle.filterByRedis(startUpLogDStream,ssc.sparkContext)


    //打印经过批次间去重后的数据个数
    filterByReidsDStream.cache()
    filterByReidsDStream.count().print( )

    //6. 批次内去重
    val filterByGroupDStream: DStream[StartUpLog] = DauHandle.filterByGroup(filterByReidsDStream)

    //打印经过批次内去重后的数据个数

    filterByGroupDStream.cache()
    filterByGroupDStream.count().print()

    //7. 将去重后的mid保存至Redis
    DauHandle.saveToRedis(filterByGroupDStream)

    //8. 把第六步去重后的明细数据保存至Hbase(写库 操作foreachRDD)
    filterByGroupDStream.foreachRDD( rdd =>{
      rdd.saveToPhoenix(
        "gmall1027_dau",
        Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),
        HBaseConfiguration.create(),
        Some("hadoop102,hadoop103,hadoop104:2181")
      )
    })


    //打印kafka中的数据来测试是否能消费到kafka的数据
    //     kafkaDStream.foreachRDD(rdd=>{
    //       rdd.foreach(record=>{
    //         println(record.value())
    //       })
    //       })

    //开启任务并阻塞
    ssc.start()
    ssc.awaitTermination()
  }

}
