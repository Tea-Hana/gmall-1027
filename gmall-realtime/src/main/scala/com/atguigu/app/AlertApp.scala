package com.atguigu.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{CouponAlertInfo, EventLog}
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.{MyEsUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import scala.util.control.Breaks._


/**
 * @Author Hana
 * @Date 2022-03-28-9:43
 * @Description :
 */
object AlertApp {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("AlertApp")

    //2.创建StreamingContext

    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    //3.从kafka消费数据

    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_EVENT, ssc)

    //4.将数据转为样例类,并返回K，V类型

    val sdf = new SimpleDateFormat("yyyy-MM-dd HH")
    val eventLogDStream: DStream[(String, EventLog)] = kafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        val eventLog: EventLog = JSON.parseObject(record.value(), classOf[EventLog])
        //补全字段
        val times: String = sdf.format(new Date(eventLog.ts))
        eventLog.logDate = times.split(" ")(0)
        eventLog.logHour = times.split(" ")(1)

        (eventLog.mid,eventLog)
      })
    })

    //5.开窗，开启一个5min的滑动窗口
    //如果窗口只传一个参数，则默认滑动步长为当前批次时机

    val windowDStream: DStream[(String, EventLog)] = eventLogDStream.window(Minutes(5))

    //6.分组聚合（mid）
    //迭代器中放的就是5分钟内相同mid的数据

    val midToEventLogDStream: DStream[(String, Iterable[EventLog])] = eventLogDStream.groupByKey()

    //7.根据条件筛选数据,并生成疑似预警日志

    val boolToCouponAlerInfoDStream: DStream[(Boolean, CouponAlertInfo)] = midToEventLogDStream.mapPartitions(partition => {
      partition.map { case (mid, iter) =>
        //创建set集合用来存放用户id
        val uids: util.HashSet[String] = new util.HashSet[String]()

        //创建set集合用来存放领优惠卷所涉及的商品
        val itemIds: util.HashSet[String] = new util.HashSet[String]()

        //创建list集合用来存放用户所涉及的事件
        val events: util.ArrayList[String] = new util.ArrayList[String]()

        //创建一个标志位，用来判断用户是否有浏览商品行为
        //var bool: Boolean = true

        //遍历迭代器，获取每一个数据
        breakable {
          iter.foreach(log => {
            //添加用户涉及行为
            events.add(log.evid)

            //判断用户是否有浏览商品行为
            if ("clickItem".equals(log.evid)) {
              //一旦有浏览商品行为，则证明此5分钟内的数据不符合预警要求，则跳出循环
              //bool = false
              //另一种方式 ： 集合置空 uids.clear()
              uids.clear()
              break()
            } else if ("coupon".equals(log.evid)) {
              //用户没有浏览商品但是领优惠券了
              //需要把符合要求的用户放入set集合中去重，后续通过集合的长度判断是否符合预警要求
              uids.add(log.uid)
              //添加涉及商品的id
              itemIds.add(log.itemid)
            }
          })
        }
        //返回疑是预警日志 K（放的是用来判断是否是预警日志的条件），V（可能是预警日志的数据）
        (uids.size() >= 3 , CouponAlertInfo(mid, uids, itemIds, events, System.currentTimeMillis()))
      }
    })

    //8.生成预警日志
    val couponAlterInfoDStream: DStream[CouponAlertInfo] = boolToCouponAlerInfoDStream.filter(_._1).map(_._2)

    couponAlterInfoDStream.print()

    //9.将日志写入es
    couponAlterInfoDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(partition =>{
        val list: List[(String, CouponAlertInfo)] = partition.toList.map(log => {
          //将数据封装成k: doc_id，v:指的是存放到es的数据类型
          (log.mid + log.ts / 1000 / 60, log)
        })
        MyEsUtil.insertBulk(GmallConstants.ES_INDEX_ALETR + "1027",list)
      })
    })

    //10.启动任务并阻塞

    ssc.start()
    ssc.awaitTermination()
  }
}
