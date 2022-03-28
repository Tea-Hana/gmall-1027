package com.atguigu.app

import java.util

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{OrderDetail, OrderInfo, SaleDetail}
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import org.json4s.native.Serialization

/**
 * @Author Hana
 * @Date 2022-03-28-15:21
 * @Description :
 */
object SaleDetailApp {
  def main(args: Array[String]): Unit = {
    //1. 创建sparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SaleDetailApp")

    //2. 创建streamingContext

    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    //3. 消费kafka中的数据分别获取订单表的数据以及订单明细表的数据
    val orderInfoKafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER, ssc)

    val orderDetailKafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER_DETAIL, ssc)

    //4. 将数据转为样例类,并返回KV类型数据，key是两条流的关联条件，即orderId
    val orderInfoDStream: DStream[(String, OrderInfo)] = orderInfoKafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])

        //补全字段
        orderInfo.create_date = orderInfo.create_time.split(" ")(0)
        orderInfo.create_hour = orderInfo.create_time.split(" ")(1).split(":")(0)
        (orderInfo.id,orderInfo)
      })
    })

    val orderDetailDStream: DStream[(String, OrderDetail)] = orderDetailKafkaDStream.mapPartitions(partiton => {
      partiton.map(record => {
        val orderDetail: OrderDetail = JSON.parseObject(record.value(), classOf[OrderDetail])
        (orderDetail.order_id,orderDetail)
      })
    })
//    orderInfoDStream.print()
    //    orderDetailDStream.print()

    //5.通过join，对两条流进行合并

//    val joinDStream: DStream[(String, (OrderInfo, OrderDetail))] = orderInfoDStream.join(orderDetailDStream)

    val fullJoinDStream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = orderInfoDStream.fullOuterJoin(orderDetailDStream)

    //6.采用加缓存的方式处理网络延迟所带来的数据丢失问题
    fullJoinDStream.mapPartitions(partition =>{
      //创建list集合用来存放结果数据
      val details: util.ArrayList[SaleDetail] = new util.ArrayList[SaleDetail]()

      //创建redis链接
      val jedis: Jedis = new Jedis("hadoop102", 6379)



      partition.foreach{
        case (orderId,(infoOpt,detailOpt))=>

          //存放orderInfo数据的redisKey
          val orderInfoRedisKey: String = "orderInfo" + orderId

          //a.判断订单表是否存在
          if (infoOpt.isDefined) {
            //orderInfo订单表存在
            //a2.取出订单表数据
            val orderInfo: OrderInfo = infoOpt.get

            //a3.判断orderDetail数据是否存在
            if (detailOpt.isDefined) {
              //orderDetail表存在
              //a4.取出orderDetail数据
              val orderDetail: OrderDetail = detailOpt.get

              //a5.关联两个表的数据，将其写入SaleDetail样例类中
              val detail: SaleDetail = new SaleDetail(orderInfo, orderDetail)
              //a6.将关联后的SaleDetail存入结果集合中
              details.add(detail)
            }
            //b1.无论如何将orderInfo数据写入redis换成
            implicit val formats=org.json4s.DefaultFormats
            val orderInfoJson: String = Serialization.write(orderInfo)

            jedis.set(orderInfoRedisKey,orderInfoJson)

            //b2.给存入redis中orderinfo数据设置过期时间，通过过期时间是网络延迟时间（建议：比这个时间多几秒）
            jedis.expire(orderInfoRedisKey,20)

            //c1.查询对方缓存中是否有能关联上的数据
              
            //
          }else {
            //orderInfo不在
            //d1.判断orderDetail数据是否存在
            /**
             * if存在 -> 获取到orderDetail数据 ->
             * 查询orderinfo缓存中是否有能关联上的数据 -> 能关联则写入结果集合
             *                                         |
             *                                         V
             *                                     没有能关联上，则将自己吸缓存
             */
          }
        }
      jedis.close()
      partition
    })

    //关闭阻塞流
    ssc.start()
    ssc.awaitTermination()

  }
}
