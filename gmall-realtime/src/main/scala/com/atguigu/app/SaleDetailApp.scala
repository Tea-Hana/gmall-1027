package com.atguigu.app

import java.util

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.{MyEsUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import org.json4s.native.Serialization

import collection.JavaConverters._

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
    val noUserSaleDetailDStream: DStream[SaleDetail] = fullJoinDStream.mapPartitions(partition => {
      //创建list集合用来存放结果数据
      val details: util.ArrayList[SaleDetail] = new util.ArrayList[SaleDetail]()

      //创建redis链接
      val jedis: Jedis = new Jedis("hadoop102", 6379)

      implicit val formats = org.json4s.DefaultFormats

      partition.foreach {

        case (orderId, (infoOpt, detailOpt)) =>

          //存放orderInfo数据的redisKey
          val orderInfoRedisKey: String = "orderInfo" + orderId
          //存放orderDerail数据的RedisKey
          val orderDetailRedisKey: String = "orderDetail" + orderId


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

            val orderInfoJson: String = Serialization.write(orderInfo)

            jedis.set(orderInfoRedisKey, orderInfoJson)

            //b2.给存入redis中orderinfo数据设置过期时间，通过过期时间是网络延迟时间（建议：比这个时间多几秒）
            jedis.expire(orderInfoRedisKey, 200)

            //c1.查询对方缓存中是否有能关联上的数据

            if (jedis.exists(orderDetailRedisKey)) {
              //证明有能关联式行的OrderDetail数据
              //c2.获取redis缓存中能够关联上orderDetail数据
              val detailSet: util.Set[String] = jedis.smembers(orderDetailRedisKey)

              //c3.遍历set集合获取到每一个数OrderDetail据
              for (elem <- detailSet.asScala) {
                //将查询出来的Json字符串转为样例类
                val orderDetail: OrderDetail = JSON.parseObject(elem, classOf[OrderDetail])

                val detail = new SaleDetail(orderInfo, orderDetail)

                //c4.将关联后的样例类写入结果集合
                details.add(detail)
              }
            }

            //
          } else {
            //orderInfo不在
            //d1.判断orderDetail数据是否存在

            if (detailOpt.isDefined) {
              //orderDetail数据存在
              //取出orderDetail数据

              val orderDetail: OrderDetail = detailOpt.get
              //e.查询orderInfo缓存中是否能有关联上的数据

              if (jedis.exists(orderInfoRedisKey)) {

                //有能关联上的orderInfo数据
                //e2.取出orderInfo数据
                val infoJsonStr: String = jedis.get(orderInfoRedisKey)

                //e3.将读出来的JSON字符串转换成样例类
                val orderInfo: OrderInfo = JSON.parseObject(infoJsonStr, classOf[OrderInfo])

                //关联数据写入SaleDetail样例类
                val detail: SaleDetail = new SaleDetail(orderInfo, orderDetail)
                details.add(detail)
              } else {
                //没有能关联上的orderInfo数据
                //f.将自己写如数据
                //将orderDetail样例类转为Json字符串

                val orderDetailJsonStr: String = Serialization.write(orderDetail)
                jedis.sadd(orderDetailRedisKey, orderDetailJsonStr)

                //对orderDetail数据设置过期时间，以防redis内存溢出
                jedis.expire(orderDetailRedisKey, 200)
              }
            }
          }
      }
      jedis.close()
      //将集合中的数据返回，同时将集合转为迭代器
      details.asScala.toIterator
    })

    //7.反查userInfo缓存关联用户表数据
    val saleDetailDStream: DStream[SaleDetail] = noUserSaleDetailDStream.mapPartitions(partition => {
      //创建redis链接
      val jedis: Jedis = new Jedis("hadoop102", 6379)
      val details: Iterator[SaleDetail] = partition.map(saleDetail => {
        val userInfoRedisKey: String = "userInfo: " + saleDetail.user_id
        //查询userInfo的数据
        val userInfoJsonStr: String = jedis.get(userInfoRedisKey)
        //将查询到的json字符串转为样例类
        val userInfo: UserInfo = JSON.parseObject(userInfoJsonStr, classOf[UserInfo])

        //关联用户表的数据
        saleDetail.mergeUserInfo(userInfo)
        saleDetail
      })
      jedis.close()
      details
    })

    saleDetailDStream.print()

    //8.将数据写入ES
    saleDetailDStream.foreachRDD(rdd =>{
      rdd.foreachPartition(iterator=>{
        val list: List[(String, SaleDetail)] = iterator.toList.map(SaleDetail => {
          (SaleDetail.order_detail_id, SaleDetail)
        })
        MyEsUtil.insertBulk(GmallConstants.ES_INDEX_SALEDETAIL + "211027",list)
      })
    })

    //关闭阻塞流
    ssc.start()
    ssc.awaitTermination()

  }
}
