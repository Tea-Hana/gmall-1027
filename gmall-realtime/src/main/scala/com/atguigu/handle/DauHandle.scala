package com.atguigu.handle

import java.text.SimpleDateFormat
import java.util.Date
import java.{lang, util}

import com.atguigu.bean.StartUpLog
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object DauHandle {
  /**
   * 批次内去重
   * groupByKey->将相同的日期，相同的mid聚和到一块->按照时间戳进行排序，由小到大排序，利用take取第一条
   * @param filterByRedisDStream
   */
  def filterByGroup(filterByReidsDStream: DStream[StartUpLog]) = {
    //1. 将数据格式转换为k,v类型，为了我们调用GroupByKey算子
    val midAndLogDateToLogDStream: DStream[((String, String), StartUpLog)] = filterByReidsDStream.map(log => {
      ((log.logDate, log.mid), log)
    })
    //2. 对相同mid以及同一天的数据聚合到一块
    val midAndLogDateToIterLogDStream: DStream[((String, String), Iterable[StartUpLog])] = midAndLogDateToLogDStream.groupByKey()

    //3. 按照时间戳进行排序，由小到大进行排序，利用take取第一条
    val midAndLogToListLogDstream: DStream[((String, String), List[StartUpLog])] = midAndLogDateToIterLogDStream.mapValues(log => {
      log.toList.sortWith(_.ts < _.ts).take(1)
    })
    //4. 使用FlatMap算子打散List集合中数据
    val value: DStream[StartUpLog] = midAndLogToListLogDstream.flatMap(_._2)

    value
  }


  /**
   * 批次间去重（把经过去重后的数据（mid）缓存起来到Redis->后面每一个批次过来之后，先对比redis中有没有相同的mid，如果有的话
   * 则过滤掉）
   *
   * @param startUpLogDStream
   */
  def filterByRedis(startUpLogDStream: DStream[StartUpLog],sc:SparkContext) = {
    /**
     * filter算子返回值是false的情况下，过滤数据，true的话保留数据
     */
    /* val value: DStream[StartUpLog] = startUpLogDStream.filter(log => {
       //1.创建redis连接
       val jedis: Jedis = new Jedis("hadoop102", 6379)

       //      //2.查询redis中的数据
       val redisKey: String = "Dau:" + log.logDate
       //      val mids: util.Set[String] = jedis.smembers(redisKey)
       //
       //      //3.拿当前的mid与查询出来的mid作对比看是否有相同的
       //      val bool: Boolean = mids.contains(log.mid)

       //2.查询redis中的数据，利用redis中的方法来判断当前mid是否在redis中也有，在的话返回true，不在的话返回false
       val bool: lang.Boolean = jedis.sismember(redisKey, log.mid)

       jedis.close()
       !bool
     })
     value
 */

    //方案二：在每个分区下获取连接，以减少链接个数
    /*    val value: DStream[StartUpLog] = startUpLogDStream.mapPartitions(partition => {
          //1.在每个分区下创建redis连接
          val jedis: Jedis = new Jedis("hadoop102", 6379)
          val logs: Iterator[StartUpLog] = partition.filter(log => {
            val redisKey: String = "Dau:" + log.logDate
            //2.查询redis中的数据，利用redis中的方法来判断当前mid是否在redis中也有，在的话返回true，不在的话返回false
            val bool: lang.Boolean = jedis.sismember(redisKey, log.mid)
            !bool
          })
          jedis.close()
          logs
        })
        value*/

    //方案三：在每个批次下获取链接，以减少连接个数
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val value: DStream[StartUpLog] = startUpLogDStream.transform(rdd => {
      //1.在Driver端获取redis连接
      val jedis: Jedis = new Jedis("hadoop102", 6379)

      //2.获取redis中保存的mid
      val timeDay: String = sdf.format(new Date(System.currentTimeMillis()))
      val redisKey: String = "Dau:" + timeDay
      val mids: util.Set[String] = jedis.smembers(redisKey)

      //3.将查询出来的数据广播至Executor端
      val midsBc: Broadcast[util.Set[String]] = sc.broadcast(mids)

      //4.获取广播的数据，进而进行判断
      val filterRDD: RDD[StartUpLog] = rdd.filter(log => {
        !midsBc.value.contains(log.mid)
      })
      jedis.close()
      filterRDD
    })
    value
  }

  /**
   * 将mid保存至Redis
   *
   * @param startUpLogDStream
   */
  def saveToRedis(startUpLogDStream: DStream[StartUpLog]) = {
    startUpLogDStream.foreachRDD(rdd => {
      //不能在foreachRDD中创建redis的原因是连接在Driver端创建，使用在Execoter端使用，会报序列化错误，因为连接不能被序列化
      //建议在每个分区下获取连接，以此减少连接个数
      rdd.foreachPartition(partition => {
        //1.在分区下创建redis连接
        val jedis: Jedis = new Jedis("hadoop102", 6379)
        partition.foreach(log => {
          //2.将mid写入redis
          val redisKey: String = "Dau:" + log.logDate
          jedis.sadd(redisKey, log.mid)
        })
        jedis.close()
      })


    })

  }

}