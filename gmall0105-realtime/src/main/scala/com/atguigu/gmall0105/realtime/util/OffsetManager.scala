package com.atguigu.gmall0105.realtime.util

import java.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis



object OffsetManager {


  /**
   * 从redis中读取偏移量
   * @param topicName  消费主题名称
   * @param groupId   消费者组名称
   * @return
   */
  def  getOffset(topicName:String,groupId:String): Map[TopicPartition,Long]  ={
    // Redis 中偏移量的保存格式
    //type         key            field        value
    //hash    topic+groupid   partition_id    offset
    val jedis: Jedis = RedisUtil.getJedisClient
    val offsetKey="offset:"+topicName+":"+groupId
    //通过hgetAll来获取每个分区的偏移量
    val offsetMap: util.Map[String, String] = jedis.hgetAll(offsetKey)
    jedis.close()
    import scala.collection.JavaConversions._
    //将map结构转化为Map[TopicPartition, Long]结构来提供给kafka读取偏移量
    val kafkaOffsetMap:  Map[TopicPartition, Long] = offsetMap.map { case (patitionId, offset) =>
      println("加载分区偏移量："+patitionId +":"+offset  )
      (new TopicPartition(topicName, patitionId.toInt), offset.toLong)
    }.toMap
    kafkaOffsetMap
  }

  /**
   * 保存偏移量到redis
   * @param topicName
   * @param groupId
   * @param offsetRanges
   */
  def saveOffset(topicName:String ,groupId:String ,offsetRanges: Array[OffsetRange]): Unit ={
    //redis偏移量的写入
    // Redis 中偏移量的保存格式
    //type         key            field        value
    //hash    topic+groupid   partition_id    offset
    val offsetKey="offset:"+topicName+":"+groupId
    val offsetMap:util.Map[String,String]=new util.HashMap()
    //转换结构 offsetRanges -> offsetMap
    for (offset <- offsetRanges) {
      //获取分区数
      val partition: Int = offset.partition
      //获取最后偏移量
      val untilOffset: Long = offset.untilOffset
      //将分区数和最后偏移量存入map
      offsetMap.put(partition+"",untilOffset+"")
      println("写入分区："+partition +":"+offset.fromOffset+"-->"+offset.untilOffset)
    }
    //写入redis
    if(offsetMap!=null&&offsetMap.size()>0){
      val jedis: Jedis = RedisUtil.getJedisClient
      jedis.hmset(offsetKey,offsetMap)
      jedis.close()
    }


  }


}