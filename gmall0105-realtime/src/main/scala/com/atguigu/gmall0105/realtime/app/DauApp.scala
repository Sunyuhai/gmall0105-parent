package com.atguigu.gmall0105.realtime.app

import java.lang
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall0105.realtime.bean.DauInfo
import com.atguigu.gmall0105.realtime.util.{MyEsUtil, MyKafkaUtil, OffsetManager, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

object DauApp {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("dau_app")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    //消费启动日志的数据（GMALL_STARTUP_0105）
    val topic = "GMALL_STARTUP_0105"
    val groupId = "DAU_GROUP"

    //去redis中读取偏移量
    val kafkaOffsetMap: Map[TopicPartition, Long] = OffsetManager.getOffset(topic,groupId)
    //保存读取偏移量后开始保存的日志数据
    var recordInputStream: InputDStream[ConsumerRecord[String, String]]=null

    //判定是不是第一次启动，第一次启动redis偏移量为空
    if(kafkaOffsetMap!=null&&kafkaOffsetMap.size>0){
      recordInputStream = MyKafkaUtil.getKafkaStream(topic,ssc,kafkaOffsetMap,groupId)
    }else{
      recordInputStream = MyKafkaUtil.getKafkaStream(topic,ssc)
    }

    //得到本批次的偏移量的结束位置，用于更新redis中的偏移量
    var  offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val  inputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = recordInputStream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges  //driver? executor?  //周期性的执行
      rdd
    }

    /**
     * 格式化为json对象，增加两个字段，将原本的最后毫秒级字段添加为日期和小时字段，返回json对象
     */
    val jsonObjDstream: DStream[JSONObject] = inputGetOffsetDstream.map { record =>
      val jsonString: String = record.value()
      val jsonObj: JSONObject = JSON.parseObject(jsonString)
      val ts: lang.Long = jsonObj.getLong("ts")
      val datehourString: String = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(ts))
      val dateHour: Array[String] = datehourString.split(" ")

      jsonObj.put("dt", dateHour(0))
      jsonObj.put("hr", dateHour(1))

      jsonObj
    }

    //redis :  type set   string hash list set zset       key ? dau:2020-06-17        value?  mid   (field? score?)  (expire?) 24小时
    /**
     * 去重思路：利用redis保存今天访问过系统的用户清单，并用redis保存，存储时间为24小时
     * redis的存储数据结构：
     * type ?      key ?               value ?
     * set     dau:当天日期           mid（设备数）
     * 将json对象传入，利用redis将其去重，返回去重后的数据
     */
    val filteredDstream: DStream[JSONObject] = jsonObjDstream.mapPartitions { jsonObjItr =>
      //一个分区只申请一次连接
      val jedis: Jedis = RedisUtil.getJedisClient
      //存取每个分区去重后的数据
      val filteredList = new ListBuffer[JSONObject]()
      //  Iterator 只能迭代一次 包括取size   所以要取size 要把迭代器转为别的容器
      val jsonList: List[JSONObject] = jsonObjItr.toList
      for (jsonObj <- jsonList) {
        //获取dt字段的（年-月-日） 信息
        val dt: String = jsonObj.getString("dt")
        //获取mid字段的唯一设备信息
        val mid: String = jsonObj.getJSONObject("common").getString("mid")
        //拼接key
        val dauKey = "dau:" + dt
        //sadd方法：如果未存在则保存 返回1  如果已经存在则不保存 返回0
        val isNew: lang.Long = jedis.sadd(dauKey, mid)
        //设置保存数据过期时间
        jedis.expire(dauKey,3600*24)
        if (isNew == 1L) {
          filteredList += jsonObj
        }
      }
      jedis.close()
      filteredList.toIterator
    }

    //保存到ES
    filteredDstream.foreachRDD { rdd =>
      rdd.foreachPartition { jsonItr =>
        val list: List[JSONObject] = jsonItr.toList
        //list.map 把源数据 转换成为要保存的数据格式
        val dauList: List[(String, DauInfo)] = list.map { jsonObj =>
          //抽取common字段为实体类（daoInfo）
          val commonJSONObj: JSONObject = jsonObj.getJSONObject("common")
          //根据ES构成的索引模板来转化数据
          val dauInfo = DauInfo(commonJSONObj.getString("mid"),
            commonJSONObj.getString("uid"),
            commonJSONObj.getString("ar"),
            commonJSONObj.getString("ch"),
            commonJSONObj.getString("vc"),
            jsonObj.getString("dt"),
            jsonObj.getString("hr"),
            "00",
            jsonObj.getLong("ts")
          )

          (dauInfo.mid, dauInfo)

        }
        val dt: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
        //调用保存方法
        MyEsUtil.bulkDoc(dauList, "gmall0105_dau_info_" + dt)
      }
      // 偏移量提交区
      OffsetManager.saveOffset(topic,groupId,offsetRanges)
    }



      ssc.start()
      ssc.awaitTermination()
    }


}