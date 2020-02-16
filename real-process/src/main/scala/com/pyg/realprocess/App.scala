package com.pyg.realprocess

import java.util.Properties

import com.alibaba.fastjson.JSON
import com.pyg.realprocess.bean.{ClickLog, Message}
import com.pyg.realprocess.util.GlobalConfigUtil
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010 // 隐式转换

object App {
  def main(args: Array[String]): Unit = {
    // 初始化Flink流式环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 设置流式处理时间为EventTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 设置并行度
    env.setParallelism(1)

    // 本地测试，记载本地集合成为一个DataStream 打印输出
    // 后面报(...)这种错误时需要导入隐式转换 import org.apache.flink.api.scala._
/*    val localDataStream: DataStream[String] = env.fromCollection(
      List("a", "b", "c", "d")
    )

    localDataStream.print()*/

    // 添加Checkpoint容错
    // 5s钟启动一次checkpoint
    env.enableCheckpointing(5000)
    // 设置checkpoint只checkpoint一次
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    // 设置两次checkpoint的最小时间间隔
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(1000)
    // 设置checkpoint的超时时长
    env.getCheckpointConfig.setCheckpointTimeout(60000)
    // 最大并行度
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    // 当程序关闭时，触发额外的checkpoint
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    // 设置checkpoint的地址
    env.setStateBackend(new FsStateBackend("hdfs://192.168.178.11:9000/flink-checkpoint/"))

    /**
     * 整合kafka
     */
    val properties = new Properties()
    // # Kafka集群地址
    properties.setProperty("bootstrap.servers", GlobalConfigUtil.bootstrapServers)
    // # ZooKeeper集群地址
    properties.setProperty("zookeeper.connect", GlobalConfigUtil.zookeeperConnect)
    // # Kafka Topic名称
    properties.setProperty("input.topic", GlobalConfigUtil.inputTopic)
    // # 消费组ID
    properties.setProperty("group.id", GlobalConfigUtil.groupId)
    // # 自动提交拉取到消费端的消息offset到kafka
    properties.setProperty("enable.auto.commit", GlobalConfigUtil.enableAutoCommit)
    // # 自动提交offset到zookeeper的时间间隔单位（毫秒）
    properties.setProperty("auto.commit.interval.ms", GlobalConfigUtil.autoCommitIntervalMs)
    // # 每次消费最新的数据
    properties.setProperty("auto.offset.reset", GlobalConfigUtil.autoOffsetReset)

    // topic 反序列化器 属性集合
    val consumer = new FlinkKafkaConsumer010[String](GlobalConfigUtil.inputTopic, new SimpleStringSchema(), properties)

    val kafkaDataStream: DataStream[String] = env.addSource(consumer)

    // 测试
    // kafkaDataStream.print()

    /**
     * 将JSON格式的日志转为元组格式 -> 在转为样例类 -> 统一转为Message样例类
     */
    val tupleDataStream = kafkaDataStream.map {
      msgJson =>
        val jsonObject = JSON.parseObject(msgJson)

        // 提取需要的数据
        val message = jsonObject.getString("message")
        val count = jsonObject.getLong("count")
        val timeStamp = jsonObject.getLong("timeStamp")

        // (message, count, timeStamp)
        // (ClickLog(message), count, timeStamp)
        Message(ClickLog(message), count, timeStamp)
    }

    tupleDataStream.print()

    // 添加水印支持(解决消息因为网络延迟而没有被处理)
    tupleDataStream.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[Message] {
      var currentTimeStamp = 0l

      // 延迟时间
      var maxDelayTime = 2000l

      // 获取当前时间戳(水印的时间)
      override def getCurrentWatermark: Watermark = {
        new Watermark(currentTimeStamp - maxDelayTime)
      }

      // 获取事件时间
      /**
       * @param element 从当前消息中取得时间
       * @param previousElementTimestamp 前一个消息的时间
       * @return 哪个大用哪个
       */
      override def extractTimestamp(element: Message, previousElementTimestamp: Long): Long = {
        currentTimeStamp = Math.max(element.timeStamp, previousElementTimestamp)
        currentTimeStamp
      }
    })

    // 执行
    env.execute("real-process")
  }
}
