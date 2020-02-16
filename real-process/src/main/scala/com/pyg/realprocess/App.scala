package com.pyg.realprocess

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._ // 隐式转换

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
    val localDataStream: DataStream[String] = env.fromCollection(
      List("a", "b", "c", "d")
    )

    localDataStream.print()

    // 执行
    env.execute("real-process")
  }
}
