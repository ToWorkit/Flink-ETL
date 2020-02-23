package com.pyg.realprocess.task

import com.pyg.realprocess.bean.ClickLogWide
import org.apache.flink.streaming.api.scala.{DataStream, WindowedStream}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

case class ChannelFreshness(
                             var channelId: String,
                             var date: String,
                             var newCount: Long, // 新用户
                             val oldCount: Long // 老用户
                           )

/**
 * 各维度的用户新鲜度
 * 1. 转换
 * 2. 分组
 * 3. 时间窗口
 * 4. 聚合
 * 5. 落地HBase
 */
object ChannelFreshness {
  def process(clickLogWideDataStream: DataStream[ClickLogWide]) = {
    // 1、转换
    // 多个时间段的，使用flatMap
    val mapDataStream = clickLogWideDataStream.flatMap {
      clickLog =>
        // 如果是老用户，只有在他第一次来的时候计数为1
        val isOld = (isNew: Int, isDateNew: Int) => if (isNew == 0 && isDateNew == 1) 1 else 0

        List(
          ChannelFreshness(clickLog.channelID, clickLog.yearMonthDayHour, clickLog.isNew, isOld(clickLog.isNew, clickLog.isHourNew)),
          ChannelFreshness(clickLog.channelID, clickLog.yearMonthDay, clickLog.isNew, isOld(clickLog.isNew, clickLog.isDayNew)),
          ChannelFreshness(clickLog.channelID, clickLog.yearMonth, clickLog.isNew, isOld(clickLog.isNew, clickLog.isMonthNew))
        )
    }

    // 2、分组
    // 渠道id和时间
    val keyedStream = mapDataStream.keyBy {
      freshness => (freshness.channelId + freshness.date)
    }

    // 3、时间窗口
    val windowedStream: WindowedStream[ChannelFreshness, String, TimeWindow] = keyedStream.timeWindow(Time.seconds(3))

    // 4、聚合
    val reduceDataStream = windowedStream.reduce {
      (t1, t2) =>
        ChannelFreshness(t1.channelId, t1.date, t1.newCount + t2.newCount, t1.oldCount + t2.oldCount)
    }

    // 5、落地
    reduceDataStream.addSink(new SinkFunction[ChannelFreshness] {
      override def invoke(value: ChannelFreshness): Unit = {
        // 创建HBase相关变量
        val tableName = "channel_freshness"
        val clfName = "info"
        val rowkey = value.channelId + ":" + value.date

        // 查询历史数据


        // 累加

        // 保存数据
      }
    })
  }

}
