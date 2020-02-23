package com.pyg.realprocess.task

import com.pyg.realprocess.bean.ClickLogWide
import com.pyg.realprocess.util.HBaseUtil
import org.apache.commons.lang.StringUtils
import org.apache.flink.streaming.api.scala.{DataStream, WindowedStream}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

case class ChannelPvUv(
                        val channelId: String,
                        val yearDayMonthHour: String,
                        val pv: Long,
                        val uv: Long)

/**
 * pu和uv统计
 * rowkey设计为频道ID和时间的组合
 *
 * 1、转换
 * 2、分组
 * 3、时间窗口
 * 4、聚合
 * 5、落地
 */
object ChannelPvUvTask {
  // 以小时为维度的
  //  def process(clickLogWideDataStream: DataStream[ClickLogWide]) = {
  //    // 1、转换
  //    val channelPvUvDS = clickLogWideDataStream.map {
  //      clickLogWide => {
  //        // 新用户 1， 老用户 0
  //        ChannelPvUv(clickLogWide.channelID, clickLogWide.yearMonthDayHour, clickLogWide.count, clickLogWide.isHourNew)
  //      }
  //    }
  //
  //    // 2、按照 频道ID和时间 分组
  //    val keyedStream = channelPvUvDS.keyBy {
  //      channelPvUv =>
  //        channelPvUv.channelId + channelPvUv.yearDayMonthHour
  //    }
  //
  //    // 3、窗口
  //    val windowStream: WindowedStream[ChannelPvUv, String, TimeWindow] = keyedStream.timeWindow(Time.seconds(3))
  //
  //    // 4、聚合
  //    val reduceDataStream = windowStream.reduce {
  //      (t1, t2) =>
  //        ChannelPvUv(t1.channelId, t1.yearDayMonthHour, t1.pv + t2.pv, t1.uv + t2.uv)
  //    }
  //
  //    // 5、落地
  //    reduceDataStream.addSink(new SinkFunction[ChannelPvUv] {
  //      override def invoke(value: ChannelPvUv): Unit = {
  //        // 取值累加
  //        val tableName = "channel_pvuv"
  //        val clfName = "info"
  //        val channelIdColumn = "channelId"
  //        val yearMonthDayHourColumn = "yearMonthDayHour"
  //        val pvColumn = "pv"
  //        val uvColumn = "uv"
  //
  //        val rowkey = value.channelId + ":" + value.yearDayMonthHour
  //
  //        val pvInHBase: String = HBaseUtil.getData(tableName, rowkey, clfName, pvColumn)
  //        val uvInHBase: String = HBaseUtil.getData(tableName, rowkey, clfName, uvColumn)
  //
  //        var totalPv = 0L
  //        var totalUv = 0L
  //
  //        // 如果HBase中没有pv值,那么就把当前值保存,如果有值,进行累加
  //        if (StringUtils.isBlank(pvInHBase)) {
  //          totalPv = value.pv
  //        } else {
  //          totalPv = value.pv + pvInHBase.toLong
  //        }
  //
  //        // 如果HBase中没有pv值,那么就把当前值保存,如果有值,进行累加
  //        if (StringUtils.isBlank(uvInHBase)) {
  //          totalUv = value.uv
  //        } else {
  //          totalUv = value.uv + uvInHBase.toLong
  //        }
  //
  //        HBaseUtil.putMapData(tableName, rowkey, clfName, Map(
  //          channelIdColumn -> value.channelId,
  //          yearMonthDayHourColumn -> value.yearDayMonthHour,
  //          pvColumn -> totalPv.toString,
  //          uvColumn -> totalUv.toString
  //        ))
  //
  //      }
  //    })
  //  }

  /**
   * 天，小时，月维度的pv uv
   */
  def process(clickLogWideDataStream: DataStream[ClickLogWide]) = {
    // 1、转换
    // flatMap 展开 1 -> 3
    // map是一种类型转换为另一种类型，而flatMap是一种转多种
    val channelPvUvDS = clickLogWideDataStream.flatMap {
      clickLogWide =>
        List(
          ChannelPvUv(clickLogWide.channelID, clickLogWide.yearMonthDayHour, clickLogWide.count, clickLogWide.isHourNew),
          ChannelPvUv(clickLogWide.channelID, clickLogWide.yearMonthDay, clickLogWide.count, clickLogWide.isDayNew),
          ChannelPvUv(clickLogWide.channelID, clickLogWide.yearMonth, clickLogWide.count, clickLogWide.isMonthNew)
        )
    }

    // 2、按照 频道ID和时间 分组
    val keyedStream = channelPvUvDS.keyBy {
      channelPvUv =>
        channelPvUv.channelId + channelPvUv.yearDayMonthHour
    }

    // 3、窗口
    val windowStream: WindowedStream[ChannelPvUv, String, TimeWindow] = keyedStream.timeWindow(Time.seconds(3))

    // 4、聚合
    val reduceDataStream = windowStream.reduce {
      (t1, t2) =>
        ChannelPvUv(t1.channelId, t1.yearDayMonthHour, t1.pv + t2.pv, t1.uv + t2.uv)
    }

    // 5、落地
    reduceDataStream.addSink(new SinkFunction[ChannelPvUv] {
      override def invoke(value: ChannelPvUv): Unit = {
        // 取值累加
        val tableName = "channel_pvuv"
        val clfName = "info"
        val channelIdColumn = "channelId"
        val yearMonthDayHourColumn = "yearMonthDayHour"
        val pvColumn = "pv"
        val uvColumn = "uv"

        val rowkey = value.channelId + ":" + value.yearDayMonthHour

        val pvInHBase: String = HBaseUtil.getData(tableName, rowkey, clfName, pvColumn)
        val uvInHBase: String = HBaseUtil.getData(tableName, rowkey, clfName, uvColumn)

        var totalPv = 0L
        var totalUv = 0L

        // 如果HBase中没有pv值,那么就把当前值保存,如果有值,进行累加
        if (StringUtils.isBlank(pvInHBase)) {
          totalPv = value.pv
        } else {
          totalPv = value.pv + pvInHBase.toLong
        }

        // 如果HBase中没有pv值,那么就把当前值保存,如果有值,进行累加
        if (StringUtils.isBlank(uvInHBase)) {
          totalUv = value.uv
        } else {
          totalUv = value.uv + uvInHBase.toLong
        }

        HBaseUtil.putMapData(tableName, rowkey, clfName, Map(
          channelIdColumn -> value.channelId,
          yearMonthDayHourColumn -> value.yearDayMonthHour,
          pvColumn -> totalPv.toString,
          uvColumn -> totalUv.toString
        ))

      }
    })
  }
}
