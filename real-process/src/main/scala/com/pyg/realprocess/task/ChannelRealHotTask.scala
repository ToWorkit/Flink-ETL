package com.pyg.realprocess.task

import com.pyg.realprocess.bean.ClickLogWide
import com.pyg.realprocess.util.HBaseUtil
import org.apache.commons.lang.StringUtils
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, WindowedStream}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

case class ChannelRealHot(var channelId: String,
                          var visited: Long)

/**
 * 频道热点分析
 *
 * 1、字段的转换
 * 2、分组
 * 3、时间窗口
 * 4、聚合
 * 5、落地HBase
 */
object ChannelRealHotTask {


  /**
   *
   * @param clickLogWideDataStream 处理之后的数据
   */
  def process(clickLogWideDataStream: DataStream[ClickLogWide]) = {
    // 1、字段转换为样例类 channelId, visited
    val realHotDataStream: DataStream[ChannelRealHot] = clickLogWideDataStream.map {
      clickLogWide: ClickLogWide =>
        ChannelRealHot(clickLogWide.channelID, clickLogWide.count)
    }

    // 2、分组 channelID
    val keyedStream: KeyedStream[ChannelRealHot, String] = realHotDataStream.keyBy(_.channelId)

    // 3、时间窗口
    // 3秒钟计算一次窗口内的数据
    val windowedStream: WindowedStream[ChannelRealHot, String, TimeWindow] = keyedStream.timeWindow(Time.seconds(3))

    // 4、聚合
    // 上次统计的结果和本次待统计的结果累加
    val reduceDataStream = windowedStream.reduce {
      (t1: ChannelRealHot, t2: ChannelRealHot) =>
        ChannelRealHot(t1.channelId, t1.visited + t2.visited)
    }

    // reduceDataStream

    // 5、落地HBase
    reduceDataStream.addSink(new SinkFunction[ChannelRealHot] {
      override def invoke(value: ChannelRealHot): Unit = {
        // hbase相关字段

        val tableName = "channel"
        val clfName = "info"
        val channelIdColumn = "channelId"
        val visitedColumn = "visited"
        val rowkey = value.channelId

        // 查询HBase，获取相关记录
        val visitedValue: String = HBaseUtil.getData(tableName, rowkey, clfName, visitedColumn)
        // 创建总数的临时变量
        var totalCount: Long = 0

        // 为空
        /**
         * 在校验一个String类型的变量是否为空时，通常存在3中情况
         *
         * 是否为 null
         * 是否为 ""
         * 是否为空字符串(引号中间有空格)  如： "     "。
         * StringUtils的isBlank()方法可以一次性校验这三种情况，返回值都是true
         */
        if (StringUtils.isBlank(visitedValue)) {
          totalCount = value.visited
        } else { // 不为空
          totalCount = visitedValue.toLong + value.visited
        }

        // 保存数据
        HBaseUtil.putMapData(tableName, rowkey, clfName, Map(
          channelIdColumn -> value.channelId,
          visitedColumn -> value.visited
        ))
      }
    })
  }
}
