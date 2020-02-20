package com.pyg.realprocess.task

import com.pyg.realprocess.bean.{ClickLogWide, Message}
import com.pyg.realprocess.util.HBaseUtil
import org.apache.commons.lang.time.FastDateFormat
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.api.scala._
import org.apache.commons.lang.StringUtils
import org.mortbay.util.StringUtil // 出现(...)这样的error时需要导入隐式转换

/**
 * 预处理任务
 */
object PreprocessTask {
  def process(watermarkDataStream: DataStream[Message]): DataStream[ClickLogWide] = {
    // 预处理
    watermarkDataStream.map {
      msg =>
        // 转换时间 FastDateFormat 线程安全的
        val yearMonth = FastDateFormat.getInstance("yyyyMM").format(msg.timeStamp)
        val yearMonthDay = FastDateFormat.getInstance("yyyyMMdd").format(msg.timeStamp)
        val yearMonthDayHour = FastDateFormat.getInstance("yyyyMMddHH").format(msg.timeStamp)

        // 转换地区
        val address = msg.clickLog.country + msg.clickLog.province + msg.clickLog.city

//        val isNewTuple: (Int, Int, Int, Int) = isNewProcess(msg)

        ClickLogWide(
          msg.clickLog.channelID,
          msg.clickLog.categoryID,
          msg.clickLog.produceID,
          msg.clickLog.country,
          msg.clickLog.province,
          msg.clickLog.city,
          msg.clickLog.network,
          msg.clickLog.source,
          msg.clickLog.browserType,
          msg.clickLog.entryTime,
          msg.clickLog.leaveTime,
          msg.clickLog.userID,
          msg.count,
          msg.timeStamp,
          address,
          yearMonth,
          yearMonthDay,
          yearMonthDayHour, 0, 0, 0, 0)
//          isNewTuple._1,
//          isNewTuple._2,
//          isNewTuple._3,
//          isNewTuple._4)
    }
  }

  /**
   * 判断新老用户
   *
   * @param msg
   */
  private def isNewProcess(msg: Message): (Int, Int, Int, Int) = {
    // 1、定义4个遍历初始化为0
    var isNew = 0
    var isHourNew = 0
    var isDayNew = 0
    var isMonthNew = 0

    // 2、从HBase中查询用户记录，如果有记录，再去判断其他时间，如果没有记录，则证明是新用户
    val tableName = "user_history"
    var clfName = "info"
    var rowkey = msg.clickLog.userID + ":" + msg.clickLog.channelID

    // 用户ID(userid)
    var userIdColumn = "userid"
    // 频道ID(channelid)
    var channelidColumn = "channelid"
    // 最后访问时间（时间戳）(lastVisitedTime)
    var lastVisitedTimeColumn = "lastVisitedTime"

    val userId: String = HBaseUtil.getData(tableName, rowkey, clfName, userIdColumn)
    val channelid: String = HBaseUtil.getData(tableName, rowkey, clfName, channelidColumn)
    // 历史时间
    val lastVisitedTime: String = HBaseUtil.getData(tableName, rowkey, clfName, lastVisitedTimeColumn)

    // 如果userid为空，则该用户一定是新用户
    if (StringUtils.isBlank(userId)) {
      isNew = 1
      isHourNew = 1
      isDayNew = 1
      isMonthNew = 1

      // 保存用户的访问记录到 user_history
      HBaseUtil.putMapData(tableName, rowkey, clfName, Map(
        userIdColumn -> msg.clickLog.userID,
        channelidColumn -> msg.clickLog.channelID,
        lastVisitedTimeColumn -> msg.timeStamp
      ))
    } else {
      // 老用户
      isNew = 0
      // 其他字段需要进行时间戳的比对
      isHourNew = compareDate(msg.timeStamp, lastVisitedTime.toLong, "yyyyMMddHH")
      isDayNew = compareDate(msg.timeStamp,lastVisitedTime.toLong,"yyyyMMdd")
      isMonthNew = compareDate(msg.timeStamp,lastVisitedTime.toLong,"yyyyMM")

      // 更新 user_history 表的用户时间戳
      HBaseUtil.putData(tableName, rowkey, clfName,lastVisitedTimeColumn , msg.timeStamp.toString)
    }

    (isNew, isHourNew, isDayNew, isMonthNew)
  }

  /**
   * 比对时间戳
   * 201912 > 201911
   *
   * @param currentTime 当前时间
   * @param historyTime 历史时间
   * @param format      时间格式
   * @return 1 或者 0
   */
  def compareDate(currentTime: Long, historyTime: Long, format: String): Int = {
    val currentTimeStr: String = timestamp2Str(currentTime, format)
    val historyTimeStr: String = timestamp2Str(historyTime, format)

    // 比对字符串大小，如果当前时间 > 历史时间 则返回1(新用户)
    var result: Int = currentTimeStr.compareTo(historyTimeStr)

    if (result > 0) {
      result = 1
    } else {
      result = 0
    }

    result
  }

  /**
   * 转换日期
   *
   * @param timestamp Long类型的时间戳
   * @param format    日期格式
   * @return
   */
  def timestamp2Str(timestamp: Long, format: String): String = {
    FastDateFormat.getInstance(format).format(timestamp)
  }
}
