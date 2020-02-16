package com.pyg.realprocess.bean

import com.alibaba.fastjson.JSON

/**
 * 样例类
 */

//频道ID(channelID)
//产品类别ID(categoryID)
//产品ID(produceID)
//国家(country)
//省份(province)
//城市(city)
//网络方式(network)
//来源方式(source)
//浏览器类型(browserType)
//进入网站时间(entryTime)
//离开网站时间(leaveTime)
//用户的ID(userID)
case class ClickLog (
                      var channelID:String,
                      var categoryID:String,
                      var produceID:String,
                      var country:String,
                      var province:String,
                      var city:String,
                      var network:String,
                      var source:String,
                      var browserType:String,
                      var entryTime:String,
                      var leaveTime:String,
                      var userID:String)

/**
 * 创建一个伴生对象
 */
object ClickLog {
  def apply(json: String): ClickLog = {
    // 1、先将json转为jsonObject
    val jsonObject = JSON.parseObject(json)

    // 2、提取jsonObject中的各个属性并赋值给样例类
    val channelID = jsonObject.getString("channelID")
    val categoryID = jsonObject.getString("categoryID")
    val produceID = jsonObject.getString("produceID")
    val country = jsonObject.getString("country")
    val province = jsonObject.getString("province")
    val city = jsonObject.getString("city")
    val network = jsonObject.getString("network")
    val source = jsonObject.getString("source")
    val browserType = jsonObject.getString("browserType")
    val entryTime = jsonObject.getString("entryTime")
    val leaveTime = jsonObject.getString("leaveTime")
    val userID = jsonObject.getString("userID")

    ClickLog(
      channelID,
      categoryID,
      produceID,
      country,
      province,
      city,
      network,
      source,
      browserType,
      entryTime,
      leaveTime,
      userID)
  }
}
