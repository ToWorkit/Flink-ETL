package com.pyg.realprocess.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Admin, ColumnFamilyDescriptor, ColumnFamilyDescriptorBuilder, Connection, ConnectionFactory, Delete, Get, Put, Result, Table, TableDescriptor, TableDescriptorBuilder}
import org.apache.hadoop.hbase.util.Bytes

/**
 * HBase的工具类
 *
 * 获取table
 * 保存单列数据
 * 查询单列数据
 * 保存多列数据
 * 查询多列数据
 * 删除数据
 */
object HBaseUtil {
  // HBase的配置类，不需要指定配置文件，内部已实现(需指定一致的文件名 hbase-site.xml)
  val conf: Configuration = HBaseConfiguration.create()

  // HBase的连接
  val conn: Connection = ConnectionFactory.createConnection(conf)

  // HBase的操作API
  val admin: Admin = conn.getAdmin

  /**
   * 返回table，如果不存在，则创建表
   * @param tableNameStr
   * @param columnFamilyName
   * @return
   */
  def getTable(tableNameStr: String, columnFamilyName: String): Table = {
    // 获取tableName
    val tableName: TableName = TableName.valueOf(tableNameStr)

    // 不存在则创建表
    if(!admin.tableExists(tableName)) {
      // 构建出表的描述的创建者
      val descBuilder: TableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName)

      // 给表添加列族
      val familyDescriptor: ColumnFamilyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(columnFamilyName.getBytes()).build()
      descBuilder.setColumnFamily(familyDescriptor)

      // 创建表
      admin.createTable(descBuilder.build())
    }

    conn.getTable(tableName)
  }

  /**
   * 存储单列数据
   * @param tableNameStr 表名
   * @param rowkey  rowkey
   * @param columnFamilyName 列族名
   * @param columnName 列名
   * @param columnValue 列值
   */
  def putData(tableNameStr: String, rowkey: String, columnFamilyName: String, columnName: String, columnValue: String) = {
    // 获取表
    val table: Table = getTable(tableNameStr, columnFamilyName)

    try {
      // Put
      val put: Put = new Put(rowkey.getBytes)
      put.addColumn(columnFamilyName.getBytes, columnName.getBytes, columnValue.getBytes)

      // 保存数据
      table.put(put)
    } catch {
      case ex: Exception => ex.printStackTrace()
    } finally {
      table.close()
    }
  }

  /**
   * 通过单列名获取列值
   * @param tableNameStr
   * @param rowkey
   * @param columnFamilyName
   * @param columnName
   */
  def getData(tableNameStr: String, rowkey: String, columnFamilyName: String, columnName: String): String = {
    // 1、获取table对象
    val table = getTable(tableNameStr, columnFamilyName)

    try {
      // 2、构建Get对象
      val get = new Get(rowkey.getBytes)
      // 3、进行查询
      val result: Result = table.get(get)
      // 4、判断查询结果是否为空，并且包含我们要查询的列
      if (result!=null && result.containsColumn(columnFamilyName.getBytes, columnName.getBytes)) {
        val bytes: Array[Byte] = result.getValue(columnFamilyName.getBytes, columnName.getBytes)

        Bytes.toString(bytes)
      } else {
        ""
      }
    } catch {
      case e: Exception => {
        e.printStackTrace()
        ""
      }
    } finally {
      // 5、关闭表
      table.close()
    }

  }


  /**
   * 存储多列数据
   * @param tableNameStr
   * @param rowkey
   * @param columnFamilyName
   * @param map 多个列名和列值集合
   */
  def putMapData(tableNameStr: String, rowkey: String, columnFamilyName: String, map: Map[String, Any]) = {
    // 1、获取Table
    val table = getTable(tableNameStr, columnFamilyName)
    try {
      // 2、创建Put
      val put = new Put(rowkey.getBytes)
      // 3、在Put中添加多个列名和列值
      for ((colName, colValue) <- map) {
        put.addColumn(columnFamilyName.getBytes, colName.getBytes, colValue.toString.getBytes)
      }
      // 4、保存Put
      table.put(put)
    } catch {
      case e: Exception => {
        e.printStackTrace()
        ""
      }
    } finally {
      // 5、关闭表
      table.close()
    }
  }

  /**
   * 获取多列数据的值
   * @param tableNameStr
   * @param rowkey
   * @param columnFamilyName
   * @param columnNameList 多个列名
   * @return 多个列名和多个列值的Map集合
   */
  def getMapData(tableNameStr: String, rowkey: String, columnFamilyName: String, columnNameList: List[String]):Map[String, String] = {
    // 1、获取table
    val table = getTable(tableNameStr, columnFamilyName)
    try {
      // 2、构建Get对象
      val getRK = new Get(rowkey.getBytes)

      // 3、执行查询
      val result = table.get(getRK)

      // 4、遍历列名集合，取出列值，构建成Map返回
      columnNameList.map{
        col =>
          val bytes: Array[Byte] = result.getValue(columnFamilyName.getBytes(), col.getBytes)

          // 返回Map对象
          if (bytes != null && bytes.size > 0) {
            col -> Bytes.toString(bytes)
          } else {
            "" -> ""
          }
      }.filter(_._1 != "").toMap // 过滤掉空值
    } catch {
      case e: Exception => e.printStackTrace()
      Map[String, String]()
    } finally {
      // 5、关闭table
      table.close()
    }
  }

  /**
   * 删除数据
   * @param tableNameStr
   * @param rowkey
   * @param columnFamilyName
   */
  def deleteData(tableNameStr: String, rowkey: String, columnFamilyName: String) = {
    // 1、获取table
    val table: Table = getTable(tableNameStr, columnFamilyName)

    try {
      // 2、构建Delete对象
      val delete: Delete = new Delete(rowkey.getBytes)

      // 3、执行删除
      table.delete(delete)
    } catch {
      case e: Exception =>
        e.printStackTrace()
    } finally {
      // 4、关闭table
      table.close()
    }
  }

  def main(args: Array[String]): Unit = {
     println(getTable("test", "info"))

    // 保存数据
    // putData("test", "1", "info", "t1", "Hello World")

    // println(getData("test", "1", "info", "t1"))

    val map = Map(
      "t2" -> "a",
      "t3" -> "b",
      "t4" -> "c"
    )

    // putMapData("test", "1", "info", map)
    // println(getMapData("test", "1", "info", List("t1", "t2")))

//    deleteData("test", "1", "info")
//    println(getMapData("test", "1", "info", List("t1", "t2")))
  }
}
