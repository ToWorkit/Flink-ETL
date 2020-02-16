package com.pyg.realprocess.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Admin, ColumnFamilyDescriptor, ColumnFamilyDescriptorBuilder, Connection, ConnectionFactory, Put, Table, TableDescriptor, TableDescriptorBuilder}

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

  def main(args: Array[String]): Unit = {
    // println(getTable("test", "info"))

    // 保存数据
    putData("test", "1", "info", "t1", "Hello World")
  }
}
