package com.github.marino_serna.parallel_tool

import org.apache.spark.sql.DataFrame

trait Storage {
  def write(sourceName:String, elementName:String, elementValue:DataFrame, partitions:List[String] = Nil)
  def read(sourceName:String, elementName:String):DataFrame
  def exists(sourceName:String, elementName:String):Boolean
}
