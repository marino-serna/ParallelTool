package com.tools.parallelTool

import org.apache.spark.sql.DataFrame

trait Storage {
  def write(sourceName:String, elementName:String, elementValue:DataFrame, partitions:List[String] = Nil)
  def read(sourceName:String, elementName:String):DataFrame
}
