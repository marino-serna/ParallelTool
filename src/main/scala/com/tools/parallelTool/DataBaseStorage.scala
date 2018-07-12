package com.tools.parallelTool

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * This class is an example of the implementation of storage.
  *
  *
  *
  * @param spark spark session
  * @param databasePath Path where the files with the data of the tables are located.
  *                     This field is only required if use partitions at writing
  */
class DataBaseStorage(spark: SparkSession, databasePath: String) extends Storage{
  import spark.implicits._

  def write(sourceName:String, elementName:String, elementValue:DataFrame, partitions:List[String]):Unit={
    val dfReadyToWrite = if(elementValue.schema.fieldNames.exists(isNormalizedRequired)){
      normalizeColumnNameDF(elementValue)
    }else{elementValue}

    writeTable(dfReadyToWrite, s"$sourceName.$elementName", partitions)
  }
  def read(sourceName:String, elementName:String):DataFrame={
    spark.read.table(s"$sourceName.$elementName")
  }

  private val invalidTableNameCharacters:List[String] =  " "::","::";"::"\\{"::"\\}"::"\\("::"\\)"::"\n"::"\t"::"="::Nil
  private val unwantedTableNameCharacters:List[String] = "'" :: "|" :: "." ::Nil

  private def isNormalizedRequired(name:String):Boolean = {
    val columnNameClean:List[String] = invalidTableNameCharacters:::unwantedTableNameCharacters
    val hasForbiddenCharacter = columnNameClean.exists(p => name.contains(p))
    val startByNum = name.toList match {
      case num :: _ if Character.isDigit(num) =>  true
      case _ => false
    }
    hasForbiddenCharacter || startByNum
  }

  private def normalizeColumnNameDF(df:DataFrame):DataFrame = {
    df.schema.fieldNames.foldLeft(df)(
      (elementValue,originalColumnName) => elementValue.withColumnRenamed(originalColumnName, normalizeColumnName(originalColumnName)))
  }

  private def normalizeColumnName(value:String):String={
    //create valid names for columns
    val columnNameCandidate = if(value != null){value.toLowerCase.replace(" ","_")}else{""}
    val columnNameClean:String = (invalidTableNameCharacters:::unwantedTableNameCharacters)
      .foldLeft(columnNameCandidate)((name,characterToClean) => name.replaceAll(characterToClean,""))
    val columnName = (columnNameClean.toList match {
      case num :: _ if Character.isDigit(num) =>  s"_$columnNameClean"
      case _ => columnNameClean
    })
    columnName
  }

  /**
    * Write a DataFrame in to a Table.
    * if partitions are defined only partitions with data will be updated, that means that not all the
    * @param dF
    * @param schemaAndTable
    * @param partitions
    */
  def writeTable(dF: DataFrame, schemaAndTable: String,partitions:List[String]=Nil): Unit ={
    try{
      if(spark.catalog.tableExists(schemaAndTable) && partitions.nonEmpty){
        val schema :: table :: _ = schemaAndTable.split('.').toList

        partitions.map(partitionField => (partitionField, dF.select(partitionField).distinct()))
          .foldLeft(("/",dF)::Nil :List[(String,DataFrame)])(
            (processed,current) => {
              val partitionField = current._1
              val partitionValues = current._2.collect().map(_(0).toString).toList

              partitionValues.map(partitionValue =>
                processed.map(partitioned => {
                  val previousPartitionPath = partitioned._1
                  val dataBelongingToPartition:DataFrame = partitioned._2

                  (s"${previousPartitionPath}${partitionField}=$partitionValue/",
                    dataBelongingToPartition.filter($"${partitionField}" === partitionValue))
                })
              ).foldLeft(Nil:List[(String,DataFrame)])((ready,processing) => processing ::: ready )
            }).par.map(partition => {
          val partitionPath = partition._1
          val dataBelongingToPartition:DataFrame = partition._2

          dataBelongingToPartition.write.format("parquet").mode(SaveMode.Overwrite)
            .save(s"$databasePath${schema}/$table$partitionPath")
        }
        )
      }else{
        dF.write.format("parquet").mode(SaveMode.Overwrite).partitionBy(partitions:_*).saveAsTable(schemaAndTable)
      }
    }catch{
      case ex:Throwable =>
        MyLogger.error(s"writeTable(DataFrame, $schemaAndTable, ${partitions.mkString(" | ")}): ${ex.getMessage}")
        throw ex
    }
  }
}

private object MyLogger{ // this object fix the problem of Logger not been serializable
  private val logger: Logger = Logger.getLogger("DataBaseStorage")
  def error(message:String):Unit={ logger.error(message)}
  def info(message:String):Unit={ logger.info(message)}
}