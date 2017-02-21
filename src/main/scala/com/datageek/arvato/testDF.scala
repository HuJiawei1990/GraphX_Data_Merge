package com.datageek.arvato

/**
  * Created by Administrator on 2017/2/21.
  */

import java.io.File

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

import org.apache.commons.io.FileUtils

object testDF {
    val sqlConnect = 0
    Logger.getLogger("org").setLevel(Level.WARN)

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("testJoinDF").setMaster("local")
        val sc = new SparkContext(conf)
        val sqlContext = new SQLContext(sc)

        import sqlContext.implicits._

        val dataDir = "./src/test/data/"

        // ====== Graph node : all ID information
        val allIdFile = "allIdValues.csv"
        val allIdLine = sc.textFile(dataDir + allIdFile)
        //        val allIdValues = sc.textFile(dataDir + allIdFile).map {
        //          line => val fields = line.split("\t")
        //            fields.
        //  }

        if (sqlConnect == 1) {
            val allIdValuesDF = sqlContext.table("allIDValues")
            val srcTableListDF = sqlContext.table("tb_source_wt")
            val idNameListDF = sqlContext.table("tb_IDType_wt")

            val allIdDf1 = allIdValuesDF.join(srcTableListDF, allIdValuesDF("TableName") === srcTableListDF("tb_name"), "left_outer")
            val allIdDf2 = allIdDf1.join(idNameListDF, allIdDf1("IDName") === idNameListDF("ID_Type"), "left_outer")
        } else {
            val allId: RDD[(Long, String, String, String, String, Int)] = allIdLine.map(line => line.split("\t"))
              .map { fields =>
                  (fields(0).toLong, // vertex Id
                    fields(1), // source table
                    fields(2), // ID name (column name)
                    fields(3), // ID type
                    fields(4), // ID value
                    fields(5).toInt) //date intervals
              }
            val allIdDF = allId.toDF()
            //allIdDF.show()
            val srcTableFile = "srcTableList.csv"
            val srcTableListDF = sc.textFile(dataDir + srcTableFile).map(line => line.split("\t"))
            .map{
                fields => (fields(0), fields(1).toDouble)
            }.toDF()

            val idNameFile = "idName.csv"
            val idNameListDF = sc.textFile(dataDir + idNameFile).map(line => line.split("\t")).
              map{
                  fields => (fields(0), fields(1))
              }.toDF()
            idNameListDF.show()

            val allIdDf1 = allIdDF.join(srcTableListDF, allIdDF("_2") === srcTableListDF("_1"), "left_outer")
            allIdDf1.show()
            val allIdDf2 = allIdDf1.join(idNameListDF, allIdDf1("_4") === idNameListDF("_1"), "left_outer")
            allIdDf2.show()

            // val aa = allIdDf2.map(line => line(0),line(1))
        }







      //  val allIdDF = sqlContext.createDataFrame(allId)


    }

}
