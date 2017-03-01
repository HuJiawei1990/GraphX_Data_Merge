package com.datageek.arvato

/**
  * Created by hjw on 2017/2/21.
  */

import java.io.File

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

import org.apache.commons.io.FileUtils

object readFromDF {
    val sqlConnect = 0
    Logger.getLogger("org").setLevel(Level.WARN)

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("testJoinDF").setMaster("local")
        val sc = new SparkContext(conf)
        val sqlContext = new SQLContext(sc)

        import sqlContext.implicits._

        val dataDir = "./src/test/data/"
        FileUtils.deleteDirectory(new File("./target/data/"))

        // ====== Graph node : all ID information

        if (sqlConnect == 1) {
            val allIdValuesDF = sqlContext.table("allIDValues")
            val srcTableListDF = sqlContext.table("tb_source_wt")
            val idNameListDF = sqlContext.table("tb_IDType_wt")

            val allIdDf1 = allIdValuesDF.join(srcTableListDF, allIdValuesDF("TableName") === srcTableListDF("tb_name"), "left_outer")
            val allIdDf2 = allIdDf1.join(idNameListDF, allIdDf1("IDName") === idNameListDF("ID_Type"), "left_outer")
        } else {
            val allIdFile = "allIdValues_o.csv"
            val allIdLine = sc.textFile(dataDir + allIdFile)
            val allIdDF = allIdLine.map(line => line.split("\t"))
              .map { fields =>
                  (fields(0).toLong, // vertex Id
                    fields(1), // source table
                    fields(2), // ID name (column name)
                    fields(3), // ID type
                    fields(4), // ID value
                    fields(5).toInt) //date intervals
              }.toDF()

            //load weight for different tables
            val srcTableFile = "srcTableList.csv"
            val srcTableListDF = sc.textFile(dataDir + srcTableFile).map(line => line.split("\t"))
            .map{
                fields => (fields(0), fields(1).toDouble)
            }.toDF()

            // load IDName's weight from csv file
            val idNameFile = "idName.csv"
            val idNameListDF = sc.textFile(dataDir + idNameFile).map(line => line.split("\t")).
              map{
                  fields => (fields(0), fields(1))
              }.toDF()
            //idNameListDF.show()

            // JOIN the three Data Frame
            val allIdDf1 = allIdDF.join(srcTableListDF, allIdDF("_2") === srcTableListDF("_1"), "left_outer")
            val allIdDf2 = allIdDf1.join(idNameListDF, allIdDf1("_4") === idNameListDF("_1"), "left_outer")

            allIdDf2.show()

            // Convert data frame into vertex rdd
            val allId: RDD[(VertexId, String, String, String, String, Double, Int)] =
                allIdDf2.rdd.map({ row => (
                  row.get(0).toString.toLong,
                  row.get(1).toString,
                  row.get(2).toString,
                  row.get(3).toString,
                  row.get(4).toString,
                  row.get(7).toString.toDouble *
                    ( 1 + math.log(1 + 1 / row.get(9).toString.toDouble) / math.log(2)), // ID weight
                  row.get(5).toString.toInt)
                })

            allId.repartition(1).saveAsTextFile("./target/data/")

            sc.stop()
        }


    }

}
