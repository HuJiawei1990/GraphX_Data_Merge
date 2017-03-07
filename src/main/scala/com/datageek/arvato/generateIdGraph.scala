package com.datageek.arvato

/**
  * Created by Administrator on 2017/2/10.
  */
import java.io.File
import java.time.LocalTime

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object generateIdGraph {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val myInfoLevel = 1

    def main(args: Array[String]): Unit = {
        // spark initialization
        //val conf = new SparkConf().setAppName("generateAllId")
        val conf = new SparkConf().setAppName("generateAllId").setMaster("local")
        val sc = new SparkContext(conf)
        val sqlContext = new SQLContext(sc)
        import sqlContext.implicits._

        // define variables
        /**
          * prepares some variables for testing
          */
        val sourceType = "CUST_ID"

        val firstTypeEdgeWeight = 0         // weight for edge type I
        val secondTypeEdgeWeight = 1        // weight for edge type II

        // Define the output directory path
        val outputDir = "./target/output/"
        FileUtils.deleteDirectory(new File(outputDir))


        /** ********** step 1 **********
          * Load graph information from csv files
          * generate the ID connection graph
          */
        val dataDir = "./src/test/data/"

        /** Read vertex information
          * JOIN the vertex id & source table weight and ID type weight
          */
       // val allIdFile = "allIdValues_o.csv"
       // val allIdLine = sc.textFile(dataDir + allIdFile)
       //val allIdDF = allIdLine.map(line => line.split("\t")).map { fields =>
       //       (fields(0).toLong, // vertex Id
       //         fields(1), // source table
       //         fields(2), // ID name (column name)
       //         fields(3), // ID type
       //         fields(4), // ID value
       //         fields(5).toInt) //date intervals
       // }.cache().toDF()

        //val allIdDF: DataFrame = sqlContext.sql("select * from arvato_temp.allIDValues_test")

        //load weight for different tables
       // val srcTableFile = "srcTableList.csv"
       // val srcTableListDF = sc.textFile(dataDir + srcTableFile).map(line => line.split("\t"))
       // .map{
       //     fields => (fields(0), fields(1))
       // }.cache().toDF()

        //val srcTableListDF: DataFrame = sqlContext.sql("select *  from arvato_temp.tb_source_wt_test")

        // load IDName's weight from csv file
       // val idNameFile = "idName.csv"
       // val idNameListDF = sc.textFile(dataDir + idNameFile).map(line => line.split("\t")).
       //   map{
       //       fields => (fields(0), fields(1))
       //   }.cache().toDF()

        //val idNameListDF: DataFrame = sqlContext.sql("select * from arvato_temp.tb_IDType_wt_test")

        // JOIN the three Data Frame
        //val allIdDf1 = allIdDF.join(srcTableListDF, allIdDF("tablename") === srcTableListDF("tb_name"), "left_outer")
        //val allIdDf2 = allIdDf1.join(idNameListDF, allIdDf1("idname") === idNameListDF("id_type"), "left_outer")

        // Convert data frame into vertex rdd
       // val allId: RDD[(VertexId, ((String, String, String, String, Double), Int))] =
       //     allIdDf2.rdd.map{ row => (
       //       row.get(0).toString.toLong,
       //       ((row.get(1).toString,
       //       row.get(2).toString,
       //       row.get(3).toString,
       //       row.get(4).toString,
       //       orderToWgt(row.get(7).toString.toDouble) *
       //         orderToWgt(row.get(9).toString.toDouble)),
       //       row.get(5).toString.toInt))
       //     }.cache()


        // ====== Graph node : all ID information
        val allIdFile = "allIdValues.csv"
        val allIdLine = sc.textFile(dataDir + allIdFile)
        val allId: RDD[(VertexId, ((String, String, String, String, Double), Int))] = allIdLine.map {
            line =>
                val fields = line.split("\t")
                (fields(0).toLong, // vertex ID
                  ((fields(1), // source table
                    fields(2), // ID name (column name)
                    fields(3), // ID type
                    fields(4), // ID value
                    fields(6).toDouble *
                    //orderToWgt(fields(6).toInt) *
                      orderToWgt(fields(7).toInt, isAscending = true)),
                    fields(5).toInt) // days difference from now to last update time
                )
          }

        if (myInfoLevel >= 1) {
            println("********** hjw test info **********")
            println("*** There are " + allId.count() + " nodes.")
        }

        // ===== Graph edges
        // ===== type I : all ID pairs from the same table
        val IdParisFile1 = "associatedIdPairs.csv"
        val IdPairs1: RDD[Edge[Int]] = sc.textFile(dataDir + IdParisFile1).map {
         //val IdPairs1DF: DataFrame = sqlContext.sql("select * from arvato_temp.AssociatedIDPairs_test")
         //val IdPairs1: RDD[Edge[Int]] = IdPairs1DF.rdd.map{
            line =>
                val fields = line.split(",")
                Edge(fields(1).toLong,      // source node ID
                    fields(2).toLong,       // destination node ID
                    firstTypeEdgeWeight     // relationship type => from the same table
                )
        }

        // ===== type II: all ID pairs have the same value
        val IdParisFile2 = "associatedKeyByValue.csv"
        val IdPairs2: RDD[Edge[Int]] = sc.textFile(dataDir + IdParisFile2).map {
        //val idPairs2DF: DataFrame = sqlContext.sql("select * from arvato_temp.AssociatedKeyByValue_test")
        //val IdPairs2: RDD[Edge[Int]] = idPairs2DF.rdd.map {
            line => val fields = line.split("\t")
                Edge( fields(1).toLong,      // source node ID
                    fields(2).toLong,       // destination node ID
                    secondTypeEdgeWeight    // relationship type => from the same table
                )
        }

        if (myInfoLevel >= 1) {
            println("********** hjw test info **********")
            println("*** There are " + IdPairs1.count() + " connections of type I.")
            println("*** There are " + IdPairs2.count() + " connections of type II.")
        }

        val IdPairs = IdPairs1.union(IdPairs2)
        val graph = Graph(allId, IdPairs)

        // ====== print out the whole graph
        if (myInfoLevel >= 2) {
            println("********** hjw debug info **********")
            val details = graph.triplets.map(
                triplet => triplet.srcAttr._1._2 + " from table " + triplet.srcAttr._1._1 + " with values " + triplet.srcAttr._1._4 +
                  " is connected with " +
                  triplet.dstAttr._1._2 + " from table " + triplet.dstAttr._1._1 + " with values " + triplet.dstAttr._1._4 +
                  " with type " + triplet.attr
            )
            println(details.collect().mkString("\n"))
        }

        // create a undirected graph by adding the reverse of the original graph
        var nonDirectedGraph = Graph(graph.vertices, graph.edges.union(graph.reverse.edges))

        if (myInfoLevel >= 1) {
            println("********** hjw test info **********")
            println("*** There are " + nonDirectedGraph.edges.count() + " connections in final graph.")
        }

        /** ********** step 2.A **********
          * Update the time information
          */

        // generate the sub-graph which only contains the time information
        var IdUpdateTime: RDD[(VertexId, Int)] = allId.map {
            line => (line._1, line._2._2)
        }.map { vertex =>
                if (vertex._2 < 0) (vertex._1, Int.MaxValue) // change value -1 to Inf
                else vertex
        }.cache()

        if (myInfoLevel > 1) {
            println("********** hjw test info **********")
            println("*** There are " + IdUpdateTime.count() + " vertices counted.")
            println(IdUpdateTime.collect.mkString("\n"))
        }

        // generate a sub-graph for time computation
        val TimeGraph = Graph(IdUpdateTime, IdPairs2)

        // aggregate time message: by edge type II
        IdUpdateTime = TimeGraph.aggregateMessages[Int](
            triplet => {
                // Send Message
                if (triplet.srcAttr < Int.MaxValue)
                    triplet.sendToDst(triplet.srcAttr)
                else if (triplet.dstAttr < Int.MaxValue)
                    triplet.sendToSrc(triplet.dstAttr)
            },
            (time1, time2) => math.min(time1, time2)    // Merge Message
        )

        // Update all vertices' update time to last information
        val updateTimeGraph = TimeGraph.joinVertices(IdUpdateTime) {
            case (_, oldDate, newDate) => math.min(oldDate, newDate.toInt)
        }.cache()

        if (myInfoLevel > 1) {
            println("********** hjw test info **********")
            println("*** There are " + IdUpdateTime.count() + " vertices counted.")
            updateTimeGraph.vertices.repartition(1).sortBy(vertex => vertex._1).saveAsTextFile(outputDir + "/timeGraph2/")
        }

        // Add time information to the original graph
        nonDirectedGraph = nonDirectedGraph.joinVertices(updateTimeGraph.vertices) {
            case (_, attr, updateTime) => (attr._1, updateTime)
        }.cache()

        //for (sourceId <- sourceIDList) {
        /** For a given source Vertex,
          *     we run step 2.B + 2.C + 2.D to define all vertices' weight
          * @param sourceId : source vertex ID
          * TODO: change the name of function
          */
        def hjwTest(sourceId: Long): Unit = {
            /** ********** step 2.B **********
              * shortest path algorithm
              * count the jump times from one table to another when joining them
              */
            val startTime = LocalTime.now().toSecondOfDay()
            val sourceIdValue: String = allId.filter(_._1 == sourceId).map(_._2._1._4).take(1)(0)

            // Define a initial graph which has the same structure with the original graph
            // vertices has one attribute at beginning
            // for source Vertex ID => 0.0
            // for the others       => Inf
            val initialGraph = nonDirectedGraph.mapVertices(
                (id, _) =>
                    if (id == sourceId) 0.0
                    else Double.PositiveInfinity
            )

            /** Find the shortest path from source vertex to the other vertices
              * we can define the weight for different edge types
              * we store the min sum of edge-weights from the source vertex to destination vertex
              * if there is path link them
              * Otherwise, we store the sum as Double.positiveInfinity
              */
            val shortestPathGraph = initialGraph.pregel(Double.PositiveInfinity)(
                (_, dst, newDst) => math.min(dst, newDst),
                triplet => {
                    // Send Message
                    if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
                        Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
                    } else {
                        Iterator.empty
                    }
                },
                (a, b) => math.min(a, b) // Merge Message
            )

            if (myInfoLevel > 1) {
                val connectedVertices = shortestPathGraph.vertices.filter {
                    case (_, pathLength) => pathLength < Double.PositiveInfinity
                }

                println("********** hjw test info **********")
                println("*** There are " + connectedVertices.count() + " vertices connected to vertex ID = " + sourceId)
                println(connectedVertices.collect.mkString("\n"))
            }

            val endTime1 = LocalTime.now().toSecondOfDay()

            /** ********** step 2.C **********
              * compute final weight for each vertex
              */

            /**  add the min sum of edge-weights as a new attribute into vertex.attr
              *  filter all vertices whose new attribute < inf
              *  i.e. these vertices are connected to the source vertex
              */
            val connectedVerticesAllInfo = nonDirectedGraph.outerJoinVertices(shortestPathGraph.vertices) {
                case (_, attr, Some(pathLength)) => ((attr._1._3, attr._1._4,
                  computeWeight(attr._1._5, pathLength, attr._2, timeDecayModel = "log" ,T_total = 3600, T_half = 2000)), pathLength)
                 // TODO: change params of time decay
                case (_, attr, None) => ((attr._1._3, attr._1._4, attr._1._5), Double.PositiveInfinity)
            }.vertices.filter {
                case (_, attr) => attr._2 < Double.PositiveInfinity
            }

            val endTime2 = LocalTime.now().toSecondOfDay()

            /**
              * ********** step 2.D
              * Sum all vertices' weights with same ID value
              * i.e. all vertices connected bu edge type II
              */

            // Create a sub-graph contains all ID vertices connected to source id
            // and type II edges which connect these vertices
            val defaultVertex = (("NULL", "NULL", 0.0), Double.PositiveInfinity)
            var allInfoGraph = Graph(connectedVerticesAllInfo, IdPairs2, defaultVertex).subgraph(
                vpred = (_, attr) => (attr._2 < Double.PositiveInfinity) && (attr._1._1 != sourceType)
            )

            allInfoGraph = Graph(allInfoGraph.vertices, allInfoGraph.reverse.edges.union(allInfoGraph.edges))

            if (myInfoLevel > 1) {
                println("**************** hjw test info ********************")
                println(" *** The new graph created for ID vertex " + sourceId)
                println(" *** there are " + allInfoGraph.numVertices + " vertices and " + allInfoGraph.numEdges + " Edges.")
                println(allInfoGraph.vertices.collect().mkString("\n"))
            }

            // Gathering message from all its neighbours (for edge type II)
            val cntedVerticesWgt = allInfoGraph.mapVertices(
                (_, attr) => attr._1._3
            ).aggregateMessages[Double](
                triplet => triplet.sendToDst(triplet.srcAttr),
                _ + _
            )

            // aggregate message : original message + all received message
            // and get only the first value to avoid duplicating information
            val cntedVerticesFinalInfo = allInfoGraph.outerJoinVertices(cntedVerticesWgt) {
                case (_, attr, Some(fWgt)) => (attr._1._1, attr._1._2, attr._1._3 + fWgt)
                case (_, attr, None) => attr._1
            }.vertices.map(vertex => vertex._2).map(
                prop => ((prop._1, prop._2), prop._3)
            ).reduceByKey((a, _) => a).map(
                attr => (attr._1._1, attr._1._2, attr._2)
            ).sortBy(vertex => (vertex._1, vertex._3), ascending = false)

            // Save all information as file
            if (myInfoLevel > 0) {
                cntedVerticesFinalInfo.repartition(1)
                  .saveAsTextFile(outputDir + "/allInfo_" + sourceId.toString + "/")
            }

            // TODO: insert the result cntedVerticesFinalInfo into table T_merge (HBase)

            val endTime = LocalTime.now().toSecondOfDay
            val runTime = endTime - startTime

            if (myInfoLevel > 0){
                println("=========== hjw test info ==========")
                println("Finding all connected vertices to Vertex No. " + sourceId.toString)
                println("*** " + sourceType + " = " + sourceIdValue + " ***")
                println("Run time: " + runTime + " seconds.")
                println("===> step 2.B " + (endTime1 - startTime) + " seconds.")
                println("===> step 2.C " + (endTime2 - endTime1) + " seconds.")
                println("===> step 2.D " + (endTime - endTime2) + " seconds.")
                println("=========== hjw info end  ===============")
            }
        }

        /** ********** step 3 **********
          * Get all Id vertices whose type == source ID type.
          * Generate a list and find all information of
          * vertices connected to source vertex.
          * Run iterations for all source vertex.
          */
        val diffValues = allId.filter(vertex => vertex._2._1._3 == sourceType)
          .map(vertex => (vertex._2._1._4, vertex._1))
          .reduceByKey((a, _) => a).map(_._2)

        val numSrcId = diffValues.count().toInt
        val sourceIDList:Array[Long] = diffValues.take(numSrcId)

        // Run iterations for all possible source vertex IDs
        // TODO: change name of function hjwTest
        for (vid <- sourceIDList) hjwTest(vid)

        sc.stop()
    }


    /**
      * Convert order information into a calculative real number between 0 and 1
      * deal with the ID's order and source Tables' order
      * @param order        order number
      * @param maxOrder     the maximum order(only served for descendant mode),
      *             default = 10;
      * @param isAscending
      *             true(default):   order No. is small => more important
      *             false:           order No. is large => more important
      * @return    the weight value based on order, the value is between 0 and 1
      */
    def orderToWgt(order: Double, maxOrder: Double = 10, isAscending: Boolean = true): Double = {
        if (isAscending) (1 + math.log(1 + 1 / order) / math.log(2)) / 2.0
        else {
            (1 + math.log(1 + 1 / (maxOrder - order + 1)) / math.log(2)) / 2.0
        }
    }


     /**
       * This function is used to compute the time delay coefficient by a logarithm model
       * @param daysDiff: difference in days from now to last update time
       * @param T_half:   days when this coef reduces to 0.5
       *               default = 200;
       * @param T_total:  days when this coef reduces to 0
       *               default = 360
       * @param defaultTimeCoef: the default coefficient when the information is missing
       *               default = 0.5;
       * @return
       *         time decay coefficient between 0 and 1
       * @note
       *       In this model we should make sure that
       *        0.5 * T_total < T_half < T_total
       */
    def timeDecayLog(daysDiff: Int, T_half: Int = 200, T_total: Int = 360,
                     defaultTimeCoef: Double = 0.5): Double = {
        if (daysDiff == Int.MaxValue || daysDiff < 0) defaultTimeCoef
        else if (daysDiff >= T_total) 0.0
        else {
            val gamma: Double = (2 * T_half - T_total).toDouble / ((T_total - T_half) * (T_total - T_half)).toDouble
            //println("we calculate the gamma = " + gamma.toString)
            math.log((T_total - daysDiff) * gamma + 1) / math.log(T_total * gamma + 1)
            //println("the result is " + result.toString)
        }
    }


    /**
      * This function is used to compute the time delay coefficient by a exponential model
      * @param daysDiff: difference in days from now to last update time
      * @param T_half:   days when this coef reduces to 0.5
      *               default = 100;
      * @param T_total:  days when this coef reduces to 0
      *               default = 360
      * @param defaultTimeCoef: the default coefficient when the information is missing
      *               default = 0.5;
      * @return
      *         time decay coefficient between 0 and 1
      * @note
      *       In this model we should make sure that
      */
    def timeDecayExp(daysDiff: Int, T_half: Int = 100 , T_total: Int = 360, defaultTimeCoef: Double = 0.5): Double = {
        if (daysDiff == Int.MaxValue || daysDiff < 0) defaultTimeCoef
        else if (daysDiff < 0.5 * T_half) 1.0
        else {
            val alpha =  1 / math.pow(2, 2 / T_half)
            math.pow(alpha, daysDiff)
        }
    }

    /**
      * This function is used to compute the time delay coefficient by a sigmoid model
      * @param daysDiff: difference in days from now to last update time
      * @param T_quad:   days when this coef reduces to 0.75
      *               default = 100;
      * @param T_half:  days when this coef reduces to 0.5
      *               default = 180
      * @param defaultTimeCoef: the default coefficient when the information is missing
      *               default = 0.5;
      * @return
      *         time decay coefficient between 0 and 1
      * @note
      *       In this model we should make sure that
      */
    def timeDecaySig(daysDiff: Int, T_quad: Int = 100 , T_half: Int = 180, defaultTimeCoef: Double = 0.5): Double = {
        if (daysDiff == Int.MaxValue || daysDiff < 0) defaultTimeCoef
        else {
            val alpha = math.log1p(3) / (T_half - T_quad)
            1 - 1 / (1 + math.exp( - (daysDiff - T_half) * alpha) )
        }
    }


    /**
      * Compute the final weight coefficient of ID
      * @param w1            basic weight of ID which defined by source table and ID type
      * @param numJumps     number of jumps between different tables from source ID to the destination
      * @param updateTime   number of days from the latest update time to now
      * @param alpha        Restore information when joining between tables
      *           default = 0.9;
      * @param timeDecayModel   choose a model to calculate time decay coefficient
      *           "log"(default):   logarithm model
      *           "exp":            exponential model
      *           "sig":            sigmoid function model
      * @param T_quad       days when time decay coefficient reduces to 0.75
      *           default = 100, used by sifmoid model;
      * @param T_half       days when time decay coefficient reduces to 0.5
      *           default = 200;
      * @param T_total      days when time decay coefficient reduces to 0
      *            default = 360;
      * @param defaultTimeCoef     the default coefficient when the information is missing
      *            default = 0.5;
      * @return
      *            the final weight to the vertex, result keep 4 decimals
      * @note
      *       Be careful with the "sig" model, its parameters are different from the others
      */
    def computeWeight(w1: Double, numJumps: Double, updateTime: Int, alpha: Double = 0.9,
                      timeDecayModel: String = "log", T_quad: Int = 100,
                      T_half: Int = 200, T_total:Int = 360, defaultTimeCoef:Double = 0.5
                     ): Double = {
        //val alpha = 0.9

        val timeCoef = timeDecayModel match {
            case "log" => timeDecayLog(updateTime, T_half, T_total, defaultTimeCoef)
            case "exp" => timeDecayExp(updateTime, T_half, T_total, defaultTimeCoef)
            case "sig" => timeDecaySig(updateTime, T_quad, T_half, defaultTimeCoef)
            case _ => 1.0
        }
        val result = w1 * math.pow(alpha, numJumps) * timeCoef
        // keep 4 decimals
        (result * 10000).toInt / 10000.0
    }


//  def deleteRecursively(file: File): Unit = {
//    if (file.isDirectory)
//      file.listFiles.foreach(deleteRecursively)
//    if (file.exists && !file.delete)
//      throw new Exception(s"Unable to delete ${file.getAbsolutePath}")
//  }


//  def generateShortestPathGraph(srcGraph: Graph[VD, Int] , srcId: VertexId): Graph[VD, Int] = {
 //   val initialGraph = srcGraph.mapVertices(
 //     (id, _) =>
 //       if (id == srcId) 0.0
 //       else Double.PositiveInfinity
 //   )

//      val shortestPathGraph = initialGraph.pregel(Double.PositiveInfinity)(
//          (id, dst, newDst) => math.min(dst, newDst),
//          triplet => {  // Send Message
//              if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
//                  Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
//              } else {
//                  Iterator.empty
//              }
//          },
//          (a, b) => math.min(a, b) // Merge Message
//      )
//
//      // join the path length param to the source graph
//      // add the path length as a new attribute into all vertices
//      srcGraph.outerJoinVertices(shortestPathGraph.vertices){
//          case (vid, attr, Some(pathLength)) => (attr, pathLength)
//      }.vertices.filter {
//          case (_, attr) => attr._2 < Double.PositiveInfinity
//      }
//  }

}



