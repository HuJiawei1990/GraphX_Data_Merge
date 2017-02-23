package com.datageek.arvato

/**
  * Created by Administrator on 2017/2/10.
  */
import java.io.File

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{Row, SQLContext, SparkSession}

object generateIdGraph {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val testMode = 1
    val debugMode = 0
    val firstTypeEdgeWeight = 0
    val secondTypeEdgeWeight = 1

    def main(args: Array[String]): Unit = {
        // spark initialization
        val conf = new SparkConf().setAppName("generateAllId").setMaster("local")
        val sc = new SparkContext(conf)
        val sqlContext = new SQLContext(sc)
        import sqlContext.implicits._

        // define variables
        /**
          * prepares some variables for testing
          */
        //val sourceId: VertexId = 2L
        //val myIdType = "CUST_ID"
        val sourceType = "MOBILE"
        //val myIdType = "MOBILE"
        //val myIdType = "OPENID"
        //val myIdType = "EMAIL"
        //val alpha = 0.9 // this variable is defined as loss coefficient when table jump

        // Define the output directory path
        val outputDir = "./target/output/"
        FileUtils.deleteDirectory(new File(outputDir))
        // define weights of different properties of ID


        /**
          * ********** step 1 **********
          * Load graph information from csv files
          * generate the ID connection graph
          */
        val dataDir = "./src/test/data/"

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
                    //fields(5).toInt,
                    fields(6).toDouble *
                      orderToWgt(fields(7).toInt)),
                      //(1 + math.log(1 + 1 / fields(7).toDouble) / math.log(2))), // ID weight
                    0) // days difference from now to last update time
                )
        }

        if (testMode >= 1) {
            println("********** hjw test info **********")
            println("*** There are " + allId.count() + " nodes.")
        }

        // define a default ID type
        //val defaultId = ("NULL", "NULL", "NULL", "NULL", 0.0, -1)

        // ===== Graph edges
        // ===== type I : all ID pairs from the same table
        val IdParisFile1 = "associatedIdPairs.csv"
        val IdPairs1: RDD[Edge[Int]] = sc.textFile(dataDir + IdParisFile1).map {
            line =>
                val fields = line.split(",")
                Edge(fields(1).toLong, // source node ID
                    fields(2).toLong, // destination node ID
                    firstTypeEdgeWeight // relationship type => from the same table
                )
        }

        // ===== type II: all ID pairs have the same value
        val IdParisFile2 = "associatedKeyByValue.csv"
        val IdPairs2: RDD[Edge[Int]] = sc.textFile(dataDir + IdParisFile2).map {
            line =>
                val fields = line.split("\t")
                Edge(fields(1).toLong, // source node ID
                    fields(2).toLong, // destination node ID
                    secondTypeEdgeWeight // relationship type => from the same table
                )
        }

        if (testMode >= 1) {
            println("********** hjw test info **********")
            println("*** There are " + IdPairs1.count() + " connections of type 1.")
            println("*** There are " + IdPairs2.count() + " connections of type 2.")
        }

        val IdPairs = IdPairs1.union(IdPairs2)
        val graph = Graph(allId, IdPairs)

        // ====== output the whole graph
        if (debugMode >= 1) {
            println("********** hjw debug info **********")
            val details = graph.triplets.map(
                triplet => triplet.srcAttr._1._2 + " from table " + triplet.srcAttr._1._1 + " with values " + triplet.srcAttr._1._4 +
                  " is connected with " +
                  triplet.dstAttr._1._2 + " from table " + triplet.dstAttr._1._1 + " with values " + triplet.dstAttr._1._4 +
                  " with type " + triplet.attr
            )
            println(details.collect().mkString("\n"))
        }

        // create the non-directed graph by adding the reverse of the original graph
        var nonDirectedGraph = Graph(graph.vertices, graph.edges.union(graph.reverse.edges))

        if (testMode >= 1) {
            println("********** hjw test info **********")
            println("*** There are " + nonDirectedGraph.edges.count() + " connections in final graph.")
        }

        /**
          * ********** step 2.A **********
          * Update the time information
          */

        // generate the sub-graph which only contains the time information
        var IdUpdateTime: RDD[(VertexId, Int)] = allIdLine.map {
            line =>
                val fields = line.split("\t")
                (fields(0).toLong, // vertex ID
                  fields(5).toInt // days difference from now to last update time
                )
        }.map {
            vertex =>
                if (vertex._2 < 0) (vertex._1, Int.MaxValue) // change value -1 to Inf
                else vertex
        }

        if (testMode > 1) {
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
            (time1, time2) => math.min(time1, time2) // Merge Message
        )

        // Update all vertices' update time to last information
        val updateTimeGraph = TimeGraph.joinVertices(IdUpdateTime) {
            case (_, oldDate, newDate) => math.min(oldDate, newDate.toInt)
        }

        if (testMode >= 1) {
            println("********** hjw test info **********")
            println("*** There are " + IdUpdateTime.count() + " vertices counted.")
            updateTimeGraph.vertices.repartition(1).sortBy(vertex => vertex._1).saveAsTextFile(outputDir + "/timeGraph2/")
        }

        // Add time information to the original graph
        nonDirectedGraph = nonDirectedGraph.joinVertices(updateTimeGraph.vertices) {
            case (_, attr, updateTime) => (attr._1, updateTime)
        }

        /*
    // ====================================
    // ====== first exploration
    // ====================================

    // for a given ID value, find all its connected components
    val myIdValue = "D0BA55FB-3216-407D-95B0-B9C2C8BFF323"

    var selectedGraph = nonDirectedGraph.subgraph(
      //vpred = (id, attr) => attr._4 == myIdValue,
      epred = e => e.srcAttr._4 == myIdValue
    )

    // initialization
    var selectedVertices = nonDirectedGraph.triplets.collect {
      case triplet if triplet.srcAttr._4 == myIdValue && triplet.dstAttr._4 != myIdValue
      => (triplet.dstId, triplet.dstAttr)
    }

    if (testMode > 1) {
      println("********** hjw debug info **********")
      println("*** all IDs connected with ID value = " + myIdValue)
      println("*** there are " + selectedVertices.count() + " neighbours.")
      //selectedGraph.vertices.map{
      selectedVertices.map {
        vertex =>
          "Vertex ID " + vertex._1 + " from table " + vertex._2._1 +
            " in column " + vertex._2._2 + " with value " + vertex._2._4
      }.collect().map(println(_))
    }
    */


        //var sourceId: Long = 0L

        //for (sourceId <- sourceIDList) {
        /**
          * For a given source Vertex,
          *     we run step 2.B, 2.C, 2.D to define all vertices' weight
          * @param sourceId : source vertex ID
          * TODO: change name the fucntion
          */
        def hjwTest(sourceId: Long):Unit = {
            /**
              * ********** step 2.B **********
              * shortest path algorithm
              * count the jump times from one table to another when joining them
              */

            if (testMode >= 1) {
                println("=========== hjw test info ===============")
                println("Finding all connected vertices to Vertex No. " + sourceId.toString)
                println("=========== hjw info end  ===============")
            }

            // Define a initial graph which has the same structure with the original graph
            // vertices has one attribute at beginning
            // for source Vertex ID => 0.0
            // for the others       => Inf
            println("========= calculating for vertex No." + sourceId.toString)
            val initialGraph = nonDirectedGraph.mapVertices(
                (id, _) =>
                    if (id == sourceId) 0.0
                    else Double.PositiveInfinity
            )

            /** find the shortest path
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

            if (testMode > 1) {
                val connectedVertices = shortestPathGraph.vertices.filter {
                    case (_, pathLength) => pathLength < Double.PositiveInfinity
                }

                println("********** hjw test info **********")
                println("*** There are " + connectedVertices.count() + " vertices connected to vertex ID = " + sourceId)
                println(connectedVertices.collect.mkString("\n"))
            }

            /**
              * ********** step 2.C **********
              * compute final weight for each vertex
              */

            /**  add the min sum of edge-weights as a new attribute into vertex.attr
              *  filter all vertices whose new attribute < inf
              *  it means these vertices are connected to the
              */
            val connectedVerticesAllInfo = nonDirectedGraph.outerJoinVertices(shortestPathGraph.vertices) {
                case (_, attr, Some(pathLength)) => ((attr._1._3, attr._1._4,
                  computeWeight(attr._1._5, pathLength, attr._2, T_half = 2500, T_total = 3600)), pathLength)
                case (_, attr, None) => ((attr._1._3, attr._1._4, attr._1._5), Double.PositiveInfinity)
            }.vertices.filter {
                case (_, attr) => attr._2 < Double.PositiveInfinity
            }


            /**
              * ********** step 2.D
              * Sum all vertices' weights with same ID value
              * i.e. all vertices connected bu edge type II
              */

            // Create a sub-graph contains all ID vertices connected to source id
            // and type II edges which connect these vertices
            val defaultVertex = (("NULL", "NULL", 0.0), Double.PositiveInfinity)
            var allInfoGraph = Graph(connectedVerticesAllInfo, IdPairs2, defaultVertex).subgraph(
                vpred = (_, attr) => attr._2 < Double.PositiveInfinity
            )

            allInfoGraph = Graph(allInfoGraph.vertices, allInfoGraph.reverse.edges.union(allInfoGraph.edges))

            if (testMode > 1) {
                println("**************** hjw test info ********************")
                println(" *** The new graph created for ID vertex " + sourceId)
                println(" *** there are " + allInfoGraph.numVertices + " vertices and " + allInfoGraph.numEdges + " Edges.")
                println(allInfoGraph.vertices.collect().mkString("\n"))
            }

            // Gathering message from all its neighbours (for edge type II)
            val cntedVerticeWgt = allInfoGraph.mapVertices(
                (_, attr) => attr._1._3
            ).aggregateMessages[Double](
                triplet => triplet.sendToDst(triplet.srcAttr),
                _ + _
            )

            // aggregate message : original message + all received message
            // and get only the first value to avoid duplicating information
            val cntedVerticesFinalInfo = allInfoGraph.outerJoinVertices(cntedVerticeWgt) {
                case (_, attr, Some(fWgt)) => (attr._1._1, attr._1._2, attr._1._3 + fWgt)
                case (_, attr, None) => attr._1
            }.vertices.map(vertex => vertex._2).map(
                prop => ((prop._1, prop._2), prop._3)
            ).reduceByKey((a, _) => a)

            if (testMode > 0) {
                cntedVerticesFinalInfo.repartition(1)
                  .sortBy(vertex => vertex._1).saveAsTextFile(outputDir + "/allInfo_" + sourceId.toString + "/")
            }
        }

        /**
          * ********** step 3 **********
          * Get all Id vertices whose type == source ID type.
          * Generate a list and find all information of
          * vertices connected to source vertex.
          * Run iterations for all source vertex.
          */
        val diffValues = allId.filter(vertex => vertex._2._1._3 == sourceType)
          .map(vertex => (vertex._2._1._4, vertex._1))
          .reduceByKey((a,b) => a).map(_._2)

        val numSrcId = diffValues.count().toInt
        val sourceIDList:Array[Long] = diffValues.take(numSrcId)

        // Run iterations
        // TODO: change name of function hjwTest
        var i: Int = 0
        for (i <- sourceIDList) hjwTest(i)


        /*
    val aa = connectedVerticesAllInfo.map{
      case (_, attr) => ((attr._1._1, attr._1._2), attr._1._3)
    }.reduceByKey(_ + _)

    val aaa = connectedVerticesAllInfo.map{
      case (_, attr) => (attr._1._2, attr._1._3)
    }.reduceByKey(_ + _)

    if (testMode > 0){
      println("********** hjw debug info **********")
      println("*** There are " + connectedVerticesAllInfo .count() + " vertices connected to vertex ID = " + sourceId)
      println(connectedVerticesAllInfo.collect.mkString("\n"))

      println("********** hjw debug info **********")
      println(aa.collect.sortBy(vertex => vertex._1._2).mkString("\n"))

      println("********** hjw debug info **********")
      println(aaa.collect.mkString("\n"))
    }

    // =====================================
    // ===== filter by ID type
    // =====================================
    val connectedVerticesType = connectedVerticesAllInfo.filter {
        case (_, attr) => attr._1._1 == myIdType
    }

    if (testMode > 0){
      println("********** hjw test info **********")
      println("*** There are " + connectedVerticesType .count() + " vertices connected to vertex ID = "
        + sourceId + " with type " + myIdType )
      println(connectedVerticesType.collect.mkString("\n"))
    }

    //val a = connectedVerticesType.map {
    //  case (id, attr) => (attr._1._4, attr._1._5)
    //}.reduceByKey(_+_)

    if (testMode > 1) {
      println("********** hjw test info **********")
      println(aa.collect.mkString("\n"))
    }
    */
    }

    /**
      * Convert order information into a calculative real number between 0 and 1
      * deal with the ID's order and source Tables' order
      * @param order        order number
      * @param maxOrder     the maximum order(only served for descendant mode),
      *                    default = 10;
      * @param isAscendent
      *                    true(default):   order is smaller => more important
      *                    false:           order is larger => more important
      * @return     the weight value based on order, the value is between 0 and 1
      */
    def orderToWgt(order: Int, maxOrder:Int = 10, isAscendent: Boolean = true): Double = {
        if (isAscendent) (1 + math.log(1 + 1 / order.toDouble) / math.log(2)) / 2.0
        else {
            (1 + math.log(1 + 1 / (maxOrder - order + 1).toDouble) / math.log(2)) / 2.0
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


    // TODO: define exponential time decay model
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
        else if (daysDiff < 0.5 * T_half) defaultTimeCoef
        else 0.5
    }


    /**
      * Compute the final weight coefficent of ID
      * @param w1            basic weight of ID which defined by source table and ID type
      * @param numJumps     number of jumps between different tables from source ID to the destination
      * @param updateTime   number of days from the latest update time to now
      * @param alpha        Restore information when joining between tables
      *           default = 0.9;
      * @param timeDecayModel   choose a model to calculate time decay coefficient
      *           "log"(default):   logarithm model
      *           "exp":            exponential model
      * @param T_half       days when time decay coefficient reduces to 0.5
      *           default = 200;
      * @param T_total      days when time decay coefficient reduces to 0
      *            default = 360;
      * @param defaultTimeCoef     the default coefficient when the information is missing
      *            default = 0.5;
      * @return
      *            the final weight to the vertex, result keep 4 decimals
      */
    def computeWeight(w1: Double, numJumps: Double, updateTime: Int, alpha: Double = 0.9,
                      timeDecayModel: String = "log",
                      T_half: Int = 200, T_total:Int = 360, defaultTimeCoef:Double = 0.5
                     ): Double = {
        //val alpha = 0.9

        val timeCoef = timeDecayModel match {
            case "log" => timeDecayLog(updateTime, T_half, T_total, defaultTimeCoef)
            case "exp" => timeDecayExp(updateTime, T_half, T_total, defaultTimeCoef)
            case _ => defaultTimeCoef
        }

        val result = w1 * math.pow(alpha, numJumps) * timeCoef
        // keep 4 decimals
        (result * 10000).toInt / 10000.0
    }


    /*
  def deleteRecursively(file: File): Unit = {
    if (file.isDirectory)
      file.listFiles.foreach(deleteRecursively)
    if (file.exists && !file.delete)
      throw new Exception(s"Unable to delete ${file.getAbsolutePath}")
  }
  */

    /*
  def generateShortestPathGraph(srcGraph: Graph[VD, Int] , srcId: VertexId): Graph[VD, Int] = {
    val initialGraph = srcGraph.mapVertices(
      (id, _) =>
        if (id == srcId) 0.0
        else Double.PositiveInfinity
    )

    val shortestPathGraph = initialGraph.pregel(Double.PositiveInfinity)(
      (id, dst, newDst) => math.min(dst, newDst),
        triplet => {  // Send Message
          if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
            Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
          } else {
          Iterator.empty
          }
        },
      (a, b) => math.min(a, b) // Merge Message
    )

    // join the path length param to the source graph
    // add the path length as a new attribute into all vertices
    srcGraph.outerJoinVertices(shortestPathGraph.vertices){
      case (vid, attr, Some(pathLength)) => (attr, pathLength)
    }.vertices.filter {
      case (_, attr) => attr._2 < Double.PositiveInfinity
    }
  }
  */
}



