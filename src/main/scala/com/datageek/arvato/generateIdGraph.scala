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
import org.apache.commons.io.filefilter.WildcardFileFilter

object generateIdGraph {
  Logger.getLogger("org").setLevel(Level.WARN)
  val testMode = 1
  val debugMode = 0
  val firstTypeEdgeWeight = 0
  val secondTypeEdgeWeight = 1

  def main(args: Array[String]) : Unit = {
    // spark initialization
    val conf = new SparkConf().setAppName("generateTCUSTOMERId").setMaster("local")
    val sc = new SparkContext(conf)

    // define variables
    val sourceId: VertexId = 2L
    //val myIdType = "CUST_ID"
    //val myIdType = "MOBILE"
    val myIdType = "OPENID"
    //val myIdType = "EMAIL"
    val alpha = 0.9 // this variable is defined as loss coefficient when table jump

    val outputDir = "./target/output/"
    FileUtils.deleteDirectory(new File(outputDir))
    // define weights of different properties of ID

    if (testMode > 1) {
      println("********** hjw test info **********")
      println("time decay model output " + timeDecayLog(100).toString)
    }

    // ============================================
    // =========== Load graph from files ==========
    //  Generate the ID connection Graphs
    // ============================================

    // ====== Graph node : all ID information
    val allIdFile = "./src/test/data/allIdValues.csv"
    val allIdLine = sc.textFile(allIdFile)
    val allId: RDD[(VertexId, (String, String, String, String, Double))] = allIdLine.map {
      line =>
        val fields = line.split("\t")
        (fields(0).toLong, // vertex ID
          (fields(1), // source table
            fields(2), // ID name (column name)
            fields(3), // ID type
            fields(4), // ID value
            //fields(5).toInt,
            fields(6).toDouble * fields(7).toDouble) // ID weight
          //fields(5).toInt)   // days difference from now to last update time
        )
    }

    if (testMode == 1) {
      println("********** hjw test info **********")
      println("*** There are " + allId.count() + " nodes.")
    }

    // define a default ID type
    //val defaultId = ("NULL", "NULL", "NULL", "NULL", 0.0, -1)

    // ===== Graph edges
    // ===== type 1: all ID from the same table
    val IdParisFile1 = "./src/test/data/associatedIdPairs.csv"
    val IdPairs1: RDD[Edge[Int]] = sc.textFile(IdParisFile1).map {
      line =>
        val fields = line.split(",")
        Edge(fields(1).toLong, // source node ID
          fields(2).toLong, // destination node ID
          firstTypeEdgeWeight // relationship type => from the same table
        )
    }

    // ===== type 2: all ID have the same value
    val IdParisFile2 = "./src/test/data/associatedKeyByValue.csv"
    val IdPairs2: RDD[Edge[Int]] = sc.textFile(IdParisFile2).map {
      line =>
        val fields = line.split(",")
        Edge(fields(1).toLong, // source node ID
          fields(2).toLong, // destination node ID
          secondTypeEdgeWeight // relationship type => from the same table
        )
    }

    if (testMode == 1) {
      println("********** hjw test info **********")
      println("*** There are " + IdPairs1.count() + " connections of type 1.")
      println("*** There are " + IdPairs2.count() + " connections of type 2.")
    }

    val IdPairs = IdPairs1.union(IdPairs2)
    val graph = Graph(allId, IdPairs)

    // ====== output the whole graph
    if (debugMode == 1) {
      println("********** hjw debug info **********")
      val details = graph.triplets.map(
        triplet => triplet.srcAttr._2 + " from table " + triplet.srcAttr._1 + " with values " + triplet.srcAttr._4 +
          " is connected with " +
          triplet.dstAttr._2 + " from table " + triplet.dstAttr._1 + " with values " + triplet.dstAttr._4 +
          " with type " + triplet.attr
      )
      details.collect().map(println(_))
    }

    // create the non-directed graph by adding the reverse of the original graph
    val nonDirectedGraph = Graph(graph.vertices, graph.edges.union(graph.reverse.edges))

    if (testMode == 1) {
      println("********** hjw test info **********")
      println("*** There are " + nonDirectedGraph.edges.count() + " connections in final graph.")
    }

    // ==============================================
    // =========== Update the time information
    // ==============================================

    var IdUpdateTime: RDD[(VertexId, Int)] = allIdLine.map {
      line =>
        val fields = line.split("\t")
        (fields(0).toLong, // vertex ID
          fields(5).toInt // days difference from now to last update time
        )
    }.map{
      vertex =>
        if (vertex._2 < 0)  (vertex._1, Int.MaxValue)
        else vertex
    }

    if (testMode == 1) {
      println("********** hjw test info **********")
      println("*** There are " + IdUpdateTime.count() + " vertices counted.")
      println(IdUpdateTime.collect.mkString("\n"))
    }

    val updateTimeGraph = Graph(IdUpdateTime, IdPairs2)

    IdUpdateTime = updateTimeGraph.aggregateMessages[Int](
      triplet => {
        // Send Message
        if (triplet.srcAttr > 0)
          triplet.sendToDst(triplet.srcAttr)
        if (triplet.dstAttr > 0)
          triplet.sendToSrc(triplet.dstAttr)
        //else triplet.sendToDst(Int.MaxValue)
      },
      (a, b) => math.min(a, b) // Merge Message
    )

    val bb = updateTimeGraph.joinVertices(IdUpdateTime) {
      case (id, oldDate, newDate) => math.min(oldDate, newDate.toInt )
    }

    if (testMode == 1) {
      println("********** hjw test info **********")
      println("*** There are " + IdUpdateTime.count() + " vertices counted.")
      println(IdUpdateTime.collect.mkString("\n"))
      //IdUpdateTime.repartition(1).saveAsTextFile(outputDir + "/timeGraph/")
      //bb.vertices.repartition(1).saveAsTextFile(outputDir + "/timeGraph2/")
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

    // ===========================================
    // ===== test shortest path algorithm
    // ===== count the jump times between tables when join
    // ===========================================

    // Define a initial graph which has the same structure with the original graph
    // vertices has one attribute at beginning
    // for source Vertex ID => 0.0
    // for the others       => Inf
    val initialGraph = nonDirectedGraph.mapVertices(
      (id, _) =>
        if (id == sourceId) 0.0
        else Double.PositiveInfinity
    )

    // find the shortest path
    // we can define the weight for different edge types
    // we store the min sum of edge-weights from the source vertex to destination vertex
    // if there is path link them
    // Otherwise, we store the sum as Double.positiveInfinity
    val shortestPathGraph = initialGraph.pregel(Double.PositiveInfinity)(
      (id, dst, newDst) => math.min(dst, newDst),
      triplet => {  // Send Message
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        } else {
          Iterator.empty }
      },
      (a, b) => math.min(a, b) // Merge Message
    )

    if (testMode > 1){
      val connectedVertices = shortestPathGraph.vertices.filter {
        case (id, pathLength) => pathLength < Double.PositiveInfinity
      }

      println("********** hjw test info **********")
      println("*** There are " + connectedVertices.count() + " vertices connected to vertex ID = " + sourceId)
      println(connectedVertices.collect.mkString("\n"))
    }

    // add the min sum of edge-weights as a new attribute into vertex.attr
    // filter all vertices whose new attribute < inf
    // it means these vertices are connected to the
    val connectedVerticesAllInfo = nonDirectedGraph.outerJoinVertices(shortestPathGraph.vertices) {
        case (vid, attr, Some(pathLength)) => ((attr._1, attr._2, attr._3, attr._4, attr._5 * math.pow(alpha, pathLength)), pathLength)
        case (vid, attr, None) => (attr, Double.PositiveInfinity)
      }.vertices.filter {
      case (_, attr) => attr._2 < Double.PositiveInfinity
    }

    val aa = connectedVerticesAllInfo.map{
      case (id, attr) => ((attr._1._3, attr._1._4), attr._1._5)
    }.reduceByKey(_ + _)

    val aaa = connectedVerticesAllInfo.map{
      case (id, attr) => (attr._1._4, attr._1._5)
    }.reduceByKey(_ + _)

    if (testMode == 1){
      println("********** hjw debug info **********")
      println("*** There are " + connectedVerticesAllInfo .count() + " vertices connected to vertex ID = " + sourceId)
      println(connectedVerticesAllInfo.collect.mkString("\n"))

      println("********** hjw debug info **********")
      println(aa.collect.mkString("\n"))
      println(aaa.collect.mkString("\n"))
    }

    // =====================================
    // ===== filter by ID type
    // =====================================
    val connectedVerticesType = connectedVerticesAllInfo.filter {
        case (id, attr) => attr._1._3 == myIdType
    }

    if (testMode == 1){
      println("********** hjw test info **********")
      println("*** There are " + connectedVerticesType .count() + " vertices connected to vertex ID = "
        + sourceId + " with type " + myIdType )
      println(connectedVerticesType.collect.mkString("\n"))
    }

    val a = connectedVerticesType.map {
      case (id, attr) => (attr._1._4, attr._1._5)
    }.reduceByKey(_+_)

    if (testMode == 1) {
      println("********** hjw test info **********")
      println(a.collect.mkString("\n"))
    }
  }

  /*
   * This function is used to compute the time delay coefficient by a logarithm model
   * @param daysDiff: difference in days from now to last update time
   * @param T_half:   days when this coef reduces to 0.5
   * @param T_total:  days when this coef reduces to 0
   * NOTE: in this model we should have
   *        0.5 * T_total < T_half < T_total
   */
  def timeDecayLog(daysDiff: Int, T_half: Int = 200, T_total: Int = 360): Double = {
    if (daysDiff < 0) 0.5
    else if (daysDiff >= T_total) 0.0
    else {
      val gamma : Double = (2 * T_half - T_total).toDouble / ((T_total - T_half) * (T_total - T_half)).toDouble
      //println("we calculate the gamma = " + gamma.toString)
      math.log((T_total - daysDiff) * gamma + 1) / math.log(T_total * gamma + 1)
      //println("the result is " + result.toString)
    }
  }

  //TODO: define exponential time decay model
  def timeDecayExp(daysDiff : Int, T_half : Int, T_total : Int ): Double = {
    0.0
  }

  def deleteRecursively(file: File): Unit = {
    if (file.isDirectory)
      file.listFiles.foreach(deleteRecursively)
    if (file.exists && !file.delete)
      throw new Exception(s"Unable to delete ${file.getAbsolutePath}")
  }


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



