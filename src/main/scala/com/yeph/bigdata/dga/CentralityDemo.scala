package com.yeph.bigdata.dga

import org.apache.spark.graphx.{EdgeDirection, EdgeTriplet, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.reflect.ClassTag

object CentralityDemo {

  def dependency(sourceId: VertexId, graph: Graph[Double, Double]) = {
    val initialGraph = graph.mapVertices((id, _) => if (id == sourceId) 0.0 else Double.PositiveInfinity)
    val vertexNum = initialGraph.vertices.count()

    val sssp = initialGraph.pregel(Double.PositiveInfinity)(
      (id, dist, newDist) => math.min(dist, newDist),
      triplet => {
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr)
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        else
          Iterator.empty
      },
      math.min
    )

    //前置节点搜集
    val proGraph = sssp.mapVertices((id, dist) => (dist, mutable.LongMap[Double]()))

    val preRDD = proGraph.aggregateMessages[mutable.LongMap[Double]](
      e => {
        if (e.srcAttr._1 + e.attr == e.dstAttr._1)
          e.sendToDst(mutable.LongMap[Double](e.srcId -> 0.0))
      },
      _ ++ _
    )

    val sorRDD = proGraph.aggregateMessages[mutable.LongMap[Double]](
      e => {
        if (e.srcAttr._1 + e.attr == e.dstAttr._1)
          e.sendToDst(mutable.LongMap[Double](e.dstId -> 0.0))
      },
      _ ++ _
    )

    //sigma 计算初始化
    val graphForSigma = sssp.outerJoinVertices(preRDD)((id, dist, preMap) =>
      (if (id == sourceId) 1.0 else 0.0, 0.0, preMap.getOrElse(mutable.LongMap[Double]()))
    )

    //sigma vertex program
    def sigmaVertexProgram(id: VertexId,
                           oldAttr: (Double, Double, mutable.LongMap[Double]),
                           newMap: mutable.LongMap[Double]) = {
      if (newMap.nonEmpty) {
        for (key <- newMap.keys) {
          if (newMap(key) != oldAttr._3(key)) {
            oldAttr._3(key) = newMap(key)
          }
        }
      }
      (if (id != sourceId) 1.0 else oldAttr._3.values.sum, 0.0, oldAttr._3)
    }

    //sigma send message
    def sigmaSendMessage(triple: EdgeTriplet[(Double, Double, mutable.LongMap[Double]), Double]) = {
      if (triple.dstAttr._3.contains(triple.srcId)) {
        if (triple.srcAttr._1 != triple.dstAttr._3(triple.srcId)) {
          Iterator((triple.dstId, mutable.LongMap(triple.srcId -> triple.srcAttr._1)))
        } else {
          Iterator.empty
        }
      } else {
        Iterator.empty
      }
    }

    val sigmaGraph = graphForSigma.pregel(
      mutable.LongMap[Double](),
      maxIterations = vertexNum.toInt,
      activeDirection = EdgeDirection.Out)(
      sigmaVertexProgram, sigmaSendMessage, _ ++ _
    )

    val graphForDelta = sigmaGraph.outerJoinVertices(sorRDD)(
      (_, data, sorRDD) => (data._1, data._2, sorRDD.getOrElse(mutable.LongMap[Double]()), false)
    )

    //delta vertex program
    def deltaVertexProgram(id: VertexId,
                           oldAttr: (Double, Double, mutable.LongMap[Double], Boolean),
                           newMap: mutable.LongMap[Double]) = {
      if (!newMap.contains(-1L) && newMap.nonEmpty) {
        for (key <- newMap.keys) {
          if (newMap(key) != oldAttr._3(key)) {
            oldAttr._3(key) = newMap(key)
          }
        }
      }
      (oldAttr._1, if (id == sourceId) 0.0 else oldAttr._3.values.sum, oldAttr._3, if (newMap.contains(-1L)) true else false)
    }

    //delta send message
    def deltaSendMessage(triple: EdgeTriplet[(Double, Double, mutable.LongMap[Double], Boolean), Double]) = {
      val dependency: Double = triple.srcAttr._1 / triple.dstAttr._1 * (1 + triple.dstAttr._2)
      if (triple.srcAttr._3.contains(triple.dstId)) {
        if (dependency != triple.srcAttr._3(triple.dstId) || triple.srcAttr._4) {
          Iterator((triple.srcId, mutable.LongMap(triple.dstId -> dependency)))
        } else {
          Iterator.empty
        }
      } else {
        Iterator.empty
      }
    }

    val bcGraph = graphForDelta.pregel(
      mutable.LongMap(-1L -> 0.0),
      maxIterations = vertexNum.toInt,
      activeDirection = EdgeDirection.In
    )(
      deltaVertexProgram,
      deltaSendMessage,
      _ ++ _
    )

    bcGraph

  }

  def acculateEdge(spark: SparkSession, graph: Graph[Double, Double], sampleId: Array[VertexId]) = {
    var rst: RDD[((VertexId, VertexId), Double)] = null
    val vertexNum: Long = graph.vertices.count()
    for (sourceId <- sampleId) {
      val newDep = dependency(sourceId, graph).mapEdges(e => 0.0)
        .mapTriplets(e => if (e.srcAttr._3.contains(e.dstId)) e.srcAttr._1 / e.dstAttr._1 * (1 + e.dstAttr._2) else 0.0)
        .edges.map(e => ((e.srcId, e.dstId), e.attr)).persist()
      if (null == rst) {
        rst = newDep
      } else {
        rst = (rst union newDep).reduceByKey(_ + _).persist()
      }
    }

    val sampleNum = sampleId.length
    val approRst = rst.map { case ((src, dst), attr) => (src, dst, attr * vertexNum.toDouble / sampleNum.toDouble) }
  }

  def acculate(spark: SparkSession, grpah: Graph[Double, Double], sampleId: Array[VertexId]) = {
    var rst: RDD[(VertexId, Double)] = null
    for (sourceId <- sampleId) {
      val newDep = dependency(sourceId, grpah).vertices.map(vertex => (vertex._1, vertex._2._2)).persist()
      if (rst == null) {
        rst = newDep
      } else {
        rst = (rst union newDep).reduceByKey(_ + _).persist()
      }

    }

  }

  def betweenesssCentrality(spark: SparkSession,
                            graph: Graph[Double, Double],
                            k: Int = 0,
                            isEdge: Boolean = false): Unit = {
    var sampleId: Array[VertexId] = null
    if (k == 0) {
      sampleId = graph.vertices.map(e => e._1).collect()
    } else {
      sampleId = graph.vertices.map(e => e._1).takeSample(withReplacement = false, k)
    }

    if (isEdge) acculateEdge(spark, graph, sampleId)
    else acculate(spark, graph, sampleId)
  }

  def main(args: Array[String]): Unit = {
    ClassTag
  }

}
