package com.yeph.bigdata.dga.cpm

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.{Edge, EdgeDirection, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

class CliquePercolationMethod(spark:SparkSession) {

  def run(graph: Graph[Char, Char], k: Int): RDD[Set[VertexId]] = {
    val clique: RDD[Set[VertexId]] = TSMaximalCliqueEnumerate(graph)
    community(clique, k)
  }

  def community(clique: RDD[Set[VertexId]], k: Int): RDD[Set[VertexId]] = {
    val vertex: RDD[(VertexId, Set[VertexId])] = clique.filter(set => set.size >= k).zipWithUniqueId().map(s => (s._2, s._1))

    val vertexBroadcast: Broadcast[collection.Map[VertexId, Set[VertexId]]] = spark.sparkContext.broadcast(vertex.collectAsMap())
//    val edge: RDD[Edge[Char]] = vertex.cartesian(vertex).filter {
//      case (src, dst) => {
//        (src._2 & dst._2).size >= k - 1 && dst._1 != src._1
//      }
//    }.map(e => Edge(e._1._1, e._2._1, 1))



    val edge: RDD[Edge[Char]] = vertex.mapPartitions({ iter =>
      val m: collection.Map[VertexId, Set[VertexId]] = vertexBroadcast.value
      for {
        vRDD <- iter
        v <- m
        if (vRDD._2 & v._2).size >= k - 1 && vRDD._1 != v._1
      } yield if (vRDD._1 > v._1) Edge(v._1, vRDD._1, '1') else Edge(vRDD._1, v._1, '1')
    }).distinct()

    val graph: Graph[Set[VertexId], Char] = Graph(vertex, edge)

    val conGraph: Graph[VertexId, Char] = graph.connectedComponents()
    val cliqueRDD: RDD[Set[VertexId]] = conGraph
      .vertices
      .join(vertex)
      .map(v => (v._2._1, v._2._2))
      .reduceByKey(_ ++ _)
      .map(clique => clique._2)

    cliqueRDD.cache().count()
    graph.edges.unpersist(blocking = false)
    graph.unpersistVertices(blocking = false)
    cliqueRDD
  }


  def TSMaximalCliqueEnumerate(graph: Graph[Char, Char]): RDD[Set[VertexId]] = {
    //收集所有的邻居节点
    val adjRDD: VertexRDD[Array[VertexId]] = graph.collectNeighborIds(EdgeDirection.Either)
    val graphWithNei: Graph[Array[VertexId], Char] = graph.outerJoinVertices(adjRDD)(
      (_, _, newAttr) => newAttr.getOrElse(Array[VertexId]())
    )

    //收集所有邻居节点及其邻居节点
    val neighbor: VertexRDD[Map[VertexId, Set[VertexId]]] = graphWithNei
      .aggregateMessages[Map[VertexId, Set[VertexId]]](
        e => {
          e.sendToSrc(Map(e.dstId -> e.dstAttr.toSet))
          e.sendToDst(Map(e.srcId -> e.srcAttr.toSet))
        },
        _ ++ _
      )

    //加上自己的邻居节点
    val infoGraph: Graph[Map[VertexId, Set[VertexId]], Char] = graphWithNei
      .outerJoinVertices(neighbor)(
        (vid, oldAttr, newAttr) => newAttr.getOrElse(Map()) ++ Map(vid -> oldAttr.toSet)
      )

    val vertices: VertexRDD[Map[VertexId, Set[VertexId]]] = infoGraph.vertices.cache()
    vertices.count()

    neighbor.unpersist(blocking = false)
    graphWithNei.unpersistVertices(blocking = false)
    graphWithNei.edges.unpersist(blocking = false)
    adjRDD.unpersist(blocking = false)

    val maximalCliqueSetRDD: RDD[mutable.Set[Set[VertexId]]] = vertices.map(vertex => {
      val X: mutable.Set[VertexId] = mutable.Set[VertexId]()
      val R: mutable.Set[VertexId] = mutable.Set[VertexId]()
      val n_v: Set[VertexId] = vertex._2(vertex._1)
      val P: mutable.Set[VertexId] = mutable.Set[VertexId]((n_v ++ Set(vertex._1)).toSeq: _*)

      for (v: (VertexId, Set[VertexId]) <- vertex._2) {
        P ++ Set(v._1)
        P ++ (n_v & v._2)
      }

      val bk = new BornKerbosch2(vertex._2)
      bk.maximalCliquesWithPivot(R, P, X)
      bk.getCliqueSet
    }
    )

    val cacheClique: RDD[Set[VertexId]] = maximalCliqueSetRDD
      .flatMap(set => set)
      .distinct()
      .cache()
    cacheClique.count()
    vertices.unpersist(blocking = false)
    cacheClique
  }

}
