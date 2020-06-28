package com.yeph.bigdata.dga.cpm

import org.apache.spark.graphx.{Edge, EdgeDirection, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

class CliquePercolationMethod(spark: SparkSession) {

  def run(graph: Graph[Char, Char], k: Int): RDD[Set[VertexId]] = {
    val clique: RDD[Set[VertexId]] = TSMaximalCliqueEnumerate(graph)
    community(clique, k)
  }

  def community(clique: RDD[Set[VertexId]], k: Int): RDD[Set[VertexId]] = {
    val vertex: RDD[(VertexId, Set[VertexId])] = clique.filter(set => set.size >= k).zipWithUniqueId().map(s => (s._2, s._1))

    val verDF: DataFrame = spark.createDataFrame(vertex.map(v => (v._1,v._2.toArray))).toDF("id", "key")
    spark.udf.register("udf_insert", udf_insert)
 verDF.createOrReplaceTempView("vertex")
    def udf_insert: UserDefinedFunction = udf((a: Seq[Long], b: Seq[Long]) => (a.toSet & b.toSet).size >= k - 1)

    val edge: RDD[Edge[Char]] = spark.sql("select t0.id as src,t1.id as dst  from" +
      "  vertex t0 " +
      " join vertex t1 on udf_insert(t0.key,t1.key)==true and t0.id < t1.id").
      rdd.
      map(e => Edge(e.getLong(0), e.getLong(1), '1'))



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
