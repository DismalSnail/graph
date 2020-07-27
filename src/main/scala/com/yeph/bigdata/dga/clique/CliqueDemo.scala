package com.yeph.bigdata.dga.clique

import org.apache.spark.graphx.lib.TriangleCount
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object CliqueDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("CPM")
      .getOrCreate()

    val edgeRDD: RDD[Edge[Char]] = spark.sparkContext.parallelize(Seq(
      (1L, 2L),
      (1L, 3L),
      (1L, 4L),
      (2L, 3L),
      (2L, 4L),
      (2L, 5L),
      (3L, 4L),
      (4L, 5L)
    )).map { case (src, dst) => Edge(src, dst, '1') }

    val graph: Graph[Int, Char] = Graph.fromEdges(edgeRDD,1)


    val cli = new KClique
    val result = cli.run(spark.sparkContext,graph,4).collect()

    println(result.mkString("\n"))

  }
}