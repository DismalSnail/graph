package com.yeph.bigdata.dga.cpm

import java.io.{File, PrintWriter}

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object CPMDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("CPM")
      .getOrCreate()

    val sc = spark.sparkContext
    val edgeRDD: RDD[Edge[Char]] = sc.textFile("D:/graph.txt").map(str => {
      val line: Array[String] = str.split(",")
      Edge(line(0).toLong, line(1).toLong, 1)
    })


    val graph: Graph[Char, Char] = Graph.fromEdges(edgeRDD, 1)
    val enumerate = new CliquePercolationMethod(spark)
    val communities: RDD[Set[VertexId]] = enumerate.run(graph, 4)


    val s: Array[Set[VertexId]] = communities.collect()
    spark.stop()

    println("********************************************************************")
    println(s.length)
    println("********************************************************************")

    val writer = new PrintWriter(new File("D:\\ideaProjects\\spark\\src\\main\\resources\\scala.txt"))

    for (set: Set[VertexId] <- s.sortBy(_.sum)) {
      writer.write(set.toList.sortBy(s => s).mkString(" ") + "\n")
    }

    writer.close()

    spark.close()
  }

}
