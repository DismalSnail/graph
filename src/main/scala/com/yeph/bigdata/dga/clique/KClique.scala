package com.yeph.bigdata.dga.clique

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.{EdgeDirection, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class KClique {

  def run[VD: ClassTag, ED: ClassTag](sc: SparkContext, graph: Graph[VD, ED], k: Int) = {
    val neighborIds: VertexRDD[Array[VertexId]] = graph.collectNeighborIds(EdgeDirection.Either)
    val neighborList: RDD[(VertexId, Array[VertexId])] = neighborIds.map { case (id, nIds) =>
      (id, nIds.filter(_ > id))
    }

    val localAdjList: collection.Map[VertexId, Array[VertexId]] = neighborList.collectAsMap()
    val bLocalAdgList: Broadcast[collection.Map[VertexId, Array[VertexId]]] = sc.broadcast(localAdjList)

    var completion: RDD[(Set[VertexId], Set[VertexId])] = neighborList.map { case (id, _) => (Set(id), bLocalAdgList.value(id).toSet) }

    var extensions: RDD[(VertexId, Set[VertexId], Set[VertexId])] = null


    var count = k - 1
    while (count > 0) {
      extensions = completion.flatMap { case (clique, extensions) =>
        for {
          i <- extensions
        } yield (i, clique + i, extensions)
      }

      completion = extensions.map {
        case (id, clique, possExt) => (clique, bLocalAdgList.value(id).toSet & possExt)
      }
      count -= 1
    }

    completion.map(_._1)
  }


}
