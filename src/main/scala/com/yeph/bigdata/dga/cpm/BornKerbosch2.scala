package com.yeph.bigdata.dga.cpm

import scala.collection.mutable

class BornKerbosch2(adj: Map[Long, Set[Long]]) {

  private val cliqueSet: mutable.Set[Set[Long]] = mutable.Set[Set[Long]]()

  private val adjSize: Map[Long, Int] = adj.mapValues(_.size)

  def getCliqueSet: mutable.Set[Set[Long]] = cliqueSet

  private def pivotSelect(candidates: Set[Long]): Long = {
    adjSize.filter(v => candidates.contains(v._1)).maxBy(_._2)._1
  }


  def maximalCliquesWithPivot(R: mutable.Set[Long], P: mutable.Set[Long], X: mutable.Set[Long]): Unit = {

    if (P.isEmpty && X.isEmpty) {
      cliqueSet.add(R.toSet)
    } else {
      val pivot = pivotSelect((P ++ X).toSet)

      val P_part = P -- adj(pivot)

      for (v <- P_part) {
        maximalCliquesWithPivot(R ++ Set(v), P & adj(v), X & adj(v))
        P.remove(v)
        X.add(v)
      }
    }

  }

}
