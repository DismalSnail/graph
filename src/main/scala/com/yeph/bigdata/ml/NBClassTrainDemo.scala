package com.yeph.bigdata.ml

import org.apache.spark.sql
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf

import scala.reflect.internal.util.TableDef.Column

object NBClassTrainDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("CPM")
      .getOrCreate()
    import spark.implicits._
    spark.udf.register("udf_insert",udf_insert)
     def udf_insert: UserDefinedFunction = udf((a:Seq[Long], b:Seq[Long]) => (a.toSet & b.toSet).size >= 2)


    val df1: DataFrame = spark.createDataFrame(Seq((1, Set(2L,5L,8L).toArray), (3, Set(4L,6L).toArray))).toDF("id", "key")
    val df2: DataFrame = spark.createDataFrame(Seq((1, Set(5L,8L).toArray), (3, Set(2L,5L,6L,7L).toArray))).toDF("id", "key")

    df1.join(df2,udf_insert(df1.col("key"),df2.col("key"))).show()
    case class Person(name: String, age: Long)

  }
}
