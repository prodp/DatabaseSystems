package cubeoperator

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import java.io._

object Main {
  def main(args: Array[String]) {
    val reducers = 20

    val inputFile= "../lineorder_medium.tbl"
    val input = new File(getClass.getResource(inputFile).getFile).getPath

    val sparkConf = new SparkConf().setAppName("CS422-Project2").setMaster("local[16]")
    val ctx = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(ctx)

    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", "|")
      .load(input)

    val rdd = df.rdd

    val schema = df.schema.toList.map(x => x.name)

    val dataset = new Dataset(rdd, schema)

    val cb = new CubeOperator(reducers)

    var groupingList = List("lo_suppkey","lo_shipmode","lo_orderdate")

    val t1 = System.nanoTime

    val res = cb.cube(dataset, groupingList, "lo_supplycost", "SUM")
    val col = res.collect()

    val t2 = System.nanoTime

    //col.foreach(println)
    //println(col.size)

    val restime1 = (t2-t1)/(Math.pow(10,9))

    val t3 = System.nanoTime

    val res2 = cb.cube_naive(dataset, groupingList, "lo_supplycost", "SUM")
    val col2 = res2.collect()

    val t4 = System.nanoTime

    val restime2 = (t4-t3)/(Math.pow(10,9))

    //val col2 = res2.collect()
    //col2.foreach(println)
    //println(col2.size)

    println("cube : " + restime1)
    println("naive : " + restime2)

    /*
       The above call corresponds to the query:
       SELECT lo_suppkey, lo_shipmode, lo_orderdate, SUM (lo_supplycost)
       FROM LINEORDER
       CUBE BY lo_suppkey, lo_shipmode, lo_orderdate
     */


    //Perform the same query using SparkSQL
      /*  val q1 = df.cube("lo_suppkey","lo_shipmode","lo_orderdate")
          .agg(avg("lo_supplycost") as "sum supplycost")
        q1.show
*/

  }
}