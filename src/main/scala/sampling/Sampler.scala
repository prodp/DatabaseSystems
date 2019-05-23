package sampling

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.rand
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.asc
import org.apache.spark.sql.functions.column
import org.apache.spark.sql.functions.stddev
import org.apache.spark.sql.functions.sum
import scala.collection.mutable.ListBuffer


object Sampler {

  def sample(lineitem: DataFrame, storageBudgetBytes: Long, e: Double, ci: Double): (List[RDD[_]], _) = {
    //val QCS = List("l_shipdate", "l_orderkey", "l_suppkey")
    val QCS: List[List[String]] = List(List("l_shipdate", "l_orderkey", "l_suppkey", "l_quantity", "l_partkey"), List("l_returnflag", "l_orderkey", "l_suppkey", "l_partkey"), List("l_shipdate", "l_returnflag", "l_linestatus", "l_orderkey"))

    val n = lineitem.count()

    var samples = ListBuffer[RDD[Row]]()
    var info = ListBuffer[List[String]]()

    for (qc <- QCS) {
      val test = lineitem.groupBy(qc.head, qc.tail:_*)

      val variance : Double = lineitem.agg(stddev("l_extendedprice")).first.get(0).toString().toDouble
      val error_max =  lineitem.agg(sum("l_extendedprice")).first.get(0).toString().toDouble * e

      var left_anchor = 0d
      val step = n / 10d
      var storageLeft = storageBudgetBytes

      // Compute first sample
      //var sampleRDD : RDD[Row]
      // Draw a key X independantly from U(0, 1)
      val lineitemX = lineitem.withColumn("X", rand())

      // Search for the best k, starting in the middle
      var keep_going = true
      while (keep_going) {
        val k = left_anchor + step
        val p = k / n.toDouble
        val gamma1 = -math.log(e) / n
        val q1 = math.min(1, p + gamma1 + math.sqrt(gamma1 * gamma1 + 2 * gamma1 * p))
        val gamma2 = -(2 * math.log(e) / (3 * n))
        val q2 = math.max(0, p + gamma2 - math.sqrt(gamma2 * gamma2 + 3 * gamma2 * p))

        val list = lineitemX.filter(col("X") < q2)
        val waiting_list = lineitemX.filter(col("X") >= q2 && col("X") < q1)

        val wait_to_add = (k - list.count()).toInt
        val sample = if (wait_to_add <= 0) list.orderBy(asc("X")).limit(k.toInt)
        else list.union(waiting_list.orderBy("X").limit(wait_to_add))

        val groups_size_n = lineitem.groupBy(qc map column: _*).count()
        // TODO : std dev for each group, not for the whole dataset
        //val groups_size_n_var =  lineitem.groupBy(qc map column: _*)
        //  .agg(stddev("l_extendedprice")).as("dev")
        //groups_size_n_var.columns.foreach(println)
        /*val groups_size_n = groups_size_n_1.withColumnRenamed("count", "count_n").join(
          groups_size_n_var
        )*/
        val groups_size_k = sample.groupBy(qc map column: _*).count()
        val groups_join = groups_size_n.withColumnRenamed("count", "count_n")join(
          groups_size_k.withColumnRenamed("count", "count_k"), qc)



        //val variance : Double = lineitem.agg(stddev("l_extendedprice")).first.get(0).toString().toDouble


        val partial_sums = groups_join.withColumn("parts",
          col("count_k") * col("count_k") *
            variance * variance * k / col("count_n"))
        val v = partial_sums.agg(sum("parts")).first.getDouble(0)

        val est_error = get_z_value(ci) * math.sqrt(v / k)

        println(est_error)

        if (est_error < error_max && groups_size_n.count() == groups_size_k.count()) {
          val size = sample.count()
          if (size < storageLeft) {
            samples += sample.rdd.persist()
            info += qc
            storageLeft -= size
          }
          else {
            keep_going = false
          }
        }
        else {
          left_anchor += step
        }
      }
    }


    /*groups_size_n.columns.foreach(println)
    groups_size_n.take(3).foreach(println)

    lineitem.columns.foreach(println)
    lineitem.take(5).foreach(println)*/
    (samples.toList, info)
  }

  def get_z_value(conf: Double) = {
    conf match {
      case 0.8 => 1.28
      case 0.9 => 1.645
      case 0.95 => 1.96
      case 0.98 => 2.33
      case 0.99 => 2.58
    }
  }
}
