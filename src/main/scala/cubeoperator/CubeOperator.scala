package cubeoperator

import org.apache.spark.rdd.RDD
import scala.collection.mutable.ListBuffer

class CubeOperator(reducers: Int) {

  /*
 * This method gets as input one dataset, the grouping attributes of the cube (CUBE BY clause)
 * the attribute on which the aggregation is performed
 * and the aggregate function (it has to be one of "COUNT", "SUM", "MIN", "MAX", "AVG")
 * and returns an RDD with the result in the form of <key = string, value = double> pairs.
 * The key is used to uniquely identify a group that corresponds to a certain combination of attribute values.
 * You are free to do that following your own naming convention.
 * The value is the aggregation result.
 * You are not allowed to change the definition of this function or the names of the aggregate functions.
 * */
  def cube(dataset: Dataset, groupingAttributes: List[String], aggAttribute: String, agg: String): RDD[(String, Double)] = {

    val rdd = dataset.getRDD()
    val schema = dataset.getSchema()

    val index = groupingAttributes.map(x => schema.indexOf(x))
    val indexAgg = schema.indexOf(aggAttribute)

    //TODO Task 1
    val aggFunction = (x1: Double, x2: Double) => {
      {
        agg match {
          case "COUNT" => x1 + x2
          case "SUM" => x1 + x2
          case "MIN" => Math.min(x1, x2)
          case "MAX" => Math.max(x1, x2)
          case "AVG" => x1 + x2
          case unknown => throw new IllegalArgumentException("Unknown agg function: " + unknown.toString)
        }
      } : Double
    }

    val data = rdd.map(x => (x.getValuesMap(groupingAttributes).toList,
                            (x.get(indexAgg).asInstanceOf[Integer].toDouble, 1d)))

    val combined = data.reduceByKey((x, y) => (x._1+y._1, x._2+y._2), reducers)

    val reduced = combined.flatMap(x => {
      (0 to groupingAttributes.length).flatMap(y => {
        x._1.combinations(y).map(z => {
          var list = ListBuffer[String]()
          for (i <- 0 to groupingAttributes.length-1) {
            list += "*"
          }

          for (tup <- z) {
            val idx = groupingAttributes.indexOf(tup._1)
            if (idx != -1) {
              list(idx) = tup._2
            }
          }
          (list, x._2)
        })
      })
    })

    val recombined = reduced.map(x => (x._1.mkString(","), x._2))
      .reduceByKey((x1, x2) => (aggFunction(x1._1, x2._1),  x1._2 + x2._2), reducers)

    if (agg == "COUNT") recombined.map(x => (x._1, x._2._2))
    else if (agg == "AVG") recombined.map(x => (x._1, x._2._1/x._2._2))
    else recombined.map(x => (x._1, x._2._1))
  }

  def cube_naive(dataset: Dataset, groupingAttributes: List[String], aggAttribute: String, agg: String): RDD[(String, Double)] = {

    //TODO naive algorithm for cube computation
    val rdd = dataset.getRDD()
    val schema = dataset.getSchema()

    val index = groupingAttributes.map(x => schema.indexOf(x))
    val indexAgg = schema.indexOf(aggAttribute)

    //TODO Task 1
    val aggFunction = (x1: Double, x2: Double) => {
      {
        agg match {
          case "COUNT" => x1 + x2
          case "SUM" => x1 + x2
          case "MIN" => Math.min(x1, x2)
          case "MAX" => Math.max(x1, x2)
          case "AVG" => x1 + x2
          case unknown => throw new IllegalArgumentException("Unknown agg function: " + unknown.toString)
        }
      } : Double
    }

    val data = rdd.map(x => (x.getValuesMap(groupingAttributes).toList,
      (x.get(indexAgg).asInstanceOf[Integer].toDouble, 1d)))

    //val combined = data.reduceByKey((x, y) => (x._1+y._1, x._2+y._2), reducers)

    val reduced = data.flatMap(x => {
      (0 to groupingAttributes.length).flatMap(y => {
        x._1.combinations(y).map(z => {
          var list = ListBuffer[String]()
          for (i <- 0 to groupingAttributes.length-1) {
            list += "*"
          }

          for (tup <- z) {
            val idx = groupingAttributes.indexOf(tup._1)
            if (idx != -1) {
              list(idx) = tup._2
            }
          }
          (list, x._2)
        })
      })
    })

    val recombined = reduced.map(x => (x._1.mkString(","), x._2))
      .reduceByKey((x1, x2) => (aggFunction(x1._1, x2._1),  x1._2 + x2._2), reducers)

    if (agg == "COUNT") recombined.map(x => (x._1, x._2._2))
    else if (agg == "AVG") recombined.map(x => (x._1, x._2._1/x._2._2))
    else recombined.map(x => (x._1, x._2._1))
  }

}
