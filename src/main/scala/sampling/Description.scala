package sampling

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

class Description {
  var lineitem: DataFrame = _
  var customer: DataFrame = _
  var orders  : DataFrame = _
  var supplier: DataFrame = _
  var nation  : DataFrame = _
  var region  : DataFrame = _
  var part    : DataFrame = _
  var partsupp: DataFrame = _

  // The samples generated by the sampler
  var samples : List[RDD[_]] = _
  // Metadata that match the generated samples to queries
  var sampleDescription : Any = _

  // target error [0, 1]
  var e : Double = _
  // target confidence level [0, 1]
  var ci: Double = _

}