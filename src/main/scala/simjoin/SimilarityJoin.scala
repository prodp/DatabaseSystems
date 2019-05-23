package simjoin

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import scala.collection.mutable.Map
import EditDistance.edit_distance

class SimilarityJoin(numAnchors: Int, distThreshold:Int) extends java.io.Serializable {
  val logger = LoggerFactory.getLogger("SimilarityJoin")
  var rdd: RDD[String] = null

  /*
   * this method gets as input a dataset and the index of an attribute
   * of the dataset, and returns the result of the similarity self join on
   * that attribute.
   * */
  def similarity_join(dataset: Dataset, attrIndex: Int) : RDD[(String, String)] = {
    rdd = dataset.getRDD().map(x => x(attrIndex).asInstanceOf[String])
    val size : Double = rdd.count()
    val p = numAnchors/size

    // 1. Extract a random sample of anchor points
    val A = rdd.sample(false, p)

    // check if the anchors are similar
    val similarsAnchors = A.cartesian(A).filter(x => x._1 < x._2 && edit_distance(x._1, x._2) <= distThreshold).flatMap(x => List((x._1, x._2), (x._2, x._1)))

    // used afterwards to check only one time the H inter Outer
    var outersToCheck = A.cartesian(A).filter{ case (a,b) => a < b }.map{
      case (input1, input2) => if(input1 < input2 && (input1 + input2).hashCode() % 2 == 0){
        (input1, input2)
      }else{
        (input2,input1)
      }}.groupBy(_._1).mapValues(buff => buff.map(_._2)).collectAsMap()

    // 2. Assign each element to the closest anchor point and construct the partitions.
    val inputMinusAnchors = rdd.subtract(A)
    val inputCrossAnchors = inputMinusAnchors.cartesian(A)
    val allDistances = inputCrossAnchors.map(x => x._1 -> (x._2, edit_distance(x._1, x._2)))

    // check the similarity between records and anchors
    val similarsRecToAnchors = allDistances.filter(x => x._2._2 <= distThreshold).map(x => (x._1, x._2._1)).flatMap(x => List((x._1, x._2), (x._2, x._1)))
    val minDistances = allDistances.reduceByKey((x, y) => if (x._2 > y._2) y else x)
    val inputToClosestAnchor = minDistances.map(x => (x._1, x._2._1, x._2._2))  // (input, closest_anchor, distance)


    val inputAnchorCrossAnchors = inputToClosestAnchor.cartesian(A) //  ((input, closest_anchor, distance), anchors)
    val outerDistances = inputAnchorCrossAnchors.map(x => (x._1._1, x._1._2, x._1._3, x._2, edit_distance(x._1._1, x._2)))
    // outerDistances : (input, closest_anchor, distance, other_anchor, other_distance)

    val outerDistancesFiltered = outerDistances.filter{x =>
      x._2 == x._4 || (x._5 <= x._3 + 2*distThreshold && (outersToCheck get x._2 match {
        case None => false
        case Some(value) => value.toList.contains(x._4)
      }))}
    /*val outerDistancesFiltered = outerDistances.filter(x => x._5 <= x._3 + 2*distThreshold)*/

    // take the combinations of home partition elements
    val homesVSOuters = outerDistancesFiltered.flatMap(x => List( (x._2, (x._1, true)), (x._4, (x._1, false || x._2 == x._4)) )).distinct()
    val homes = homesVSOuters.filter(x => x._2._2).map(x => (x._1, x._2._1))
    val homesGrouped = homes.groupByKey()
    val homesWithAnchor = homesGrouped.map(x => x._2.toBuffer)
    val partitionsHomes = homesWithAnchor.flatMap(x => x.combinations(2)).map(x => (x(0), x(1))).distinct()
    val similarHomes = partitionsHomes.filter(x => edit_distance(x._1, x._2) <= distThreshold).flatMap(x => List((x._1, x._2), (x._2, x._1)))

    // check the outer elements (not in the partition) with the home partition elements
    val outers = homesVSOuters.filter(x => !x._2._2).map(x => (x._1, x._2._1)).groupByKey
    val partitionsOuters = outers.join(homesGrouped).flatMap(x => x._2._1.flatMap(str1 => x._2._2.map(str2 => (str1, str2))) ).distinct()
    val similarOuters = partitionsOuters.filter(x => edit_distance(x._1, x._2) <= distThreshold).flatMap(x => List((x._1, x._2), (x._2, x._1)))

    similarsAnchors union similarsRecToAnchors union (similarHomes union similarOuters).distinct()

  }
}