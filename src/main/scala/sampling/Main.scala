package sampling

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.random.RandomRDDs
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

import org.apache.spark.util.SizeEstimator

object Main {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("CS422-Project2").setMaster("local[*]")
    val sc = SparkContext.getOrCreate(conf)
    val session = SparkSession.builder().getOrCreate();

    //val attrToChoose = List("l_orderkey", "l_partkey", "l_suppkey", "l_linenumber", "l_quantity", "l_extendedprice", "l_discount", "l_tax", "l_returnflag", "l_linestatus", "l_shipdate", "l_commitdate", "l_receiptdate", "l_shipinstruct", "l_shipmode", "l_comment")
    val attrToChoose = List("l_shipdate", "l_returnflag", "l_linestatus", "l_orderkey", "l_suppkey", "l_discount", "l_quantity", "l_year", "l_partkey", "l_commitdate", "l_receiptdate", "l_shipmode", "l_shipinstruct")
    var attrQueries = List(List("l_shipdate", "l_returnflag", "l_linestatus") //Q1
      ,List("l_orderkey", "l_shipdate") // Q3
      ,List("l_orderkey", "l_suppkey") // Q5
      ,List("l_shipdate", "l_discount", "l_quantity") // Q6
      ,List("l_orderkey", "l_suppkey", "l_shipdate", "l_year") // Q7
      ,List("l_orderkey", "l_suppkey", "l_partkey") // Q9
      ,List("l_orderkey", "l_returnflag") // Q10
      ,List("l_orderkey", "l_shipdate", "l_commitdate", "l_receiptdate", "l_shipmode") // Q12
      ,List("l_partkey", "l_quantity") // Q17
      ,List("l_orderkey", "l_quantity") // Q18
      ,List("l_partkey", "l_quantity", "l_shipmode", "l_shipinstruct") // Q19
      ,List("l_partkey", "l_suppkey", "l_shipdate")) // Q20

    println(attrToChoose.combinations(5).toList.map(x => (x, attrQueries.map(attrList => attrList.toSet.subsetOf(x.toSet)).count(_==true))).filter(x => x._2 > 4).sortBy(_._2).reverse)
    println(attrToChoose.combinations(4).toList.map(x => (x, attrQueries.map(attrList => attrList.toSet.subsetOf(x.toSet)).count(_==true))).filter(x => x._2 > 3).sortBy(_._2).reverse)
    println(attrToChoose.combinations(3).toList.map(x => (x, attrQueries.map(attrList => attrList.toSet.subsetOf(x.toSet)).count(_==true))).filter(x => x._2 > 1).sortBy(_._2).reverse)
    println(attrToChoose.combinations(2).toList.map(x => (x, attrQueries.map(attrList => attrList.toSet.subsetOf(x.toSet)).count(_==true))).filter(x => x._2 > 1).sortBy(_._2).reverse)
    // (List(l_shipdate, l_orderkey, l_suppkey, l_quantity, l_partkey),6) is the best
    // however queries 1, 6, 7, 10, 12, 19
    // aren't satisfied
    attrQueries = List(List("l_shipdate", "l_returnflag", "l_linestatus") //Q1
      ,List("l_shipdate", "l_discount", "l_quantity") // Q6
      ,List("l_orderkey", "l_suppkey", "l_shipdate", "l_year") // Q7
      ,List("l_orderkey", "l_suppkey", "l_partkey") // Q9
      ,List("l_orderkey", "l_returnflag") // Q10
      ,List("l_orderkey", "l_shipdate", "l_commitdate", "l_receiptdate", "l_shipmode") // Q12
      ,List("l_partkey", "l_quantity", "l_shipmode", "l_shipinstruct")) // Q19
    println("TEST2")
    println(attrToChoose.combinations(4).toList.map(x => (x, attrQueries.map(attrList => attrList.toSet.subsetOf(x.toSet)).count(_==true))).filter(x => x._2 > 1).sortBy(_._2).reverse)
    // List((List(l_returnflag, l_orderkey, l_suppkey, l_partkey),2), (List(l_shipdate, l_returnflag, l_linestatus, l_orderkey),2))
    // However, Q6, Q7, Q12
    println(attrToChoose.combinations(3).toList.map(x => (x, attrQueries.map(attrList => attrList.toSet.subsetOf(x.toSet)).count(_==true))).filter(x => x._2 > 1).sortBy(_._2).reverse)
    println(attrToChoose.combinations(2).toList.map(x => (x, attrQueries.map(attrList => attrList.toSet.subsetOf(x.toSet)).count(_==true))).filter(x => x._2 > 1).sortBy(_._2).reverse)



    var desc = new Description
    desc.lineitem = session.read.parquet("/home/dap/EPFL2018-2019/Semestre2/Database/Project2/CS422-Project2/src/main/resources/tpch_parquet_sf1/lineitem.parquet/*.parquet")
    //desc.lineitem.show()
    println(" SIZE " + SizeEstimator.estimate(desc.lineitem))
    desc.customer = session.read.parquet("/home/dap/EPFL2018-2019/Semestre2/Database/Project2/CS422-Project2/src/main/resources/tpch_parquet_sf1/customer.parquet/*.parquet")
    desc.orders = session.read.parquet("/home/dap/EPFL2018-2019/Semestre2/Database/Project2/CS422-Project2/src/main/resources/tpch_parquet_sf1/order.parquet/*.parquet")
    desc.supplier = session.read.parquet("/home/dap/EPFL2018-2019/Semestre2/Database/Project2/CS422-Project2/src/main/resources/tpch_parquet_sf1/supplier.parquet/*.parquet")
    desc.nation = session.read.parquet("/home/dap/EPFL2018-2019/Semestre2/Database/Project2/CS422-Project2/src/main/resources/tpch_parquet_sf1/nation.parquet/*.parquet")
    desc.region = session.read.parquet("/home/dap/EPFL2018-2019/Semestre2/Database/Project2/CS422-Project2/src/main/resources/tpch_parquet_sf1/region.parquet/*.parquet")
    //desc.part = session.read.parquet("//home/dap/EPFL2018-2019/Semestre2/Database/Project2/CS422-Project2/src/main/resources/tpch_parquet_sf1/part.parquet/*.parquet")
    desc.partsupp = session.read.parquet("/home/dap/EPFL2018-2019/Semestre2/Database/Project2/CS422-Project2/src/main/resources/tpch_parquet_sf1/partsupp.parquet/*.parquet")
    //desc.nation.show()
    desc.e = 0.1
    desc.ci = 0.95

    //Executor.execute_Q1(desc, session, List("90"))
    //Executor.execute_Q3(desc, session, List("BUILDING", "1995-03-15"))
    //Executor.execute_Q5(desc, session, List("ASIA", "1994-01-01"))
    //Executor.execute_Q6(desc, session, List("1994-01-01", 0.06, 24))
    //Executor.execute_Q7(desc, session, List("FRANCE", "GERMANY"))
    //Executor.execute_Q9(desc, session, List("green"))
    //Executor.execute_Q10(desc, session, List("1993-10-01"))
    //Executor.execute_Q11(desc, session, List("GERMANY", "0.0001000000"))
    //Executor.execute_Q12(desc, session, List("MAIL", "SHIP", "1994-01-01"))
    //Executor.execute_Q17(desc, session, List("Brand#23", "MED BOX"))
    //Executor.execute_Q18(desc, session, List(300))
    //Executor.execute_Q19(desc, session, List("Brand#12", "Brand#23", "Brand#34", 1, 10, 20))
    //Executor.execute_Q20(desc, session, List("forest", "1994-01-01", "CANADA"))

    /*val tmp = Sampler.sample(desc.lineitem, 1000000, desc.e, desc.ci)
    desc.samples = tmp._1
    desc.sampleDescription = tmp._2

    // check storage usage for samples

    // Execute first query
    Executor.execute_Q1(desc, session, List("3"))*/
  }     
}
