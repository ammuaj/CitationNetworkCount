
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql

import scala.collection.mutable.ListBuffer

object NodeEdgeCount {
  def main(args: Array[String]): Unit = {
    /// args(0) = publicationYear.txt
    // args(1) = citation.txt
    // args(2) = path to write year-total node,
    // args (3) = path to write year - total edges

    val sc = SparkSession.builder().master("spark://carson-city:30044").getOrCreate().sparkContext
    //val sc = SparkSession.builder().master("local").getOrCreate().sparkContext
    val year_rdd = sc.textFile(args(0)).filter(x => !x.startsWith("#")).map(line => (line.split("\t")(1).split("-")(0), 1))
    val nodesPerYear_rdd = year_rdd.reduceByKey(_+_).sortByKey()
    nodesPerYear_rdd.saveAsTextFile(args(2))

    //Edge Counting....
    val edge_rdd = sc.textFile(args(1)).filter(x => !x.startsWith("#")).map(line => (line.split("\t")(0),1)).reduceByKey(_+_)
    val node_year_rdd = sc.textFile(args(0)).filter(x => !x.startsWith("#")).map(line => (line.split("\t")(0), line.split("\t")(1).split("-")(0)))
    val edgeYear_rdd = edge_rdd.join(node_year_rdd).values.map(_.swap).reduceByKey(_+_).sortByKey()
    edgeYear_rdd.saveAsTextFile(args(3))
    //val cumEdge_rdd = edgeYear_rdd.
    //edge_rdd.saveAsTextFile(args(4))




  }

}
