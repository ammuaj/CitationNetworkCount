
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ListBuffer

object graphDiameterCountAll {
  def main(args: Array[String]): Unit = {
    /// args(0) = publicationYear.txt
    // args(1) = citation.txt
    // args(2) = up to year ,
    // args (3) = path to write adjacency list
    // args (4)  = path to write graph path with d = 1
    // args(5)  = path to write graph path with d = 2
    //Provided count of g(20) of the year from 1992-1997
    val g20AllYear: Map[Int, Long] = Map(1992 -> 727, 1993 -> 869493, 1994 -> 6685253, 1995 -> 22234693, 1996 -> 50406009, 1997 -> 76823404)
    var all_g_Count = new ListBuffer[Long]()
    var allGDPercentage = new ListBuffer[Double]()
    var countPercent = 0.0
    var pathcount = 0;

    val year = args(2).toInt
    val sc = SparkSession.builder().master("spark://carson-city:30044").getOrCreate().sparkContext
    //val sc = SparkSession.builder().master("local").getOrCreate().sparkContext

    //Creating node and corresponding RDD pair
    val node_year_rdd = sc.textFile(args(0), 8).filter(x => !x.startsWith("#")).map(line => (line.split("\t")(0), line.split("\t")(1).split("-")(0))).filter { case (_, y) => y.toInt <= year }.persist(StorageLevel.MEMORY_ONLY)
    //val node_year_rdd = nodeyear_rdd.filter { case (_, y) => y.toInt <= year }.persist()
   // nodeyear_rdd.unpersist()
    //val nodesPerYear_rdd = year_rdd.reduceByKey(_+_).sortByKey()
    //nodesPerYear_rdd.saveAsTextFile(args(1))

    //Creating Two way edge list ( v1->v2 and v2->v1)
    val validEdges_rdd = sc.textFile(args(1), 8).filter(x => !x.startsWith("#")).map(line => (line.split("\t")(0), line.split("\t")(1))).join(node_year_rdd).map(x => (x._1, x._2._1)).persist(StorageLevel.MEMORY_ONLY)
    //val validNodes = node_year_rdd.filter{case(_,y)=>y.toInt<year}

   // val validEdges_rdd = edge_rdd.join(node_year_rdd).map(x => (x._1, x._2._1)).persist()
   // edge_rdd.unpersist()
    node_year_rdd.unpersist()
    // keep count of all peths of d = 1
    //all_g_Count += validEdges_rdd.count()
    var countP = validEdges_rdd.count()
    all_g_Count+=countP


    //Checking for 90% pathCount
    countPercent = (countP*100.0/g20AllYear(year))
    allGDPercentage.append(countPercent)
    //allGDPercentage+= (all_g_Count(all_g_Count.size-1))/g20AllYear(year))*100
/*    if(allGDPercentage(pathcount)>=90){
      sc.parallelize(allGDPercentage).saveAsTextFile(args(3))
      sc.parallelize(all_g_Count).saveAsTextFile(args(4))
      sc.stop()
      sys.exit()
    }*/


    // Starting processing for g= 2
    val two_way_edge_rdd = validEdges_rdd.flatMap {
      case (v1, v2) =>
        var edges = new ListBuffer[String]()
        edges += v1 + '-' + v2
        edges += v2 + '-' + v1
        edges.toList
    }.map(x => (x.split("-")(0), x.split("-")(1))).persist(StorageLevel.MEMORY_ONLY)

    validEdges_rdd.unpersist()


    // Filtering out the edge list up to provided year


    //Writing All adjacency lists
    //adjacentNodes_rdd.saveAsTextFile(args(3))

    //d=2, path with length 2 calculating and finding the shortest....



    var allShortestPath = two_way_edge_rdd.map(x => (x._1 + "~" + x._2, x._1 + "-" + x._2))
    //val adjacentNodes_rdd = two_way_edge_rdd.map(e => (e._1, List(e._2))).reduceByKey(_ ::: _).persist(StorageLevel.MEMORY_ONLY)
    // All shortest Path in a single RDD
    //var allShortestPath = allPath_1
    //allPath_1.unpersist()

    var prev_pathList = two_way_edge_rdd.map(e => (e._1, List(e._2))).reduceByKey(_ ::: _).flatMap {
      case (node, adjList) =>
        var edges = new ListBuffer[String]()
        if (adjList.size > 1) {
          for (i <- 0 to (adjList.size - 2)) {
            for (j <- i + 1 to (adjList.size - 1)) {
              val start_node = adjList(i)
              val end_node = adjList(j)
              var start_end = ""
              if (start_node.toInt > end_node.toInt) {
                start_end = end_node + "~" + start_node
                edges += (start_end + ":" + end_node + "-" + node + "-" + start_node)
              }
              else {
                start_end = start_node + "~" + end_node
                edges += (start_end + ":" + start_node + "-" + node + "-" + end_node)
              }
            }
          }
        }
        edges.toList
    }.map(x => (x.split(":")(0), x.split(":")(1))).reduceByKey((p1, _) => p1).subtractByKey(allShortestPath).persist(StorageLevel.MEMORY_ONLY)

    allShortestPath = allShortestPath.union(prev_pathList)

   // adjacentNodes_rdd.unpersist()
   // all_g_Count += (allPath_2.count() + all_g_Count(0))
    countP = prev_pathList.count() + all_g_Count(all_g_Count.size-1)
    all_g_Count+=countP


    //Checking for 90% pathCount
    countPercent = (countP*100.0/g20AllYear(year))
    allGDPercentage.append(countPercent)

   // pathcount+=1
   // allGDPercentage+=(all_g_Count(pathcount)/g20AllYear(year))*100
/*    if(allGDPercentage(pathcount)>=90){
      sc.parallelize(allGDPercentage).saveAsTextFile(args(3))
      sc.parallelize(all_g_Count).saveAsTextFile(args(4))
      sc.stop()
      sys.exit()
    }*/

    //var prev_pathList = allPath_2

    //allPath_2.saveAsTextFile(args(3))
     // allPath_2.unpersist()

    /*
    println(all_g_Count)
    println(g20AllYear)
    sc.parallelize(all_g_Count).saveAsTextFile(args(4))*/


    /// Processing to calculate path 3 - 4 -5....
    val two_way_path1 = two_way_edge_rdd.map(x => (x._1, x._1 + "-" + x._2)).persist(StorageLevel.MEMORY_ONLY)
    two_way_edge_rdd.unpersist()
    /// Loop should be started from here...............
    var k = 2
   while(countPercent<90 && k<10){
    //for(k<-3 to 5){

     prev_pathList = prev_pathList.flatMap {
      case (sd, spd) =>
        val sD = sd.split("~")
       // val d = sd.split("~")(1)

        var edges = new ListBuffer[String]()
        edges += sD(1) + ":" + spd

        val ds = spd.split("-").reverse
        var dps = ds(0)
        for (i <- 1 to ds.size - 1) {
          dps = dps + "-" + ds(i)
        }
        edges += sD(0) + ":" + dps
        edges.toList
    }.map(x => (x.split(":")(0), x.split(":")(1))).join(two_way_path1).flatMap {
      case (midNode, (spdLong, spdShort)) =>
        val s = spdLong.split("-")(0)
        val d = spdShort.split("-")(1)
        var edges = new ListBuffer[String]()
        if (spdLong.contains(d) != true) {
          if (s.toInt < d.toInt) {
            edges += s + "~" + d + ":" + spdLong + "-" + d
          }
          else {
            val ds = spdLong.split("-").reverse
            var dps = d
            for (i <- 0 to ds.size - 1) {
              dps = dps + "-" + ds(i)
            }
            edges += d + "~" + s + ":" + dps
          }
        }
        edges.toList
    }.map(x => (x.split(":")(0), x.split(":")(1))).reduceByKey((p1, _) => p1).subtractByKey(allShortestPath).persist(StorageLevel.MEMORY_ONLY)
     //val newShortestPath = prev_pathList
    // newPath.unpersist()

     //newPath.unpersist()
  //  all_g_Count += (all_g_Count(all_g_Count.size - 1) + newShortestPath.count())

   //   pathcount+=1
     // allGDPercentage+=(all_g_Count(all_g_Count.size - 1)/g20AllYear(year))*100


      countP = prev_pathList.count() + all_g_Count(all_g_Count.size-1)
      all_g_Count+=countP
      //Checking for 90% pathCount
      countPercent = (countP*100.0/g20AllYear(year))
      allGDPercentage.append(countPercent)

/*      if(allGDPercentage(pathcount)>=90){
        sc.parallelize(allGDPercentage).saveAsTextFile(args(3))
        sc.parallelize(all_g_Count).saveAsTextFile(args(4))
        two_way_path1.unpersist()
        sc.stop()
        sys.exit()
      }*/

    // Initializing for new start of loop
     allShortestPath = allShortestPath.union(prev_pathList)
     //prev_pathList = newShortestPath
     //newShortestPath.unpersist()
      //writing each new pathlist
      //newShortestPath.saveAsTextFile(args(2)+"_"+k.toString)
      k+=1
  }
    //Writing to file for debugging .............
   // prev_pathList.saveAsTextFile(args(3))
    //sc.parallelize(all_g_Count).saveAsTextFile(args(4))
    prev_pathList.unpersist()
    two_way_path1.unpersist()

    //Saving the graph path with diameter 1 and 2
     // allPath_1.saveAsTextFile(args(4))
      //allPath_2.saveAsTextFile(args(5))
    //prev_pathList.saveAsTextFile(args(5))
    sc.parallelize(allGDPercentage).saveAsTextFile(args(4))
    sc.parallelize(all_g_Count).saveAsTextFile(args(3))


    //sc.stop()





    // }
   // val node_year_rdd = sc.textFile(args(0)).filter(x => !x.startsWith("#")).map(line => (line.split("\t")(0), line.split("\t")(1).split("-")(0)))
   // val edgeYear_rdd = edge_rdd.join(node_year_rdd).values.map(_.swap).reduceByKey(_+_).sortByKey()
    //edgeYear_rdd.saveAsTextFile(args(3))
    //val cumEdge_rdd = edgeYear_rdd.
    //edge_rdd.saveAsTextFile(args(4))




  }

}
