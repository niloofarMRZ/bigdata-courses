package com.sundogsoftware.spark
import  data.Flight
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import  org.apache.spark.graphx.PartitionStrategy._
import scala.util.hashing.MurmurHash3
import org.apache.log4j._
import scala.reflect.ClassTag
import scala.collection.{mutable, Map}
object graphx {
Logger.getLogger("org").setLevel(Level.ERROR)
  var quiet = false
  def main(args: Array[String]) {
    val input = "../flight.csv"
    val conf = new SparkConf()
      .setAppName("graphx")
      .setMaster("local[*]")
      .set("spark.app.id", "GraphX") 
        val sc = new SparkContext(conf)
        try{
        val flights = for {
        line <- sc.textFile(input) 
        flight <-Flight.parse(line)
          } yield flight
//_______________________________________________________________________________________________
          
   val nEdges = flights.map(e=> Edge(MurmurHash3.stringHash(e.origin).toLong,MurmurHash3.stringHash(e.dest).toLong, e.distance.toInt))
   val airportCodes = flights.flatMap { f => Seq(f. origin, f.origin) }
   val airportVertices = 
        airportCodes.distinct().map(x => (MurmurHash3.stringHash(x).toLong, x))
        val airportVerticess = 
        airportCodes.distinct().map(x => (MurmurHash3.stringHash(x), x))
   val graphh = Graph(airportVertices,nEdges,"")
 //_______________________________________________________________________________________________

   // part a & b
 
val partitionedUserGraph=graphh.partitionBy(CanonicalRandomVertexCut)
val graph=partitionedUserGraph.groupEdges( (ED , ED1) => ED)
      val numroutes = graph.numEdges
      val numairports= graph.numVertices
      val EdgeHighDist=  graph.edges.filter { case ( Edge(org_id, dest_id,distance))=> distance > 1000}
      var en=EdgeHighDist.count()
graph.triplets.sortBy(_.attr, ascending=false).map(triplet =>
  "Distance  from " + triplet.srcAttr + " to " + triplet.dstAttr + " is " + triplet.attr.toString + ".").collect.take(en.toInt).foreach(println)
     println("num of airports = " + numairports)
     println("num of routes = " + numroutes )   
 // _______________________________________________________________________________________________
    
     // part e

val ranks = graphh.pageRank(0.001).vertices
val temp= ranks.join(airportVertices)
val temp2 = temp.map(t => t._2).sortBy(_._1, false).take(10).foreach(println)

//__________________________________________________________________________________________________

   // part f

val ANC = flights.filter(_.origin == "ANC")    
val delay = ANC.map(f => (f.carrierDelay,f.weatherDelay,f.nasDelay,f.lateAircraftDelay,f.securityDelay,(f.dest))).distinct
val delays= delay.map(x => (x._1+ x._2+x._3+x._4+x._5,x._6))
delays.sortBy(x=>x._1, false).take(5).foreach(println);
      
//____________________________________________________________________________________________________
 
    // part c&d

   val maxinDegree = graphh.inDegrees.collect.sortWith(_._2 > _._2)
   .map(x => (airportVertices.lookup(x._1), x._2)).foreach(println)
   
   val maxoutDegree = graphh.outDegrees.collect.sortWith(_._2 > _._2)
   .map(x => (airportVertices.lookup(x._1), x._2)).foreach(println)
   
   val maxDegree = graphh.degrees.collect.sortWith(_._2 > _._2)
   .map(x => (airportVertices.lookup(x._1), x._2)).foreach(println)
   
//____________________________________________________________________________________________________   
  
   // part g
 
def Distance[VD](g:Graph[String,Int], origin:VertexId) = {
  var g2 = g.mapVertices(
    (vid,vd) => (false, if (vid == origin) 0 else Double.MaxValue,
                 List[VertexId]()))

  for (i <- 1L to g.vertices.count-1) {
    val currentVertexId =
      g2.vertices.filter(!_._2._1)
        .fold((0L,(false,Double.MaxValue,List[VertexId]())))((a,b) =>
           if (a._2._2 < b._2._2) a else b)
        ._1

    val newDistances = g2.aggregateMessages[(Double,List[VertexId])](
        ctx => if (ctx.srcId == currentVertexId)
                 ctx.sendToDst((ctx.srcAttr._2 + ctx.attr,
                                ctx.srcAttr._3 :+ ctx.srcId)),
        (a,b) => if (a._1 < b._1) a else b)

    g2 = g2.outerJoinVertices(newDistances)((vid, vd, newSum) => {
      val newSumVal =
        newSum.getOrElse((Double.MaxValue,List[VertexId]()))
      (vd._1 || vid == currentVertexId,
       math.min(vd._2, newSumVal._1),
       if (vd._2 < newSumVal._1) vd._3 else newSumVal._2)})
  }

  g.outerJoinVertices(g2.vertices)((vid, vd, dist) =>
    (vd, dist.getOrElse((false,Double.MaxValue,List[VertexId]()))    
             .productIterator.toList.tail))
}
   val x= Distance(graphh,1128308899 ).vertices.map(_._2).filter({case(vId, _) => vId =="LAS"}) .foreach(println)
 //____________________________________________________________________________________________________
    } finally {
      sc.stop()
    }
  }
  def stringHash(str: String): Int = MurmurHash3.stringHash(str)
}

