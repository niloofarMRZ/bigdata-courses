package com.sundogsoftware.spark
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.log4j._
object Clustering {
  def main(args: Array[String]) {
   val time = System.nanoTime
   Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "Clustering")
    val data1 = sc.textFile("../C1.txt")
    val data2 = sc.textFile("../C2.txt")
    val data3 = sc.textFile("../C3.txt")
    
    val st = data1.map(s =>(s.toString().split("\t")(0)))
    val td1=st.map(s =>(s.split("    ")(1).toDouble,s.split("    ")(2).toDouble))
    val c1=td1.map(s => Vectors.dense(List(s._1, s._2).toArray))
    val td2=data2.map(s =>(s.split("\t")(0).toDouble, s.split("\t")(1).toDouble))
    val c2=td2.map(s => Vectors.dense(List(s._1, s._2).toArray))
    val td3=data3.map(s =>(s.split("\t")(0).toDouble, s.split("\t")(1).toDouble))
    val c3 = td3.map(s=>Vectors.dense(List(s._1, s._2).toArray))
 
////////     Part a
    
     var k1=0
     println("\nCost for C1.txt\n\n") 
     for (k1 <- 2 to 25) {
     val clusters1 = KMeans.train(c1, k1, 20 ,KMeans.K_MEANS_PARALLEL)
     val SE = clusters1.computeCost(c1)
     println(s" Sum of Squared Errors for k = $k1 : $SE")}
   
     var k2=0
     println("\n\nCost for C2.txt\n")
     for (k2 <- 2 to 25) {
     val clusters2 = KMeans.train(c2, k2, 20 ,KMeans.K_MEANS_PARALLEL)
     val SE = clusters2.computeCost(c2)
     println(s" Sum of Squared Errors for k = $k2 : $SE")}
   
    var k3=0
    println("\n\nCost for C3.txt\n")
    for (k3 <- 2 to 25) {
    val clusters3 = KMeans.train(c3, k3, 20 ,KMeans.K_MEANS_PARALLEL)
    val SE = clusters3.computeCost(c3)
    println(s" Sum of Squared Errors for k = $k3 : $SE")
    }

/////   Part   b,c 

val K1_opt=25
val K2_opt=24
val K3_opt=25
val clusters1_opt = KMeans.train(c3, K1_opt, 20 ,KMeans.K_MEANS_PARALLEL)
val clusters2_opt = KMeans.train(c3, K2_opt, 20 ,KMeans.K_MEANS_PARALLEL)
val clusters3_opt = KMeans.train(c3, K3_opt, 20 ,KMeans.K_MEANS_PARALLEL)
 println("\n\nCentroids for C1.txt in optimum cluster number\n")
clusters1_opt.clusterCenters.foreach(println)
 println("\n\nCentroids for C2.txt in optimum cluster number\n")
clusters2_opt.clusterCenters.foreach(println)
 println("\n\nCentroids for C3.txt in optimum cluster number\n")
clusters3_opt.clusterCenters.foreach(println)
}  
}
