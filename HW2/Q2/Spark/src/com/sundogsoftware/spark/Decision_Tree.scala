package com.sundogsoftware.spark
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j._
object Decision_Tree {
def main(args: Array[String]): Unit = { 
Logger .getLogger("org").setLevel(Level.ERROR)
val conf = new SparkConf()
conf.setMaster("local")
conf.setAppName("Decision_Tree")
val sc = new SparkContext(conf)

val training = MLUtils.loadLibSVMFile(sc,"../mnist")
val test = MLUtils.loadLibSVMFile(sc,"../mnist.t")
 
//////  part a

val numClasses = 10
val categoricalFeaturesInfo = Map[Int, Int]()
val impurity = "entropy"
val maxBins = 32
for (l<- 3  to 8){
val t= System.nanoTime
println("\n\nMaxDepth : "+l)
val maxDepth = l
val model = DecisionTree.trainClassifier(training, numClasses, categoricalFeaturesInfo,impurity, maxDepth, maxBins)

val duration=(System.nanoTime-t)/1e9d
println ("RunTime : "+duration )
val labelAndPreds = test.map { point =>
val prediction = model.predict(point.features)
     (point.label, prediction)}
val testErr =labelAndPreds .filter(r =>r._1!=r._2).count().toDouble/test.count()
println("Test Error = "+testErr)

val metrics = new MulticlassMetrics(labelAndPreds)
val accuracy = metrics .accuracy
println(s"Accuracy = $accuracy ")
}

////// part b

val maxDepth_opt = 8
for (l<- List(4,8,16,32)){
val t_opt = System.nanoTime
println("\n\nMaxBins : "+l)
val maxBins_opt = l
val model = DecisionTree.trainClassifier(training, numClasses, categoricalFeaturesInfo,impurity, maxDepth_opt, maxBins_opt)
val duration_opt=(System.nanoTime-t_opt)/1e9d
println ("RunTime : "+duration_opt )
val labelAndPreds = test.map { point =>
val prediction = model.predict(point.features)
     (point.label, prediction)}
val testErr =labelAndPreds .filter(r =>r._1!=r._2).count().toDouble/test.count()
println("Test Error = "+testErr)

val metrics_opt = new MulticlassMetrics(labelAndPreds)
val accuracy = metrics_opt .accuracy
println(s"Accuracy = $accuracy ")

}

  }
}
