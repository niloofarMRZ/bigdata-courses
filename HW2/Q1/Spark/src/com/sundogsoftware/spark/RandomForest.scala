package com.sundogsoftware.spark
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import scala.collection.mutable.MutableList
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.configuration.Strategy
import org.apache.log4j._
object RanndomForest {
  def main(args: Array[String]) {
    val DEBUG       = 0

val PRTS        = 1	// location of protocol in rawData
val SRVS        = 2	// location of service in rawData
val FLGS        = 3	// location of flags in rawData
val offset	= 1				// Array's start at 0
val PIDX	= PRTS - offset
val SIDX        = SRVS - offset
val FIDX	= FLGS - offset
Logger.getLogger("org").setLevel(Level.ERROR)
      val sc = new SparkContext("local[*]", "RanndomForest")//, scnf)
        
      val train_data = sc.textFile("../kddcup.data.gz")
      val test_data = sc.textFile("../corrected.gz")
 var protocols = train_data.map(_.split(",")(PRTS)).distinct.collect.zipWithIndex
 var services  = train_data.map(_.split(",")(SRVS)).distinct.collect.zipWithIndex
 var flags     = train_data.map(_.split(",")(FLGS)).distinct.collect.zipWithIndex 
      val encodings = Array(protocols.toList.toMap,	// encodings(PIDX)
                       services.toList.toMap,	// encodings(SIDX)
                          flags.toList.toMap)	// encodings(FIDX)
      
      val labelsAndData = train_data.map { line =>
        val buffer = line.split(',').toBuffer
         //buffer(PRTS)   = encodings(PIDX)(buffer(PRTS)).toString
         //buffer(SRVS)   = encodings(SIDX)(buffer(SRVS)).toString
         //buffer(FLGS)   = encodings(FIDX)(buffer(FLGS)).toString
        buffer.remove(1, 3)
        var label = buffer.remove(buffer.length-1)
        label match {
          case "normal." => label = "normal"
          case "back." => label = "dos"
          case "land." => label = "dos"
          case "neptune." => label = "dos"
          case "pod." => label = "dos"  
          case "smurf." => label = "dos"
          case "teardrop." => label = "dos"
          case "buffer_overflow." => label = "u2r"
          case "loadmodule." => label = "u2r"
          case "perl." => label = "u2r"
          case "rootkit." => label = "u2r"
          case "ftp_write." => label = "r2l"
          case "guess_passwd." => label = "r2l"
          case "imap." => label = "r2l"
          case "multihop." => label = "r2l"
          case "phf." => label = "r2l"
          case "spy." => label = "r2l"
          case "warezclient." => label = "r2l"
          case "warezmaster." => label = "r2l"
          case "ipsweep." => label = "probe"
          case "nmap." => label = "probe"
          case "portsweep." => label = "probe"   
          case "satan." => label = "probe" }
        var labels = 1
        label match {
          case "normal" => labels = 0
          case "dos" => labels = 1
          case "u2r" => labels = 2
          case "r2l" => labels = 3
          case "probe" => labels = 4 }
        val vector = Vectors.dense(buffer.map(_.toDouble).toArray)
        LabeledPoint(labels, vector)
      }
      val test = test_data.map { line =>
        val buffer = line.split(',').toBuffer
       // buffer(PRTS)   = encodings(PIDX)(buffer(PRTS)).toString
       // buffer(SRVS)   = encodings(SIDX)(buffer(SRVS)).toString
       // buffer(FLGS)   = encodings(FIDX)(buffer(FLGS)).toString
        buffer.remove(1, 3)
        var label = buffer.remove(buffer.length-1)
        label match {
          case "normal." => label = "normal"
          case "back." => label = "dos"
          case "land." => label = "dos"
          case "neptune." => label = "dos"
          case "pod." => label = "dos"  
          case "smurf." => label = "dos"
          case "teardrop." => label = "dos"
          case "buffer_overflow." => label = "u2r"
          case "loadmodule." => label = "u2r"
          case "perl." => label = "u2r"
          case "rootkit." => label = "u2r"
          case "ftp_write." => label = "r2l"
          case "guess_passwd." => label = "r2l"
          case "imap." => label = "r2l"
          case "multihop." => label = "r2l"
          case "phf." => label = "r2l"
          case "spy." => label = "r2l"
          case "warezclient." => label = "r2l"
          case "warezmaster." => label = "r2l"
          case "ipsweep." => label = "probe"
          case "nmap." => label = "probe"
          case "portsweep." => label = "probe" 
          case "satan." => label = "probe" 
          case _ => label = "attack" 
        }
        var labels = 1
        label match {
          case "normal" => labels = 0
          case "dos" => labels = 1
          case "u2r" => labels = 2
          case "r2l" => labels = 3
          case "probe" => labels = 4
          case "attack" => labels = 5
        }
        val vector = Vectors.dense(buffer.map(_.toDouble).toArray)
        LabeledPoint(labels, vector)
      }
      
///////      Part a
      
      val numClasses = 5
      val categoricalFeaturesInfo = Map[Int, Int]()
      val featureSubsetStrategy = "auto" 
      val impurity = "entropy"
      val maxDepth = 10
      val maxBins = 32 
     
       var i=10
      for (l <- 0 to 4) {
        val t = System.nanoTime
        println("i: " + i)
        val numTrees = i  
        val model = RandomForest.trainClassifier(labelsAndData, numClasses, categoricalFeaturesInfo,
            numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)
        val duration = (System.nanoTime - t) / 1e9d
        println("RunTime: " + duration )
       i=i+10
        val labelsAndPredictions = test.map { point =>
        val prediction = model.predict(point.features)
        (point.label, prediction)
        }
        val testErr = labelsAndPredictions.filter(r => r._1 != r._2).count().toDouble / test.count()
       println("Test Error = " + testErr)
       val metrics = new MulticlassMetrics(labelsAndPredictions)
        val accuracy = metrics.accuracy
        println(s"Accuracy = $accuracy")
      }
      
      
/////// part b,c      
      
      val numTrees_opt =40
      val maxDepth_secPart = 20
      val t_opt = System.nanoTime
      val model_opt = RandomForest.trainClassifier(labelsAndData, numClasses, categoricalFeaturesInfo,
            numTrees_opt, featureSubsetStrategy, impurity, maxDepth_secPart, maxBins)
        val duration_opt = (System.nanoTime - t_opt) / 1e9d
        println("RunTime in optimum case: " + duration_opt )
        val labelsAndPredictions = test.map { point =>
        val prediction = model_opt.predict(point.features)
        (point.label, prediction)
        }
        val testErr = labelsAndPredictions.filter(r => r._1 != r._2).count().toDouble / test.count()
       println("Test Error " + testErr)
       val metrics = new MulticlassMetrics(labelsAndPredictions)
        val accuracy = metrics.accuracy
        println(s"Accuracy = $accuracy")
        
// Precision by label
val labels = metrics.labels
labels.foreach { l =>
  println(s"Precision($l) = " + metrics.precision(l))
}
// Recall by label
labels.foreach { l =>
  println(s"Recall($l) = " + metrics.recall(l))
}
// False positive rate by label
labels.foreach { l =>
  println(s"FPR($l) = " + metrics.falsePositiveRate(l))
}
// F-measure by label
labels.foreach { l =>
  println(s"F1-Score($l) = " + metrics.fMeasure(l))
}
println(s" precision: ${metrics.weightedPrecision}")
println(s" recall or True Positive Rate: ${metrics.weightedRecall}")
println(s" FMeasure: ${metrics.weightedFMeasure}")
println(s" false positive rate:${metrics.weightedFalsePositiveRate}")
      


      
  }
}