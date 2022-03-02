package predict

import org.rogach.scallop._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

import scala.math
import shared.predictions._

import scala.math.abs


class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val train = opt[String](required = true)
  val test = opt[String](required = true)
  val separator = opt[String](default=Some("\t"))
  val num_measurements = opt[Int](default=Some(0))
  val json = opt[String]()
  verify()
}

object Baseline extends App {
  // Remove these lines if encountering/debugging Spark
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  val spark = SparkSession.builder()
    .master("local[1]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR") 

  println("")
  println("******************************************************")

  var conf = new Conf(args) 
  // For these questions, data is collected in a scala Array 
  // to not depend on Spark
  println("Loading training data from: " + conf.train()) 
  val train = load(spark, conf.train(), conf.separator()).collect()
  print(train(1))
  println("Loading test data from: " + conf.test()) 
  val test = load(spark, conf.test(), conf.separator()).collect()

  // Apply preprocessing operations on the train and test
  val normalizedTrain = preprocess(train)
  val normalizedTest = preprocess(test)

  // Average rating statistics
  val meanGlobal = mean(train.map(x => x.rating))
  val user1Avg = mean(train.filter(x => x.user == 1).map(x => x.rating))
  val item1Avg = mean(train.filter(x => x.item == 1).map(x => x.rating))
  val item1AvgDev = std(train.filter(x => x.item == 1).map(x => x.rating))
  val item1AvgDevScale = stdScale(train.filter(x => x.item == 1).map(x => x.rating))
  val predUser1Item1 = user1Avg + item1AvgDev * scaleUserRating(user1Avg + item1AvgDev, user1Avg)

  val measurements = (1 to conf.num_measurements()).map(x => timingInMs(() => {
    Thread.sleep(1000) // Do everything here from train and test
    println(train.map(x => abs(x.rating - meanGlobal)).sum)
    42        // Output answer as last value
  }))
  val timings = measurements.map(t => t._2) // Retrieve the timing measurements

  // Save answers as JSON
  def printToFile(content: String, 
                  location: String = "./answers.json") =
    Some(new java.io.PrintWriter(location)).foreach{
      f => try{
        f.write(content)
      } finally{ f.close }
  }

  conf.json.toOption match {
    case None => ;
    case Some(jsonFile) => {
      var answers = ujson.Obj(
        "Meta" -> ujson.Obj(
          "1.Train" -> ujson.Num(train(1).rating),
          "Size Train" -> ujson.Num(train.length),
          "2.Test" -> ujson.Str(conf.test()),
          "3.Measurements" -> ujson.Num(conf.num_measurements())
        ),
        "B.1" -> ujson.Obj(
          "1.GlobalAvg" -> meanGlobal, // Datatype of answer: Double
          "2.User1Avg" -> user1Avg,  // Datatype of answer: Double
          "3.Item1Avg" -> item1Avg,   // Datatype of answer: Double
          "4.Item1AvgDev" -> item1AvgDev, // Datatype of answer: Double
          "6.scaleItem1AvgDev" -> item1AvgDevScale, // Datatype of answer: Double
          "5.PredUser1Item1" -> predUser1Item1 // Datatype of answer: Double
        ),
        "B.2" -> ujson.Obj(
          "1.GlobalAvgMAE" -> ujson.Num(0.0), // Datatype of answer: Double
          "2.UserAvgMAE" -> ujson.Num(0.0),  // Datatype of answer: Double
          "3.ItemAvgMAE" -> ujson.Num(0.0),   // Datatype of answer: Double
          "4.BaselineMAE" -> ujson.Num(0.0)   // Datatype of answer: Double
        ),
        "B.3" -> ujson.Obj(
          "1.GlobalAvg" -> ujson.Obj(
            "average (ms)" -> ujson.Num(mean(timings)), // Datatype of answer: Double
            "stddev (ms)" -> ujson.Num(std(timings)) // Datatype of answer: Double
          ),
          "2.UserAvg" -> ujson.Obj(
            "average (ms)" -> ujson.Num(mean(timings)), // Datatype of answer: Double
            "stddev (ms)" -> ujson.Num(std(timings)) // Datatype of answer: Double
          ),
          "3.ItemAvg" -> ujson.Obj(
            "average (ms)" -> ujson.Num(mean(timings)), // Datatype of answer: Double
            "stddev (ms)" -> ujson.Num(std(timings)) // Datatype of answer: Double
          ),
          "4.Baseline" -> ujson.Obj(
            "average (ms)" -> ujson.Num(mean(timings)), // Datatype of answer: Double
            "stddev (ms)" -> ujson.Num(std(timings)) // Datatype of answer: Double
          )
        )
      )

      val json = ujson.write(answers, 4)
      println(json)
      println("Saving answers in: " + jsonFile)
      printToFile(json.toString, jsonFile)
    }
  }

  println("")
  spark.close()
}
