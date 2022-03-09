package predict

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.rogach.scallop._
import shared.predictions._
import ujson._


class kNNConf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val train = opt[String](required = true)
  val test = opt[String](required = true)
  val separator = opt[String](default = Some("\t"))
  val num_measurements = opt[Int](default = Some(0))
  val json = opt[String]()
  verify()
}

object kNN extends App {
  // Remove these lines if encountering/debugging Spark
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  val spark = SparkSession.builder()
                          .master("local[1]")
                          .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  println("")
  println("******************************************************")

  var conf = new PersonalizedConf(args)
  println("Loading training data from: " + conf.train())
  val train = load(spark, conf.train(), conf.separator()).collect()
  println("Loading test data from: " + conf.test())
  val test = load(spark, conf.test(), conf.separator()).collect()

  // Initialize the solver for questions related to the KNN algorithm with k = 10
  val solver10 = new KNNSolver(train, test, 10)

  val measurements = (1 to conf.num_measurements()).map(x => timingInMs(() => {
    Thread.sleep(1000) // Do everything here from train and test
    42 // Output answer as last value
  }))
  val timings = measurements.map(t => t._2) // Retrieve the timing measurements

  // Save answers as JSON
  def printToFile(content: String,
                  location: String = "./answers.json") =
    Some(new java.io.PrintWriter(location)).foreach {
      f =>
        try {
          f.write(content)
        } finally {
          f.close
        }
    }
  // TODO: Add get MAE.
  conf.json.toOption match {
    case None => ;
    case Some(jsonFile) => {
      val answers = ujson.Obj(
        "Meta" -> ujson.Obj(
          "1.Train" -> conf.train(),
          "2.Test" -> conf.test(),
          "3.Measurements" -> conf.num_measurements()
        ),
        "N.1" -> ujson.Obj(
          // Similarity between user 1 and user 1 (k=10)
          "1.k10u1v1" -> ujson.Num(solver10.userCosineSimilarity(1, 1)),
          // Similarity between user 1 and user 864 (k=10)
          "2.k10u1v864" -> ujson.Num(solver10.userCosineSimilarity(1, 864)),
          // Similarity between user 1 and user 886 (k=10)
          "3.k10u1v886" -> ujson.Num(solver10.userCosineSimilarity(1, 886)),
          // Prediction of item 1 for user 1 (k=10)
          "4.PredUser1Item1" -> ujson.Num(solver10.getPredUserItem(item = 1, user = 1, solver10.userCosineSimilarity))
        ),
        "N.2" -> ujson.Obj(
          "1.kNN-Mae" -> List(10, 30, 50, 100, 200, 300, 400, 800, 943).map(k =>
            List(
              k,
              0.0 // Compute MAE
            )
          ).toList
        ),
        "N.3" -> ujson.Obj(
          "1.kNN" -> ujson.Obj(
            "average (ms)" -> ujson.Num(mean(timings)),
            "stddev (ms)" -> ujson.Num(std(timings))
          )
        )
      )
      val json = write(answers, 4)

      println(json)
      println("Saving answers in: " + jsonFile)
      printToFile(json, jsonFile)
    }
  }

  println("")
  spark.close()
}
