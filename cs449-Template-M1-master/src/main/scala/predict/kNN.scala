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
  // Initialize the solver for questions related to the KNN algorithm with k = 300
  val solver300 = new KNNSolver(train, test, 300)

  val measurements = (1 to conf.num_measurements()).map(x => timingInMs(() => {
    // solver300.getMAE(similarity = solver300.userCosineSimilarity)
    42
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
          "1.k10u1v1" -> ujson.Num(solver10.userSimilarity(1, 1)),
          // Similarity between user 1 and user 864 (k=10)
          "2.k10u1v864" -> ujson.Num(solver10.userSimilarity(1, 864)),
          // Similarity between user 1 and user 886 (k=10)
          "3.k10u1v886" -> ujson.Num(solver10.userSimilarity(1, 886)),
          // Prediction of item 1 for user 1 (k=10)
          // TODO: change
          "4.PredUser1Item1" -> ujson.Num(solver10.personalizedPredictor(train, solver10.userCosineSimilarity)(1, 1))
        ),
        "N.2" -> ujson.Obj(
          // TODO: add the rest of the k values.
          "1.kNN-Mae" -> List(10).map(k => {
            val solver = new KNNSolver(train, test, k)
            List(
              k,
              solver.getMAE(similarity = solver.userCosineSimilarity)
            )
          }
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
