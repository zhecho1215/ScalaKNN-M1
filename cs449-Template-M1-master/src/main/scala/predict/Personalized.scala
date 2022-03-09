package predict

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.rogach.scallop._
import shared.predictions._
import ujson._


class PersonalizedConf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val train = opt[String](required = true)
  val test = opt[String](required = true)
  val separator = opt[String](default = Some("\t"))
  val num_measurements = opt[Int](default = Some(0))
  val json = opt[String]()
  verify()
}

object Personalized extends App {
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

  // Initialize the solvers for questions in P
  val solvers = new PSolvers(train, test)

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
          "1.Train" -> ujson.Str(conf.train()),
          "2.Test" -> ujson.Str(conf.test()),
          "3.Measurements" -> ujson.Num(conf.num_measurements())
        ),
        "P.1" -> ujson.Obj(
          "1.PredUser1Item1" -> ujson.Num(solvers.getPredUserItem(1, 1, similarity = solvers.userUniformSimilarity)), // Prediction of item 1 for user 1 (similarity 1 between users)
          "2.OnesMAE" -> ujson.Num(0.0) // MAE when using similarities of 1 between all users
        ),
        "P.2" -> ujson.Obj(
          "1.AdjustedCosineUser1User2" -> ujson.Num(solvers.userCosineSimilarity(1, 2)), // Similarity between user 1 and user 2 (adjusted Cosine)
          "2.PredUser1Item1" -> ujson.Num(solvers.getPredUserItem(1, 1, solvers.userCosineSimilarity)), // Prediction item 1 for user 1 (adjusted cosine)
          "3.AdjustedCosineMAE" -> ujson.Num(0.0) // MAE when using adjusted cosine similarity
        ),
        "P.3" -> ujson.Obj(
          "1.JaccardUser1User2" -> ujson.Num(solvers.userJaccardSimilarity(1, 2)), // Similarity between user 1 and user 2 (jaccard similarity)
          "2.PredUser1Item1" -> ujson.Num(solvers.getPredUserItem(1, 1, solvers.userJaccardSimilarity)), // Prediction item 1 for user 1 (jaccard)
          "3.JaccardPersonalizedMAE" -> ujson.Num(0.0) // MAE when using jaccard similarity
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
