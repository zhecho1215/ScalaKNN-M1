package predict

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.rogach.scallop._
import shared.predictions._


class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val train = opt[String](required = true)
  val test = opt[String](required = true)
  val separator = opt[String](default = Some("\t"))
  val num_measurements = opt[Int](default = Some(0))
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
  println("Loading test data from: " + conf.test())
  val test = load(spark, conf.test(), conf.separator()).collect()

  // Initialize the solvers for questions in B
  val solver = new BaselineSolver(train, test)

  // Get timing for each of the 4 methods
  var timings = scala.collection.mutable.Map.empty[String, Seq[Double]]
  for (predictor: String <- Array("Baseline", "Global", "User", "Item")) {
    val measurements = (1 to conf.num_measurements()).map(x => timingInMs(() => {
      predictor match {
        case "Baseline" => solver.getMAE(solver.baselinePredictor(train))
        case "Global" => solver.getMAE(solver.globalAvgPredictor(train))
        case "User" => solver.getMAE(solver.userAvgPredictor(train))
        case "Item" => solver.getMAE(solver.itemAvgPredictor(train))
      }
    }))
    timings(predictor) = measurements.map(t => t._2)
  }

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
          // Datatype of answer: Double
          "1.GlobalAvg" -> ujson.Num(solver.globalAvg(train)),
          // Datatype of answer: Double
          "2.User1Avg" -> ujson.Num(solver.userAvgPredictor(train)(1, 0)),
          // Datatype of answer: Double
          "3.Item1Avg" -> ujson.Num(solver.itemAvgPredictor(train)(0, 1)),
          // Datatype of answer: Double
          "4.Item1AvgDev" ->
            ujson.Num(solver.itemAvgDev(train)(1)),
          // Datatype of answer: Double
          "5.PredUser1Item1" -> ujson.Num(solver.baselinePredictor(train)(1, 1))
        ),

        "B.2" -> ujson.Obj(
          "1.GlobalAvgMAE" -> solver.getMAE(solver.globalAvgPredictor(train)), // Datatype of answer: Double
          "2.UserAvgMAE" -> solver.getMAE(solver.userAvgPredictor(train)), // Datatype of answer: Double
          "3.ItemAvgMAE" -> solver.getMAE(solver.itemAvgPredictor(train)), // Datatype of answer: Double
          "4.BaselineMAE" -> solver.getMAE(solver.baselinePredictor(train)) // Datatype of answer: Double
        ),
        "B.3" -> ujson.Obj(
          "1.GlobalAvg" -> ujson.Obj(
            "average (ms)" -> ujson.Num(mean(timings("Global"))), // Datatype of answer: Double
            "stddev (ms)" -> ujson.Num(std(timings("Global"))) // Datatype of answer: Double
          ),
          "2.UserAvg" -> ujson.Obj(
            "average (ms)" -> ujson.Num(mean(timings("User"))), // Datatype of answer: Double
            "stddev (ms)" -> ujson.Num(std(timings("User"))) // Datatype of answer: Double
          ),
          "3.ItemAvg" -> ujson.Obj(
            "average (ms)" -> ujson.Num(mean(timings("Item"))), // Datatype of answer: Double
            "stddev (ms)" -> ujson.Num(std(timings("Item"))) // Datatype of answer: Double
          ),
          "4.Baseline" -> ujson.Obj(
            "average (ms)" -> ujson.Num(mean(timings("Baseline"))), // Datatype of answer: Double
            "stddev (ms)" -> ujson.Num(std(timings("Baseline"))) // Datatype of answer: Double
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
