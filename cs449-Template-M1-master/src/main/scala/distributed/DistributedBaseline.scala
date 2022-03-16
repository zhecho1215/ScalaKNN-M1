package distributed

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.rogach.scallop._
import shared.predictions._
import ujson._

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val train = opt[String](required = true)
  val test = opt[String](required = true)
  val separator = opt[String](default = Some("\t"))
  val master = opt[String](default = Some(""))
  val num_measurements = opt[Int](default = Some(0))
  val json = opt[String]()
  verify()
}

object DistributedBaseline extends App {
  var conf = new Conf(args)

  // Remove these lines if encountering/debugging Spark
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  val spark = if (conf.master() != "") {
    SparkSession.builder().master(conf.master()).getOrCreate()
  } else {
    SparkSession.builder().getOrCreate()
  }
  spark.sparkContext.setLogLevel("ERROR")

  println("")
  println("******************************************************")

  println("Loading training data from: " + conf.train())
  val train = load(spark, conf.train(), conf.separator())
  println("Loading test data from: " + conf.test())
  val test = load(spark, conf.test(), conf.separator())

  // Initialize the solvers for questions in D
  val solver = new DistributedSolvers(test)

  val measurements = (1 to conf.num_measurements()).map(x => timingInMs(() => {
    val solver = new DistributedSolvers(test)
    solver.getMAE(train, solver.baselinePredictor(train))
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

  conf.json.toOption match {
    case None => ;
    case Some(jsonFile) => {
      val answers = ujson.Obj(
        "Meta" -> ujson.Obj(
          "1.Train" -> conf.train(),
          "2.Test" -> conf.test(),
          "3.Master" -> conf.master(),
          "4.Measurements" -> conf.num_measurements()
        ),
        "D.1" -> ujson.Obj(
          // Datatype of answer: Double
          "1.GlobalAvg" -> ujson.Num(solver.globalAvg(train)),
          // Datatype of answer: Double
          "2.User1Avg" -> ujson.Num(solver.getUserAvg(train)(1, 0)),
          // Datatype of answer: Double
          "3.Item1Avg" -> ujson.Num(solver.getItemAvg(train)(0, 1)),
          // Datatype of answer: Double,
          "4.Item1AvgDev" -> ujson.Num(solver.getItemAvgDev(train, 1)),
          // Datatype of answer: Double
          "5.PredUser1Item1" -> ujson.Num(solver.baselinePredictor(train)(1, 1)),
           //Datatype of answer: Double
          "6.Mae" -> ujson.Num(solver.getMAE(train, solver.baselinePredictor(train)))
        ),
        "D.2" -> ujson.Obj(
          "1.DistributedBaseline" -> ujson.Obj(
            "average (ms)" -> ujson.Num(mean(timings)), // Datatype of answer: Double
            "stddev (ms)" -> ujson.Num(std(timings)) // Datatype of answer: Double
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
