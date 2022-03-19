package shared

import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.math.{abs, pow, sqrt}

package object predictions {
  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")
    result
  }

  def timingInMs(f: () => Double): (Double, Double) = {
    val start = System.nanoTime()
    val output = f()
    val end = System.nanoTime()
    return (output, (end - start) / 1000000.0)
  }

  def std(s: Seq[Double]): Double = {
    if (s.size == 0) 0.0
    else {
      val m = mean(s)
      scala.math.sqrt(s.map(x => scala.math.pow(m - x, 2)).sum / s.length.toDouble)
    }
  }

  def mean(s: Seq[Double]): Double = if (s.size > 0) s.reduce(_ + _) / s.length else 0.0

  def mean(s: RDD[Double]): Double = {
    val t = s.map(rating => (rating, 1.0)).reduce({
      case ((acc, acc_c), (rating, rating_c)) => (acc + rating, acc_c + rating_c)
    })
    t._1 / t._2
  }

  def load(spark: org.apache.spark.sql.SparkSession, path: String, sep: String): org.apache.spark.rdd.RDD[Rating] = {
    val file = spark.sparkContext.textFile(path)
    return file
      .map(l => {
        val cols = l.split(sep).map(_.trim)
        toInt(cols(0)) match {
          case Some(_) => Some(Rating(cols(0).toInt, cols(1).toInt, cols(2).toDouble))
          case None => None
        }
      })
      .filter({ case Some(_) => true
      case None => false
      })
      .map({ case Some(x) => x
      case None => Rating(-1, -1, -1)
      })
  }

  def toInt(s: String): Option[Int] = {
    try {
      Some(s.toInt)
    } catch {
      case e: Exception => None
    }
  }

  case class Rating(user: Int, item: Int, rating: Double)

  /**
   * This class contains the functions that generate the results used only in the Baseline predictions.
   */
  class BaselineSolver(test: Seq[Rating]) {
    // A function that returns the global average rating given a train set
    val globalAvg: Seq[Rating] => Double = (train: Seq[Rating]) => mean(train.map(x => x.rating))
    // A function that returns the average rating by user given a train set
    val userAvg: Seq[Rating] => Map[Int, Double] = (train: Seq[Rating]) => train.groupBy(x => x.user).map {
      case (k, v) => (k, mean(v.map(x => x.rating)))
    }
    // A function that returns the average rating by item given a train set
    val itemAvg: Seq[Rating] => Map[Int, Double] = (train: Seq[Rating]) => train.groupBy(x => x.item).map {
      case (k, v) => (k, mean(v.map(x => x.rating)))
    }

    /**
     * Function that normalizes the ratings of a dataset.
     *
     * @param train           The dataset that will be normalized
     * @param avgRatingByUser The average rating by user
     * @return The normalized dataset
     */
    def normalizeData(train: Seq[Rating], avgRatingByUser: Map[Int, Double]): Seq[Rating] = {
      // Normalize each rating
      train.map(x => Rating(x.user, x.item,
        (x.rating - avgRatingByUser(x.user)) / scaleUserRating(x.rating, avgRatingByUser(x.user))))
    }

    /**
     * Scales the user rating as defined by Equation 2
     *
     * @param currRating The rating for item i, given by user u
     * @param avgRating  The average rating, given by user u
     * @return A scaled rating
     */
    def scaleUserRating(currRating: Double, avgRating: Double): Double = {
      if (currRating > avgRating) 5 - avgRating
      else if (currRating < avgRating) avgRating - 1
      else 1
    }

    /**
     * Predictor function with given signature which always returns the global average
     *
     * @param train Training data
     * @return Global average of training data
     */
    def globalAvgPredictor(train: Seq[Rating]): (Int, Int) => Double = {
      val avgGlobal = globalAvg(train)
      (user: Int, item: Int) => avgGlobal
    }

    /**
     * Predictor function with given signature which always returns the average score per user
     *
     * @param train Training data
     * @return The average score per user
     */
    def userAvgPredictor(train: Seq[Rating]): (Int, Int) => Double = {
      val avgRatingByUser = userAvg(train)
      (user: Int, item: Int) => avgRatingByUser(user)
    }

    /**
     * Predictor function with given signature which always returns the average score per item
     *
     * @param train Training data
     * @return The average score per item
     */
    def itemAvgPredictor(train: Seq[Rating]): (Int, Int) => Double = {
      val avgRatingByItem = itemAvg(train)
      val avgRatingByUser = userAvg(train)
      val avgGlobal = globalAvg(train)
      (user: Int, item: Int) => {
        if (!avgRatingByItem.contains(item)) {
          if (!avgRatingByUser.contains(user)) {
            avgGlobal
          }
          else {
            avgRatingByUser(user)
          }
        }
        else avgRatingByItem(item)
      }
    }

    /**
     * Get the average deviation score for a single item.
     *
     * @param train Training data
     * @return The average deviation of an item
     */
    def itemAvgDev(train: Seq[Rating], item: Int): Double = {
      val avgRatingDevByItem = itemAvg(normalizeData(train, userAvg(train)))
      avgRatingDevByItem(item)
    }

    /**
     * Computes a prediction according to Equation 5.
     *
     * @param train : The train data
     * @return A prediction for an item and a user based on train data
     */
    def baselinePredictor(train: Seq[Rating]): (Int, Int) => Double = {
      val avgRatingByUser = userAvg(train)
      val avgRatingDevByItem = itemAvg(normalizeData(train, avgRatingByUser))
      val avgGlobal = globalAvg(train)

      def prediction(user: Int, item: Int): Double = {
        if (!avgRatingDevByItem.contains(item) || avgRatingDevByItem(item) == 0) {
          // No rating for i in the training set of the item average dev is 0
          if (!avgRatingByUser.contains(user)) {
            // The user has no rating
            return avgGlobal
          }
          return avgRatingByUser(user)
        }
        val userAvg = avgRatingByUser(user)
        val itemAvgDev = avgRatingDevByItem(item)
        userAvg + itemAvgDev * scaleUserRating(userAvg + itemAvgDev, userAvg)
      }

      prediction
    }

    /**
     * Extracts predictions based on the given predictor function and returns MAE
     *
     * @param predictorFunc Estimates the prediction for given item and user
     * @return Mean Average Error between the predictions and the test
     */
    def getMAE(predictorFunc: (Int, Int) => Double): Double = {
      mean(test.map(x => (predictorFunc(x.user, x.item) - x.rating).abs))
    }
  }

  class DistributedSolvers(test: RDD[Rating]) extends java.io.Serializable {
    // A function that returns the global average rating given a train set
    val globalAvg: RDD[Rating] => Double = (train: RDD[Rating]) => train.map(x => x.rating).mean
    // A function that returns the average rating by user given a train set
    val userAvg: RDD[Rating] => Map[Int, Double] =
      (train: RDD[Rating]) => train.map(x => (x.user, (x.rating, 1)))
                                   .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
                                   .map(x => x._1 -> x._2._1 / x._2._2).collect().toMap

    // A function that returns the average rating by item given a train set
    val itemAvg: RDD[Rating] => Map[Int, Double] =
      (train: RDD[Rating]) => train.map(x => (x.item, (x.rating, 1)))
                                   .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
                                   .map(x => x._1 -> x._2._1 / x._2._2).collect().toMap

    def getGlobalAvg(train: RDD[Rating]): (Int, Int) => Double = {
      (user: Int, item: Int) => {
        train.map(x => x.rating).mean()
      }
    }


    /**
     * Function that normalizes the ratings of a dataset.
     *
     * @param train           The dataset that will be normalized
     * @param avgRatingByUser The average rating by user
     * @return The normalized dataset
     */
    def normalizeData(train: RDD[Rating], avgRatingByUser: Map[Int, Double]): RDD[Rating] = {

      // Normalize each rating
      train.map(x => Rating(x.user, x.item,
        (x.rating - avgRatingByUser(x.user)) / scaleUserRating(x.rating, avgRatingByUser(x.user))))
    }

    /**
     * Scales the user rating as defined by Equation 2
     *
     * @param currRating The rating for item i, given by user u
     * @param avgRating  The average rating, given by user u
     * @return A scaled rating
     */
    val scaleUserRating: (Double, Double) => Double = (currRating: Double, avgRating: Double) => {
      if (currRating > avgRating) 5 - avgRating
      else if (currRating < avgRating) avgRating - 1
      else 1
    }

    /**
     * Predictor function with given signature which always returns the global average
     *
     * @param train Training data
     * @return Global average of training data
     */
    def globalAvgPredictor(train: RDD[Rating]): (Int, Int) => Double = {
      val avgGlobal = globalAvg(train)
      (user: Int, item: Int) => avgGlobal
    }

    /**
     * Predictor function with given signature which always returns the average score per user
     *
     * @param train Training data
     * @return The average score per user
     */
    def getUserAvg(train: RDD[Rating]): (Int, Int) => Double = {
      (user: Int, item: Int) => {
        train.filter(x => x.user == user).map(x => x.rating).mean()
      }
    }

    /**
     * Predictor function with given signature which always returns the average score per item
     *
     * @param train Training data
     * @return The average score per item
     */
    def getItemAvg(train: RDD[Rating]): (Int, Int) => Double = {
      (user: Int, item: Int) => {
        if (train.filter(x => x.item == item).count() == 0) getGlobalAvg(train)(0, 0)
        else train.filter(x => x.item == item).map(x => x.rating).mean()
      }
    }

    /**
     * Get the average deviation score for a single item.
     *
     * @param train Training data
     * @return The average deviation of an item
     */
    def getItemAvgDev(train: RDD[Rating], item: Int): Double = {
      getItemAvg(normalizeData(train, userAvg(train)))(0, item)
    }

    /**
     * Computes a prediction according to Equation 5.
     *
     * @param train : The train data
     * @return A prediction for an item and a user based on train data
     */
    def baselinePredictor(train: RDD[Rating]): (Int, Int) => Double = {
      //      println("B")
      //      val avgRatingByUser = userAvg(train)
      //      println("a")
      //      val avgRatingDevByItem = itemAvg(normalizeData(train, avgRatingByUser))
      //      println("C")
      //      val avgGlobal = globalAvg(train)
      //      println("d")

      def prediction(user: Int, item: Int): Double = {

        val userAvg = getUserAvg(train)(user, 0)
        val itemAvgDev = getItemAvgDev(train, item)
        if (itemAvgDev == 0) {
          // No rating for i in the training set of the item average dev is 0
          if (train.filter(x => x.user == user).count() == 0) {
            // The user has no rating
            return globalAvg(train)
          }
          return getUserAvg(train)(user, 0)
        }
        userAvg + itemAvgDev * scaleUserRating(userAvg + itemAvgDev, userAvg)
      }

      prediction
    }

    def baselineRDDPredictor(itemAverageDev: Map[Int, Double],
                             userAverage: Map[Int, Double],
                             globalAverage: Double):
    (Int, Int) => Double = {
      def prediction(user: Int, item: Int): Double = {
        if (!(userAverage contains user)) {
          // The user has no rating
          return globalAverage
        }
        if (!(itemAverageDev contains item) || itemAverageDev(item) == 0) {
          // No rating for i in the training set of the item average dev is 0
          return userAverage(user)
        }
        val userAvg = userAverage(user)
        val itemAvgDev = itemAverageDev(item)
        userAvg + itemAvgDev * scaleUserRating(userAvg + itemAvgDev, userAvg)
      }

      prediction
    }

    /**
     * Extracts predictions based on the given predictor function and returns MAE
     *
     * @param predictorFunc Estimates the prediction for given item and user
     * @return Mean Average Error between the predictions and the test
     */
    def getMAE(train: RDD[Rating], predictorFunc: (Int, Int) => Double): Double = {
      val itemDevAverage = itemAvg(normalizeData(train, userAvg(train)))
      val userAverage = userAvg(test)
      val globalAverage = globalAvg(train)
      print(test.count())
      print(test.first())
      test.map(x => (baselineRDDPredictor(itemDevAverage, userAverage, globalAverage)(x.user, x.item) - x.rating).abs)
          .mean
    }
  }

  /**
   * An enum for similarity functions
   */
  sealed trait SimilarityFunctions

  case object Uniform extends SimilarityFunctions

  case object Cosine extends SimilarityFunctions

  case object Jaccard extends SimilarityFunctions

  /**
   * This class contains the functions that generate the results used only in the Personalized predictions.
   */
  class PersonalizedSolver(train: Seq[Rating], test: Seq[Rating], similarityMeasure: SimilarityFunctions)
    extends BaselineSolver(test) {
    // The average rating per user
    lazy val avgRatingByUser: Map[Int, Double] = userAvg(train)
    // The average global rating
    lazy val avgGlobal: Double = globalAvg(train)
    // The normalized train set
    lazy val normalizedTrain: Seq[Rating] = normalizeData(train, avgRatingByUser)
    // The normalized ratings by item
    lazy val normalizedRatingsByItem: Map[Int, Seq[Rating]] = normalizedTrain.groupBy(x => x.item)
    // The normalized ratings by user
    lazy val normalizedRatingsByUser: Map[Int, Seq[Rating]] = normalizedTrain.groupBy(x => x.user)
    // The preprocessed train set
    lazy val preprocessedTrain: Seq[Rating] = preprocessData(normalizedTrain, normalizedRatingsByUser)
    // The preprocessed ratings by user
    lazy val preprocessedRatingsByUser: Map[Int, Seq[Rating]] = preprocessedTrain.groupBy(x => x.user)
    // The list of unique user IDs
    lazy val uniqueUsers: Seq[Int] = train.map(x => x.user).distinct
    // Stores the similarity function that will be used
    val similarityFunc: (Int, Int) => Double = {
      similarityMeasure match {
        case Uniform => userUniformSimilarity
        case Cosine => userCosineSimilarity
        case Jaccard => userJaccardSimilarity
      }
    }

    // Stores the computed similarities
    var similarities: mutable.Map[Int, mutable.Map[Int, Double]] = mutable.Map[Int, mutable.Map[Int, Double]]()


    /**
     * Preprocess the whole dataset according to the law of multiplication to make computation faster.
     *
     * @param data                    The list of all ratings
     * @param normalizedRatingsByUser The normalized ratings by user
     * @return The initial dataset, with the preprocessed ratings
     */
    def preprocessData(data: Seq[Rating], normalizedRatingsByUser: Map[Int, Seq[Rating]]): Seq[Rating] = {
      // The 2-norm of user ratings by user
      val norm2RatingsByUser = mutable.Map[Int, Double]()
      for ((user, ratings) <- normalizedRatingsByUser) {
        var norm2 = 0.0
        for (rating <- ratings) {
          norm2 += pow(rating.rating, 2)
        }
        norm2 = sqrt(norm2)
        norm2RatingsByUser(user) = norm2
      }

      data.map(x => {
        val numerator = x.rating
        val denominator = norm2RatingsByUser(x.user)
        var fraction: Double = 0.0
        if (denominator != 0) {
          fraction = numerator / denominator
        }
        Rating(user = x.user, item = x.item, rating = fraction)
      })
    }

    /**
     * Get the uniform similarity score between two users. Always consider similarity 1.
     */
    def userUniformSimilarity: (Int, Int) => Double = (user1: Int, user2: Int) => 1.0

    /**
     * Get the cosine similarity score between two users.
     *
     * @param user1 The first user
     * @param user2 The second user
     * @return The cosine similarity
     */
    def userCosineSimilarity(user1: Int, user2: Int): Double = {
      if (user1 == user2) {
        return 0
      }
      val user1Ratings = preprocessedRatingsByUser.getOrElse(user1, Seq())
      val user2Ratings = preprocessedRatingsByUser.getOrElse(user2, Seq())

      var similarity = 0.0
      for (a <- user1Ratings; b <- user2Ratings) {
        if (a.item == b.item) {
          similarity += a.rating * b.rating
        }
      }
      similarity
    }

    /**
     * Get the Jaccard similarity score between two users.
     *
     * @param user1 The first user
     * @param user2 The second user
     * @return The Jaccard similarity
     */
    def userJaccardSimilarity(user1: Int, user2: Int): Double = {
      if (user1 == user2) {
        return 0
      }
      val user1Ratings = normalizedRatingsByUser.getOrElse(user1, Seq()).map(x => x.item).toSet
      val user2Ratings = normalizedRatingsByUser.getOrElse(user2, Seq()).map(x => x.item).toSet
      val itemIntersection = user1Ratings.intersect(user2Ratings).size
      val itemUnion = user1Ratings.union(user2Ratings).size
      if (itemUnion == 0) {
        return 0
      }
      itemIntersection * 1.0 / itemUnion
    }

    def getSimilarity(user1: Int, user2: Int): Double = {
      var similarity = 0.0
      if (!similarities.contains(user1)) {
        similarities(user1) = mutable.Map[Int, Double]()
      }
        if (similarities(user1).contains(user2)) {
          similarity = similarities(user1)(user2)
        } else {
          similarity = similarityFunc(user1, user2)
          similarities(user1)(user2) = similarity
          if (!similarities.contains(user2)) {
            similarities(user2) = mutable.Map[Int, Double]()
          }
          similarities(user2)(user1) = similarity
        }
      similarity
    }

    /**
     * Returns the user-specific weighted-sum deviation for an item.
     *
     * @param user       The user for which the rating will be computed.
     * @param item       The item for which the rating will be computed.
     * @return The rating.
     */
    def getUserItemAvgDev(user: Int, item: Int): Double = {
      val relevantRatings = normalizedRatingsByItem.getOrElse(item, Seq[Rating]())
      var numerator = 0.0
      var denominator = 0.0
      for (rating <- relevantRatings) {
        var similarity = getSimilarity(user, rating.user)
        numerator = numerator + similarity * rating.rating
        denominator = denominator + abs(similarity)
      }
      if (denominator != 0) {
        return numerator / denominator
      }
      0
    }


    /**
     * Generates a function that can compute a prediction for an item for an user based on train data.
     *
     * @return The predicted rating
     */
    def personalizedPredictor(user: Int, item: Int): Double = {
      val userItemAvgDev = getUserItemAvgDev(user = user, item = item)
      if (!normalizedRatingsByItem.contains(item) || userItemAvgDev == 0) {
        // No rating for i in the training set of the item average dev is 0
        if (!avgRatingByUser.contains(user)) {
          // The user has no rating
          return avgGlobal
        }
        return avgRatingByUser(user)
      }
      val userAvg = avgRatingByUser(user)
      userAvg + userItemAvgDev * scaleUserRating(userAvg + userItemAvgDev, userAvg)
    }

    /**
     * Extracts predictions based on the given predictor function and returns MAE
     *
     * @return Mean Average Error between the predictions and the test.
     */
    def getMAE: Double = {
      mean(test.map(x => (personalizedPredictor(x.user, x.item) - x.rating).abs))
    }
  }

  /**
   * This class contains the functions that generate the results used only in the Neighbourhood-based predictions.
   */
  class KNNSolver(train: Seq[Rating], test: Seq[Rating], k: Int) extends PersonalizedSolver(train, test, Cosine) {
    // Store the K-nearest neighbors of a user
    lazy val KNearestUsers: Map[Int, Seq[Int]] = uniqueUsers.map(x => x -> getKNearestUsers(x)).toMap

    /**
     * Computes the similarity between 2 users as defined in KNN.
     *
     * @param user1 The first user
     * @param user2 The second user
     * @return The similarity between 2 users
     */
    def kNearestSimilarity(user1: Int, user2: Int): Double = {
      if (user1 == user2) {
        return 0
      }
      if (!KNearestUsers(user1).contains(user2)) {
        return 0
      }
      getSimilarity(user1, user2)
    }

    /**
     * Get the k-nearest neighbors by similarity
     *
     * @param user The user for which the k-nearest neighbours will be computed
     * @return The k-nearest neighbours
     */
    private def getKNearestUsers(user: Int): Seq[Int] = {
      // Sort users by similarity, descending
      var userSimilarities: Array[(Double, Int)] = Array[(Double, Int)]()
      for (otherUser <- uniqueUsers) {
        userSimilarities = userSimilarities :+ (-getSimilarity(user, otherUser), otherUser)
      }
      if (user <= 10) {
        println("Before:")
        println(userSimilarities.mkString("Array(", ", ", ")"))
      }
      scala.util.Sorting.quickSort(userSimilarities)
      if (user <= 10) {
        println("After:")
        println(userSimilarities.mkString("Array(", ", ", ")"))
      }
      // Return the k-nearest neighbors
      val topK = userSimilarities.take(k).map(x => x._2)
      if (topK == null) {
        return Seq[Int]()
      }
      topK.toSeq
    }

    /**
     * Returns the user-specific weighted-sum deviation for an item, based on the k-nearest neighbors.
     *
     * @param user       The user for which the rating will be computed.
     * @param item       The item for which the rating will be computed.
     * @return The rating.
     */
    override def getUserItemAvgDev(user: Int, item: Int): Double = {
      val relevantRatings = KNearestUsers(user)
        .flatMap(x => normalizedRatingsByUser(x).filter(y => y.item == item))
      var numerator = 0.0
      var denominator = 0.0
      for (rating <- relevantRatings) {
        val similarity = getSimilarity(user, rating.user)
        numerator = numerator + similarity * rating.rating
        denominator = denominator + abs(similarity)
      }
      if (denominator != 0) {
        return numerator / denominator
      }
      0
    }
  }

  /**
   * This class contains the functions that generate the results used only in the Recommender predictions.
   */
  class RecommenderSolver(train: Seq[Rating], movieNames: Map[Int, String], k: Int)
    extends KNNSolver(train, Seq(), k) {

    /**
     * Recommend the top movies for a user using k-NN.
     *
     * @param user The user for which the recommendations will be made.
     * @param top  The number of movies which will be recommended.
     * @return A list of top movies, with the highest predicted score for the user.
     */
    def getRecommendations(user: Int, top: Int): List[(Int, Double)] = {
      // Get movies that were not rated by user
      val allMovies = movieNames.keys.toSet
      val ratedMovies = preprocessedRatingsByUser(user).map(x => x.item).toSet
      val notRatedMovies = allMovies.diff(ratedMovies)

      // Compute predicted score for movies that were not rated
      val allPredictions: Seq[(Int, Double)] = notRatedMovies
        .map(x => (x, personalizedPredictor(user, x))).toSeq

      // Sort predictions
      val sortedPredictions = scala.util.Sorting.stableSort(allPredictions,
        (e1: (Int, Double), e2: (Int, Double)) => e1._2 > e2._2 || e1._2 == e2._2 && e1._1 < e2._1)

      // Return the top predictions
      sortedPredictions.take(top).toList
    }
  }
}

