package shared

import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.math.{abs, pow, sqrt}

package object predictions {
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
                                   .map(x => x._1 -> x._2._1 / x._2._2).collectAsMap.toMap

    // A function that returns the average rating by item given a train set
    val itemAvg: RDD[Rating] => Map[Int, Double] =
      (train: RDD[Rating]) => train.map(x => (x.item, (x.rating, 1)))
                                   .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
                                   .map(x => x._1 -> x._2._1 / x._2._2).collectAsMap.toMap

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
        (x.rating - avgRatingByUser(x.user)) / {
          if (x.rating > avgRatingByUser(x.user)) 5 - avgRatingByUser(x.user)
          else if (x.rating < avgRatingByUser(x.user)) avgRatingByUser(x.user) - 1
          else 1
        }))
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
    def userAvgPredictor(train: RDD[Rating]): (Int, Int) => Double = {
      val avgRatingByUser = userAvg(train)
      (user: Int, item: Int) => avgRatingByUser(user)
    }

    /**
     * Predictor function with given signature which always returns the average score per item
     *
     * @param train Training data
     * @return The average score per item
     */
    def itemAvgPredictor(train: RDD[Rating]): (Int, Int) => Double = {
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
    def itemAvgDev(train: RDD[Rating], item: Int): Double = {
      val avgRatingDevByItem = itemAvg(normalizeData(train, userAvg(train)))
      avgRatingDevByItem(item)
    }

    /**
     * Computes a prediction according to Equation 5.
     *
     * @param train : The train data
     * @return A prediction for an item and a user based on train data
     */
    def baselinePredictor(train: RDD[Rating]): (Int, Int) => Double = {
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
      test.map(x => (predictorFunc(x.user, x.item) - x.rating).abs).mean
    }
  }


  /**
   * This class contains the functions that generate the results used only in the Personalized predictions.
   */
  class PersonalizedSolver(train: Seq[Rating], test: Seq[Rating]) extends BaselineSolver(test) {
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

    /**
     * Preprocess the whole dataset according to the law of multiplication to make computation faster.
     *
     * @param data                    The list of all ratings
     * @param normalizedRatingsByUser The normalized ratings by user
     * @return The initial dataset, with the preprocessed ratings
     */
    def preprocessData(data: Seq[Rating], normalizedRatingsByUser: Map[Int, Seq[Rating]]): Seq[Rating] = {
      // The 2-norm of user ratings by user
      val norm2RatingsByUser = normalizedRatingsByUser.mapValues(x => sqrt(x.map(y => pow(y.rating, 2)).sum))
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
    val userUniformSimilarity: (Int, Int) => Double = (u1: Int, u2: Int) => 1.0

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
      // Combine user ratings, group them by item and only select groups that have exactly 2 members
      val intersection = (user1Ratings ++ user2Ratings).groupBy(x => x.item)
                                                       .filter { case (_, v) => v.length == 2 }
                                                       .map { case (_, v) => v }
      intersection.map(x => x.head.rating * x(1).rating).sum
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

    /**
     * Returns the user-specific weighted-sum deviation for an item.
     *
     * @param user       The user for which the rating will be computed.
     * @param item       The item for which the rating will be computed.
     * @param similarity The function that will be used to measure similarity.
     * @return The rating.
     */
    def getUserItemAvgDev(user: Int, item: Int, similarity: mutable.Map[Int, mutable.Map[Int, Double]]): Double = {
      val relevantRatings = normalizedRatingsByItem.getOrElse(item, Seq[Rating]())
      val numerator = relevantRatings.map(x => similarity(user)(x.user) * x.rating).sum
      val denominator = relevantRatings.map(x => abs(similarity(user)(x.user))).sum
      if (denominator != 0) {
        return numerator / denominator
      }
      0
    }

    /**
     * Computes all similarity values for all users in the train dataset.
     *
     * @param similarityFunc The function that will be used to assess how similar are two users
     * @return A map of all similarities between any two unique users
     */
    protected def computeAllSimilarities(similarityFunc: (Int, Int) => Double): mutable.Map[Int,
      mutable.Map[Int, Double]] = {
      val userSimilarities = mutable.Map.empty[Int, mutable.Map[Int, Double]]
      for (user1 <- uniqueUsers) {
        // Initialize map of similarities
        userSimilarities(user1) = mutable.Map.empty[Int, Double]
      }
      for (user1 <- uniqueUsers) {
        for (user2 <- uniqueUsers) {
          val similarity = similarityFunc(user1, user2)
          userSimilarities(user1)(user2) = similarity
        }
      }
      userSimilarities
    }

    /**
     * Generated a predicted score for a given user and item.
     *
     * @param user         The user for which the prediction will be computed
     * @param item         The item for which the prediction will be computed
     * @param similarities The pre-computed similarities between all users
     * @return A predicted rating
     */
    def prediction(user: Int, item: Int, similarities: mutable.Map[Int, mutable.Map[Int, Double]]): Double = {
      val userItemAvgDev = getUserItemAvgDev(user = user, item = item, similarities)
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
     * Generates a function that can compute a prediction for an item for an user based on train data.
     *
     * @param similarities A list of precomputed similarities
     * @return The predicted rating
     */
    def personalizedPredictor(similarities: mutable.Map[Int, mutable.Map[Int, Double]]): (Int, Int) => Double = {
      prediction(_, _, similarities)
    }

    /**
     * Generates a function that can compute a prediction for an item for an user based on train data.
     * Computes all similarities between users as a first step.
     *
     * @param similarityFunc The function that will be used to measure similarity between 2 users
     * @return The predicted rating
     */
    def personalizedPredictor(similarityFunc: (Int, Int) => Double): (Int, Int) => Double = {
      // Compute similarities
      val userSimilarities = computeAllSimilarities(similarityFunc)
      prediction(_, _, userSimilarities)
    }

    /**
     * Extracts predictions based on the given predictor function and returns MAE
     *
     * @param similarityFunc The similarity function that will be used to compare users.
     * @return Mean Average Error between the predictions and the test.
     */
    override def getMAE(similarityFunc: (Int, Int) => Double): Double = {
      val predictor = personalizedPredictor(similarityFunc)
      mean(test.map(x => (predictor(x.user, x.item) - x.rating).abs))
    }
  }

  /**
   * This class contains the functions that generate the results used only in the Neighbourhood-based predictions.
   */
  class KNNSolver(train: Seq[Rating], test: Seq[Rating], k: Int) extends PersonalizedSolver(train, test) {
    // Store the similarities of all users
    lazy val userSimilarities: mutable.Map[Int, mutable.Map[Int, Double]] = computeAllSimilarities(userCosineSimilarity)
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
      userSimilarities(user1)(user2)
    }

    /**
     * Get the k-nearest neighbors by similarity
     *
     * @param user The user for which the k-nearest neighbours will be computed
     * @return The k-nearest neighbours
     */
    private def getKNearestUsers(user: Int): Seq[Int] = {
      // Sort users by similarity, descending
      val sortedUserSimilarities = userSimilarities(user).toSeq.sortBy(-_._2)

      // Return the k-nearest neighbors
      val topK = sortedUserSimilarities.take(k).map(x => x._1)
      if (topK == null) {
        return Seq[Int]()
      }
      topK
    }

    /**
     * Returns the user-specific weighted-sum deviation for an item, based on the k-nearest neighbors.
     *
     * @param user       The user for which the rating will be computed.
     * @param item       The item for which the rating will be computed.
     * @param similarity The function that will be used to measure similarity.
     * @return The rating.
     */
    override def getUserItemAvgDev(user: Int, item: Int,
                                   similarity: mutable.Map[Int, mutable.Map[Int, Double]]): Double = {
      val relevantUserRatings = KNearestUsers(user)
        .flatMap(x => normalizedRatingsByUser(x).filter(y => y.item == item))
      val numerator = relevantUserRatings.map(x => userSimilarities(user)(x.user) * x.rating).sum
      val denominator = relevantUserRatings.map(x => abs(userSimilarities(user)(x.user))).sum
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
      val predictor = personalizedPredictor(userSimilarities)(_, _)
      val allPredictions: Seq[(Int, Double)] = notRatedMovies
        .map(x => (x, predictor(user, x))).toSeq

      // Sort predictions
      val sortedPredictions = scala.util.Sorting.stableSort(allPredictions,
        (e1: (Int, Double), e2: (Int, Double)) => e1._2 > e2._2 || e1._2 == e2._2 && e1._1 < e2._1)

      // Return the top predictions
      sortedPredictions.take(top).toList
    }
  }
}

