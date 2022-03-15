package shared

import org.apache.spark.rdd.RDD

import scala.math.{abs, pow, sqrt}
import scala.util.Sorting

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
    // Apply preprocessing operations on the train and test
    val globalAverage: Seq[Rating] => Double = (train: Seq[Rating]) => mean(train.map(x => x.rating))
    val userAvg: Seq[Rating] => Map[Int, Double] = (train: Seq[Rating]) => train.groupBy(x => x.user).map {
      case (k, v) => (k, mean(v.map(x => x.rating)))
    }
    val itemAvg: Seq[Rating] => Map[Int, Double] = (train: Seq[Rating]) => train.groupBy(x => x.item).map {
      case (k, v) => (k, mean(v.map(x => x.rating)))
    }

    /**
     * Function that normalizes the ratings of a dataset.
     *
     * @param data The input dataset, with non-normalized ratings.
     * @return The normalized ratings.
     */
    def normalizeData(train: Seq[Rating]): Seq[Rating] = {
      val avgRatingByUser = userAvg(train)
      // Normalize each rating
      val normalizedData = train.map(x => Rating(x.user, x.item,
        (x.rating - avgRatingByUser(x.user)) / scaleUserRating(x.rating, avgRatingByUser(x.user))))
      normalizedData
    }

    /**
     * Function 'scale' which allows for computation of normalized deviation as defined by Equation 2
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
     * @return Global average of train data
     */
    def globalAvgPredictor(train: Seq[Rating]): (Int, Int) => Double = {
      val avgGlobal = globalAverage(train)
      (user: Int, item: Int) => avgGlobal
    }

    /**
     * Get the average score per user.
     *
     * @param train Training data.
     * @return The average score per user.
     */
    def userAvgPredictor(train: Seq[Rating]): (Int, Int) => Double = {
      val avgRatingByUser = userAvg(train)
      (user: Int, item: Int) => avgRatingByUser(user)
    }

    /**
     * Get the average score per item.
     *
     * @param train Training data
     * @return Item average in training data
     */
    def itemAvgPredictor(train: Seq[Rating]): (Int, Int) => Double = {
      val avgRatingByItem = itemAvg(train)
      val avgRatingByUser = userAvg(train)
      val avgGlobal = globalAverage(train)
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
     * Get the average deviation score per item.
     *
     * @param train Training data
     * @return Item average in training data
     */
    def itemAvgDev(train: Seq[Rating], item: Int): Double = {
      val avgRatingDevByItem = itemAvg(normalizeData(train))
      avgRatingDevByItem(item)
    }

    /**
     * Method to compute a prediction according to Equation (5)
     *
     * @param user ID of user
     * @param item ID of item
     * @return A prediction for an item for an user based on train data.
     */
    def baselinePredictor(train: Seq[Rating]): (Int, Int) => Double = {
      val avgRatingByUser = userAvg(train)
      val avgRatingDevByItem = itemAvg(normalizeData(train))
      val avgGlobal = globalAverage(train)

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
     * @param predictorFunc Estimates the prediction for given item and user.
     * @return Mean Average Error between the predictions and the test.
     */
    def getMAE(predictorFunc: (Int, Int) => Double): Double = {
      mean(test.map(x => (predictorFunc(x.user, x.item) - x.rating).abs))
    }
  }

  class DistributedSolvers(train: RDD[Rating], test: RDD[Rating]) {

    //TODO: Pass normalized train
    def getItemAvgDev(train: RDD[Rating], item: Int): Double = {
      train.filter(x => x.item == item).map(x => x.rating).mean()
    }

    /**
     * Predictor function with given signature which always returns the global average
     *
     * @param train Training data
     * @return Global average of train data
     */
    def getGlobalAvg(train: RDD[Rating]): (Int, Int) => Double = {
      def ratingPrediction(user: Int, item: Int): Double = {
        train.map(x => x.rating).mean()
      }

      ratingPrediction
    }

    /**
     * Predictor function
     *
     * @param train Training data
     * @return A function which takes the
     */
    def getUserAvg(train: RDD[Rating]): (Int, Int) => Double = {
      def ratingPrediction(user: Int, item: Int): Double = {
        train.filter(x => x.user == user).map(x => x.rating).mean()
      }

      ratingPrediction
    }

    /**
     * Predictor function with given signature which always returns the item average given an item ID
     *
     * @param train Training data
     * @return Item average in training data
     */
    def getItemAvg(train: RDD[Rating]): (Int, Int) => Double = {
      def ratingPrediction(user: Int, item: Int): Double = {
        if (train.filter(x => x.item == item).count() == 0) getGlobalAvg(train)(0, 0)
        else train.filter(x => x.item == item).map(x => x.rating).mean()
      }

      ratingPrediction
    }

    /**
     * Method to compute a prediction according to Equation (5)
     *
     * @param train Training data
     * @param item  ID of item
     * @param user  ID of user
     * @return A prediction for an item for an user based on train data.
     */
    def getPredUserItem(train: RDD[Rating], item: Int, user: Int): Double = {
      val userAvg = getUserAvg(train)(user, 0)
      val itemAvgDev = getItemAvgDev(train, item)

      if (itemAvgDev == 0 || train.filter(x => x.item == item).count() == 0) {
        // No rating for i in the training set of the item average dev is 0
        if (train.filter(x => x.user == user).count() == 0) {
          // The user has not rating
          return getGlobalAvg(train)(0, 0)
        }
        return userAvg
      }

      // userAvg + itemAvgDev * scaleUserRating(userAvg + itemAvgDev, userAvg)
      42
    }

    /**
     *
     * @param train Training data
     * @return A predicted score using the baseline approach (Equation 5)
     */
    def getBaseline(train: RDD[Rating]): (Int, Int) => Double = {
      def ratingPrediction(user: Int, item: Int): Double = {
        getPredUserItem(train = train, item = item, user = user)
      }

      ratingPrediction
    }


    /**
     * Extracts predictions based on the given predictor function and returns MAE
     *
     * @param predictorFunc Estimates the prediction for given item and user
     * @return Mean Average Error between the predictions and the test.
     */
    def getPredictorMAE(predictorFunc: RDD[Rating] => (Int, Int) => Double): Double = {
      val predictions = test
        .map(x => Rating(user = x.user, item = x.item, rating = predictorFunc(train)(x.user, x.item)))
      (predictions zip test).map({ case (x, y) => abs(x.rating - y.rating) }).sum / test.count()
    }
  }


  /**
   * This class contains the functions that generate the results used only in the Personalized predictions.
   */
  class PersonalizedSolver(train: Seq[Rating], test: Seq[Rating]) extends BaselineSolver(test) {
    // Apply preprocessing operations on the train set to make the process faster
    val normalizedRatingsByItem: Map[Int, Seq[Rating]] = normalizeData(train).groupBy(x => x.item)
    val ratingsByUser: Map[Int, Seq[Rating]] = train.groupBy(x => x.user)
    val preprocessedTrain: Seq[Rating] = preprocessData(normalizeData(train))
    val preprocessedRatingsByUser: Map[Int, Seq[Rating]] = preprocessedTrain.groupBy(x => x.user)

    /**
     * Preprocess the whole dataset according to the law of multiplication to make computation faster.
     *
     * @param data The list of all ratings.
     * @return The initial dataset, with the updated ratings.
     */
    def preprocessData(data: Seq[Rating]): Seq[Rating] = {
      val norm2RatingsByUser = ratingsByUser.mapValues(x => sqrt(x.map(y => pow(y.rating, 2)).sum))
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
    val userUniformSimilarity: (Int, Int) => Double = (user1: Int, user2: Int) => 1.0

    /**
     * Get the cosine similarity score between two users.
     *
     * @param user1 The first user.
     * @param user2 The second user.
     * @return The cosine similarity.
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
      val similarity = intersection.map(x => x.head.rating * x(1).rating).sum
      similarity
    }

    /**
     * Get the Jaccard similarity score between two users.
     *
     * @param user1 The first user.
     * @param user2 The second user.
     * @return The Jaccard similarity
     */
    def userJaccardSimilarity(user1: Int, user2: Int): Double = {
      if (user1 == user2) {
        return 0
      }
      val user1Ratings = preprocessedRatingsByUser.getOrElse(user1, Seq()).map(x => x.item).toSet
      val user2Ratings = preprocessedRatingsByUser.getOrElse(user2, Seq()).map(x => x.item).toSet
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
    def getUserItemAvgDev(user: Int, item: Int, similarity: (Int, Int) => Double): Double = {
      // TODO: ask whether we should ignore the ratings given by our user
      // TODO: precompute similarity
      val relevantRatings = normalizedRatingsByItem.getOrElse(item, Seq[Rating]())
      val numerator = relevantRatings.map(x => similarity(user, x.user) * x.rating).sum
      val denominator = relevantRatings.map(x => abs(similarity(user, x.user))).sum
      if (denominator != 0) {
        return numerator / denominator
      }
      0
    }

    /**
     * Computes a prediction for an item for an user based on train data.
     *
     * @param item       The item for which the prediction will be computed.
     * @param user       The user for which the prediction will be computed.
     * @param similarity The function that will be used to measure similarity.
     * @return The predicted rating.
     */
    def personalizedPredictor(train: Seq[Rating], similarity: (Int, Int) => Double): (Int, Int) => Double = {
      val avgRatingByUser = userAvg(train)
      val avgGlobal = globalAverage(train)

      def prediction(user: Int, item: Int): Double = {
        val userItemAvgDev = getUserItemAvgDev(user = user, item = item, similarity = similarity)
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

      prediction
    }

    /**
     * Extracts predictions based on the given predictor function and returns MAE
     *
     * @param similarity The similarity function that will be used to compare users.
     * @return Mean Average Error between the predictions and the test.
     */
    override def getMAE(similarity: (Int, Int) => Double): Double = {
      val predictor = personalizedPredictor(train, similarity)
      mean(test.map(x => (predictor(x.user, x.item) - x.rating).abs))
    }
  }

  /**
   * This class contains the functions that generate the results used only in the Neighbourhood-based predictions.
   */
  class KNNSolver(train: Seq[Rating], test: Seq[Rating], k: Int) extends PersonalizedSolver(train, test) {
    // A list of unique users.
    val uniqueUsers: Seq[Int] = Seq[Int]() // 42 normalizedTrain.map(x => x.user).distinct
    // Store the similarities of the k-closest neighbours for each user.
    val KNearestUsers: Map[Int, Seq[Int]] = uniqueUsers.map(x => x -> getKNearestUsers(x)).toMap

    private def getKNearestUsers(user: Int): Seq[Int] = {
      // Compute similarities for current user with every other user
      val allUserSimilarities: Seq[(Int, Double)] = uniqueUsers
        .map(other => (other, userCosineSimilarity(user, other)))
        .filterNot { case (other, _) => other == user }

      // Get the sorted array of similarities
      val sortedUserSimilarities = Sorting
        .stableSort(allUserSimilarities, (x: (Int, Double), y: (Int, Double)) => x._2 > y._2)

      // Return the k-nearest neighbors
      val topK = sortedUserSimilarities.map(x => x._1).take(k)
      if (topK == null) {
        return Seq[Int]()
      }
      topK
    }

    def userSimilarity(user1: Int, user2: Int): Double = {
      if (user1 == user2) {
        return 0
      }
      if (!KNearestUsers(user1).contains(user2)) {
        return 0
      }
      userCosineSimilarity(user1, user2)
    }

    /**
     * Returns the user-specific weighted-sum deviation for an item, based on the k-nearest neighbors.
     *
     * @param user       The user for which the rating will be computed.
     * @param item       The item for which the rating will be computed.
     * @param similarity The function that will be used to measure similarity.
     * @return The rating.
     */
    override def getUserItemAvgDev(user: Int, item: Int, similarity: (Int, Int) => Double): Double = {
      //      val relevantUserRatings = getKNearestUsers(user)
      //        .flatMap(x => normalizedRatingsByUser(x).filter(y => y.item == item))
      //      val numerator = relevantUserRatings.map(x => userSimilarity(user, x.user) * x.rating).sum
      //      val denominator = relevantUserRatings.map(x => abs(userSimilarity(user, x.user))).sum
      //      if (denominator != 0) {
      //        return numerator / denominator
      //      } 42
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
      val predictor = personalizedPredictor(train, userCosineSimilarity)(_, _)
      val allPredictions: Seq[(Int, Double)] = notRatedMovies
        .map(x => (x, predictor(user, x))).toSeq

      // Sort predictions
      scala.util.Sorting.stableSort(allPredictions,
        (e1: (Int, Double), e2: (Int, Double)) => e1._2 > e2._2 || e1._2 == e2._2 && e1._1 < e2._1)

      // Return the top predictions
      allPredictions.take(top).toList
    }
  }
}

