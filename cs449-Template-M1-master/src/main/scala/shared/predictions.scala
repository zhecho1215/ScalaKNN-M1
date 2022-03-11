package shared

import org.apache.spark.rdd.RDD

import scala.collection.mutable
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

  /**
   * Function that normalizes the ratings of a dataset.
   *
   * @param data The input dataset, with non-normalized ratings.
   * @return The normalized ratings.
   */
  def normalizeData(data: Seq[Rating]): Seq[Rating] = {
    // Group all ratings by user and compute the average rating for each user
    val avgRatingByUser = data.groupBy(x => x.user).map {
      case (k, v) => (k, mean(v.map(x => x.rating)))
    }

    // Normalize each rating
    val normalizedData = data.map(x => normalizeRating(x, avgRatingByUser(x.user)))
    normalizedData
  }

  /**
   * Function that normalizes a single rating.
   *
   * @param row       The input rating, a row in the dataset.
   * @param avgRating The average rating of the user.
   * @return The normalized rating.
   */
  def normalizeRating(row: Rating, avgRating: Double): Rating = {
    val normalized_rating = (row.rating - avgRating) / scaleUserRating(row.rating, avgRating)
    Rating(row.user, row.item, normalized_rating)
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

  case class Rating(user: Int, item: Int, rating: Double)

  /**
   * This class contains the functions that generate the results used only in the Baseline predictions.
   */
  class BaselineSolver(train: Seq[Rating], test: Seq[Rating]) {

    val globalAverage: Double = mean(train.map(x => x.rating))

    // Apply preprocessing operations on the train and test
    val normalizedTrain: Seq[Rating] = normalizeData(train)

    /**
     *
     * @param train Training set of Ratings
     * @param item  The ID of the item
     * @return The average deviation
     */
    def getItemAvgDev(train: Seq[Rating], item: Int): Double = {
      println(train.count(x => x.item == item))
      mean(normalizedTrain.filter(x => x.item == item).map(x => x.rating))
    }

    /**
     * Predictor function with given signature which always returns the global average
     *
     * @param train Training data
     * @return Global average of train data
     */
    def getGlobalAvg(train: Seq[Rating]): (Int, Int) => Double = {
      def ratingPrediction(user: Int, item: Int): Double = {
        globalAverage
      }

      ratingPrediction
    }

    /**
     * Get the average score per item.
     *
     * @param train Training data
     * @return Item average in training data
     */
    def getItemAvg(train: Seq[Rating]): (Int, Int) => Double = {
      (user: Int, item: Int) =>
        if (!train.exists(x => x.item == item)) globalAverage
        else mean(train.filter(x => x.item == item).map(x => x.rating))
    }

    /**
     * Get the average score per user.
     *
     * @param train Training data.
     * @return The average score per user.
     */
    val getUserAvg: Seq[Rating] => (Int, Int) => Double = (train: Seq[Rating]) => (user: Int, item: Int) => mean(
      train.filter(x => x.user == user).map(x => x.rating))

    /**
     * Method to compute a prediction according to Equation (5)
     *
     * @param item ID of item
     * @param user ID of user
     * @return A prediction for an item for an user based on train data.
     */
    def getPredUserItem(item: Int, user: Int): Double = {
      val userAvg = getUserAvg(train)(user, 0)
      val itemAvgDev = getItemAvgDev(train, item)

      if (itemAvgDev == 0 || !train.exists(x => x.item == item)) {
        // No rating for i in the training set of the item average dev is 0
        if (!train.exists(x => x.user == user)) {
          // The user has no rating
          return getGlobalAvg(train)(0, 0)
        }
        return userAvg
      }

      userAvg + itemAvgDev * scaleUserRating(userAvg + itemAvgDev, userAvg)
    }

    /**
     * Returns the baseline prediction for a user and an item.
     *
     * @param train Training data.
     * @return A predicted score using the baseline approach (Equation 5).
     */
    def getBaseline(train: Seq[Rating]): (Int, Int) => Double = {
      (user: Int, item: Int) => getPredUserItem(item = item, user = user)
    }


    /**
     * Extracts predictions based on the given predictor function and returns MAE
     *
     * @param predictorFunc Estimates the prediction for given item and user.
     * @return Mean Average Error between the predictions and the test.
     */
    def getMAE(predictorFunc: Seq[Rating] => (Int, Int) => Double): Double = {
      test.map(x => abs(predictorFunc(train)(x.user, x.item) - x.rating)).sum / test.length
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

      userAvg + itemAvgDev * scaleUserRating(userAvg + itemAvgDev, userAvg)
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
  class PersonalizedSolver(train: Array[Rating], test: Array[Rating]) extends BaselineSolver(train, test) {
    // Apply preprocessing operations on the train set to make the process faster
    val preprocessedTrain: Seq[Rating] = preprocessData(normalizedTrain)
    val ratingsByUser: Map[Int, Seq[Rating]] = preprocessedTrain.groupBy(x => x.user)
    // Store the user cosine similarities for a faster run
    var userCosineSimilarities: mutable.Map[Int, mutable.Map[Int, Double]] = mutable
      .Map[Int, mutable.Map[Int, Double]]().withDefaultValue(mutable.Map[Int, Double]().withDefaultValue(0.0))

    /**
     * Preprocess a rating based on the law of multiplication.
     *
     * @param currentRating The initial normalized rating.
     * @param data          The list of all ratings.
     * @return The normalized rating.
     */
    private def preprocessRating(currentRating: Rating, data: Seq[Rating]): Rating = {
      val numerator = currentRating.rating
      val denominator = sqrt(data.filter(x => x.user == currentRating.user).map(x => pow(x.rating, 2)).sum)
      var fraction: Double = 0.0
      if (denominator != 0) {
        fraction = numerator / denominator
      }
      Rating(user = currentRating.user, item = currentRating.item, rating = fraction)
    }

    /**
     * Preprocess the whole dataset according to the law of multiplication to make computation faster.
     *
     * @param data The list of all ratings.
     * @return The initial dataset, with the updated ratings.
     */
    def preprocessData(data: Seq[Rating]): Seq[Rating] = {
      data.map(x => preprocessRating(x, data))
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
      // Check if user similarity was already computed
      if (userCosineSimilarities.contains(user1) && userCosineSimilarities(user1).contains(user2)) {
        return userCosineSimilarities(user1)(user2)
      }

      // User similarity was not computed yet
      val user1Ratings = ratingsByUser.getOrElse(user1, Seq())
      val user2Ratings = ratingsByUser.getOrElse(user2, Seq())
      // Combine user ratings, group them by item and only select groups that have exactly 2 members
      val intersection = (user1Ratings ++ user2Ratings).groupBy(x => x.item)
                                                       .filter { case (_, v) => v.length == 2 }
                                                       .map { case (_, v) => v }
      val similarity = intersection.map(x => x.head.rating * x(1).rating).sum
      // Store similarity and return it
      userCosineSimilarities(user1)(user2) = similarity
      userCosineSimilarities(user2)(user1) = similarity
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
      val user1Ratings = ratingsByUser.getOrElse(user1, Seq()).map(x => x.item).toSet
      val user2Ratings = ratingsByUser.getOrElse(user2, Seq()).map(x => x.item).toSet
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
      println(train.count(x => x.item == item))
      val relevantUserRatings = normalizedTrain.filter(x => x.item == item)
      val numerator = relevantUserRatings.map(x => similarity(user, x.user) * x.rating).sum
      val denominator = relevantUserRatings.map(x => abs(similarity(user, x.user))).sum
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
    def getPredUserItem(item: Int, user: Int, similarity: (Int, Int) => Double): Double = {
      val userAvg = getUserAvg(train)(user, 0)
      val userItemAvgDev = getUserItemAvgDev(item, user, similarity)

      if (userItemAvgDev == 0 || !train.exists(x => x.item == item)) {
        // No rating for i in the training set of the item average dev is 0
        if (!train.exists(x => x.user == user)) {
          // The user has no rating
          return getGlobalAvg(train)(0, 0)
        }
        return userAvg
      }

      userAvg + userItemAvgDev * scaleUserRating(userAvg + userItemAvgDev, userAvg)
    }

    /**
     * Extracts predictions based on the given predictor function and returns MAE
     *
     * @param similarity The similarity function that will be used to compare users.
     * @return Mean Average Error between the predictions and the test.
     */
    def getMAE(similarity: (Int, Int) => Double): Double = {
      test.map(x => abs(getPredUserItem(item = x.item, user = x.user, similarity) - x.rating)).sum / test.length
    }
  }

  /**
   * This class contains the functions that generate the results used only in the Neighbourhood-based predictions.
   */
  class KNNSolver(train: Array[Rating], test: Array[Rating], k: Int) extends PersonalizedSolver(train, test) {
    // Store the similarities of the k-closest neighbours for each user.
    val KNearestUsers: mutable.Map[Int, Seq[Int]] = mutable.Map[Int, Seq[Int]]().withDefaultValue(Seq[Int]())
    // A list of unique users.
    val uniqueUsers: Seq[Int] = normalizedTrain.map(x => x.user).distinct

    /**
     * Returns the user-specific weighted-sum deviation for an item, based on the k-nearest neighbors.
     *
     * @param user       The user for which the rating will be computed.
     * @param item       The item for which the rating will be computed.
     * @param similarity The function that will be used to measure similarity.
     * @return The rating.
     */
    override def getUserItemAvgDev(user: Int, item: Int, similarity: (Int, Int) => Double): Double = {
      // Check if the KNNearest users have already been found for the current user
      if (!KNearestUsers.contains(user)) {
        // Compute similarities for current user with every other user
        val allUserSimilarities: Seq[(Int, Double)] = uniqueUsers
          .map(other => (other, userCosineSimilarity(user, other)))
          .filter { case (other, _) => other != user }

        // Get the sorted array of similarities
        Sorting.stableSort(allUserSimilarities, (x: (Int, Double), y: (Int, Double)) => x._2 > y._2)

        // Store the k-nearest neighbors (only the user id, the similarity is already stored in userCosineSimilarities
        KNearestUsers(user) = allUserSimilarities.map(x => x._1).take(k)
      }

      val relevantUserRatings = KNearestUsers(user).flatMap(other => ratingsByUser(other).filter(x => x.item == item))
      val numerator = relevantUserRatings.map(x => similarity(user, x.user) * x.rating).sum
      val denominator = relevantUserRatings.map(x => abs(similarity(user, x.user))).sum
      if (denominator != 0) {
        return numerator / denominator
      }
      0
    }
  }

  /**
   * This class contains the functions that generate the results used only in the Recommender predictions.
   */
  class RecommenderSolver(train: Array[Rating], movieNames: Map[Int, String], k: Int)
    extends KNNSolver(train, Array(), k) {
    /**
     * Compare two movies to sort them descending by score and ascending by identifier on equality.
     *
     * @param movie1 The first movie.
     * @param movie2 The second movie.
     * @return Whether the first movie should be placed before the second one.
     */
    private def movieComparator(movie1: (Int, Double), movie2: (Int, Double)): Boolean = {
      if (movie1._2 > movie2._2) {
        // Movies with higher scores should be first
        return true
      }
      if (movie1._2 == movie2._2 && movie1._1 < movie2._1) {
        // Movies with equal predictions should be sorted ascending by identifier
        return true
      }
      false
    }

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
      val ratedMovies = ratingsByUser(user).map(x => x.item).toSet
      val notRatedMovies = allMovies.diff(ratedMovies)

      // Compute predicted score for movies that were not rated
      val allPredictions: Array[(Int, Double)] = notRatedMovies
        .map(x => (x, getPredUserItem(item = x, user = user, userCosineSimilarity))).toArray

      // Sort predictions
      Sorting.stableSort(allPredictions, (x, y) => movieComparator(x, y))

      // Return the top predictions
      allPredictions.take(top).toList
    }
  }
}

