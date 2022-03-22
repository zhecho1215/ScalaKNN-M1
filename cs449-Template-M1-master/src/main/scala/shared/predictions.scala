package shared
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
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
   * This class contains the functions that generate the results used in the Baseline predictions.
   */
  class BaselineSolver(train: Seq[Rating], test: Seq[Rating]) {
    // The train ratings grouped by user
    lazy val ratingsByUser: Map[Int, Seq[Rating]] = train.view.groupBy(x => x.user)
    // The train ratings grouped by item
    lazy val ratingsByItem: Map[Int, Seq[Rating]] = train.view.groupBy(x => x.item)

    // A function that returns the global average rating given a train set
    val globalAvg: Seq[Rating] => Double = (train: Seq[Rating]) => {
      var sumRating = 0.0
      for (rating <- train) {
        sumRating += rating.rating
      }
      sumRating / train.length
    }

    // A function that returns the average rating by user given a train set
    val userAvg: Seq[Rating] => Map[Int, Double] = (train: Seq[Rating]) => train.view.groupBy(x => x.user).map {
      case (k, v) => (k, mean(v.map(x => x.rating)))
    }
    // The computed average rating per user is stored in a map
    val avgRatingByUser: mutable.Map[Int, Double] = mutable.Map[Int, Double]()

    // A function that returns the average rating by item given a train set
    val itemAvg: Seq[Rating] => Map[Int, Double] = (train: Seq[Rating]) => train.view.groupBy(x => x.item).map {
      case (k, v) => (k, mean(v.map(x => x.rating)))
    }

    /**
     * Returns the average rating of a user.
     * @param user The user for which the average will be computed
     * @return The average rating
     */
    def getAvgRatingByUser(user: Int): Double = {
      // Check if user has any ratings
      if (!ratingsByUser.contains(user) || ratingsByUser(user).isEmpty) {
        return 0.0
      }

      // Check if the average rating was already computed
      if (avgRatingByUser.contains(user)) {
        return avgRatingByUser(user)
      }

      // Compute average rating
      var avgRating = 0.0
      for (rating <- ratingsByUser(user)) {
        avgRating += rating.rating
      }
      avgRating = avgRating / ratingsByUser(user).size

      // Store average rating
      avgRatingByUser(user) = avgRating
      avgRating
    }

    /**
     * Normalize a single entry from the dataset.
     * @param rating The rating that will be normalized
     * @return The normalized rating
     */
    def normalizeRating(rating: Rating): Double = {
      (rating.rating - getAvgRatingByUser(rating.user)) / scaleUserRating(rating.rating, getAvgRatingByUser(rating.user))
    }

    /**
     * Function that normalizes all of the ratings of a dataset.
     *
     * @param train           The dataset that will be normalized
     * @param avgRatingByUser The average rating by user
     * @return The normalized dataset
     */
    def normalizeData(train: Seq[Rating], avgRatingByUser: Map[Int, Double]): Seq[Rating] = {
      train.map(x => Rating(x.user, x.item, normalizeRating(x)))
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
     * Predictor function with given signature which returns the average score per user
     *
     * @param train Training data
     * @return The average score per user
     */
    def userAvgPredictor(train: Seq[Rating]): (Int, Int) => Double = {
      val avgRatingByUser = userAvg(train)
      (user: Int, item: Int) => avgRatingByUser(user)
    }

    /**
     * Predictor function with given signature which returns the average score per item
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
     * Get the average rating deviation per item.
     *
     * @param train The training dataset
     * @return The average rating deviation
     */
    def itemAvgDev(train: Seq[Rating]): Int => Double = {
      val avgRatingByUser = userAvg(train)
      val avgRatingDevByItem = itemAvg(normalizeData(train, avgRatingByUser))
      (item: Int) => {
        avgRatingDevByItem(item)
      }
    }

    /**
     * Computes a prediction according to Equation 5.
     *
     * @param train : The training dataset
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
      var sumMAE:Double = 0.0
      var lenMAE:Double = 0.0
      for (rating <- test) {
        sumMAE += (predictorFunc(rating.user, rating.item) - rating.rating).abs
        lenMAE += 1
      }
      sumMAE / lenMAE
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

    def baselineRDDPredictor(train: RDD[Rating]): (Int, Int) => Double = {
      //create 2 Maps
      val itemDevAverage = itemAvg(normalizeData(train, userAvg(train)))
      val userAverage = userAvg(train)
      val globalAverage = globalAvg(train)
      def prediction(user: Int, item: Int): Double = {
        if (!(itemDevAverage contains item) || itemDevAverage(item) == 0) {
          // No rating for i in the training set of the item average dev is 0
          if (!(userAverage contains user)) {
            // The user has no rating
            return globalAverage
          }
          return userAverage(user)
        }
        //If the user doesn't exist we take the the global average as a user average
        val userAvg = if (!(userAverage contains user)) globalAverage else userAverage(user)
        val itemAvgDev = itemDevAverage(item)
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
   * An enum for similarity functions.
   */
  sealed trait SimilarityFunctions

  case object Uniform extends SimilarityFunctions

  case object Cosine extends SimilarityFunctions

  case object Jaccard extends SimilarityFunctions

  /**
   * This class contains the functions that generate the results used only in the Personalized predictions.
   */
  class PersonalizedSolver(train: Seq[Rating], test: Seq[Rating], similarityMeasure: SimilarityFunctions)
    extends BaselineSolver(train, test) {
    // The average global rating
    lazy val avgGlobal: Double = globalAvg(train)
    // The normalized ratings by user
    var normalizedRatingsByUser: mutable.Map[Int, mutable.Map[Int, Double]] = mutable.Map[Int, mutable.Map[Int, Double]]()
    // The normalized ratings by item
    var normalizedRatingsByItem: mutable.Map[Int, Seq[Rating]] = mutable.Map[Int, Seq[Rating]]()
    // The preprocessed train set
    var preprocessedRatingsByUser: mutable.Map[Int, Seq[Rating]] = mutable.Map[Int, Seq[Rating]]()
    // The list of unique user IDs
    lazy val uniqueUsers: Set[Int] = ratingsByUser.keySet
    // The list of unique items
    lazy val uniqueItems: Set[Int] = ratingsByItem.keySet
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
    // Stores the norm 2 of user rating (as used in eq. 9)
    var norm2RatingsByUser: mutable.Map[Int, Double] = mutable.Map[Int, Double]()

    /**
     * Returns all of the normalized ratings of a user.
     * @param user The user for which the normalized ratings will be computed
     * @return The normalized ratings
     */
    def getNormalizedRatingsByUser(user: Int): mutable.Map[Int, Double] = {
      if (!normalizedRatingsByUser.contains(user)) {
        if (!ratingsByUser.contains(user)) {
          return mutable.Map[Int, Double]()
        }
        normalizedRatingsByUser(user) = mutable.Map[Int, Double]()
        for (rating <- ratingsByUser(user)) {
          normalizedRatingsByUser(user)(rating.item) = normalizeRating(rating)
        }
      }
      normalizedRatingsByUser(user)
    }

    /**
     * Retuns all of the normalized ratings of an item
     * @param item The item for which the ratings will be computed
     * @return The normalized ratings of an item
     */
    def getNormalizedRatingsByItem(item: Int): Seq[Rating] = {
      if (!normalizedRatingsByItem.contains(item)) {
        val normalizedRatings: ArrayBuffer[Rating] = ArrayBuffer()
        if (!ratingsByItem.contains(item)) {
          return Seq[Rating]()
        }
        for (rating <- ratingsByItem(item)) {
          normalizedRatings += Rating(rating.user, rating.item, normalizeRating(rating))
        }
        normalizedRatingsByItem(item) = normalizedRatings
      }
      normalizedRatingsByItem(item)
    }

    /**
     * Computes the norm-2 of the ratings of an user.
     * @param user The user for which the norm-2 will be computed
     * @return The norm-2 of the user's ratings
     */
    def getNorm2RatingsByUser(user: Int): Double = {
      if (norm2RatingsByUser.contains(user)) {
        return norm2RatingsByUser(user)
      }
      var norm2 = 0.0
      for (rating <- getNormalizedRatingsByUser(user).values) {
        norm2 += pow(rating, 2)
      }
      norm2 = sqrt(norm2)
      // Store computed norm2
      norm2RatingsByUser(user) = norm2
      norm2
    }

    /**
     * Preprocesses a rating according to eq. 9.
     * @param user The user that made the rating
     * @param rating The rating that was given
     * @return The preprocessed rating
     */
    def getPreprocessedRating(user: Int, rating: Double): Double = {
      val norm2 = getNorm2RatingsByUser(user)
      if (norm2 == 0) {
        return 0.0
      }
      rating / norm2
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
      val user1Ratings = getNormalizedRatingsByUser(user1)
      val user2Ratings = getNormalizedRatingsByUser(user2)
      var similarity = 0.0
      if (user1Ratings.size > user2Ratings.size) {
        for (a <- user2Ratings.keys) {
          if (user1Ratings.contains(a)) {
            similarity += getPreprocessedRating(user1, user1Ratings(a)) * getPreprocessedRating(user2, user2Ratings(a))
          }
        }
      } else {
        for (a <- user1Ratings.keys) {
          if (user2Ratings.contains(a)) {
            similarity += getPreprocessedRating(user1, user1Ratings(a)) * getPreprocessedRating(user2, user2Ratings(a))
          }
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
      val user1Ratings = getNormalizedRatingsByUser(user1).keys.toSet
      val user2Ratings = getNormalizedRatingsByUser(user2).keys.toSet
      val itemIntersection = user1Ratings.intersect(user2Ratings).size
      val itemUnion = user1Ratings.union(user2Ratings).size
      if (itemUnion == 0) {
        return 0
      }
      itemIntersection * 1.0 / itemUnion
    }

    /**
     * Computes the similarity between two users.
     * @param user1 The first user
     * @param user2 The second user
     * @return The similarity according to the metric the object was instantiated with
     */
    def getSimilarity(user1: Int, user2: Int): Double = {
      var similarity = 0.0
      if (!similarities.contains(user1)) {
        similarities(user1) = mutable.Map[Int, Double]()
      }
      if (similarities(user1).contains(user2)) {
        // Similarity was already computed
        similarity = similarities(user1)(user2)
      } else {
        similarity = similarityFunc(user1, user2)
        // Store similarity
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
      var numerator = 0.0
      var denominator = 0.0
      val relevantRatings = getNormalizedRatingsByItem(item)
      for (rating <- relevantRatings) {
        if (user != rating.user) {
          val similarity = getSimilarity(user, rating.user)
          numerator = numerator + similarity * rating.rating
          denominator = denominator + abs(similarity)
        }
      }
      if (denominator != 0) {
        return numerator / denominator
      }
      0
    }


    /**
     * Generates a predicted rating for a user and an item.
     * @param user The user for which the prediction will be generated
     * @param item The item for which the prediction will be generated
     * @return The predicted rating
     */
    def personalizedPredictor(user: Int, item: Int): Double = {
      val userItemAvgDev = getUserItemAvgDev(user = user, item = item)
      if (!ratingsByItem.contains(item) || userItemAvgDev == 0) {
        // No rating for i in the training set of the item average dev is 0
        if (!ratingsByUser.contains(user)) {
          // The user has no rating
          return avgGlobal
        }
        return getAvgRatingByUser(user)
      }
      val userAvg = getAvgRatingByUser(user)
      userAvg + userItemAvgDev * scaleUserRating(userAvg + userItemAvgDev, userAvg)
    }

    /**
     * Extracts predictions based on the given predictor function and returns MAE
     *
     * @return Mean Average Error between the predictions and the test.
     */
    def getMAE: Double = {
      getMAE(personalizedPredictor)
    }
  }

  /**
   * This class contains the functions that generate the results used only in the Neighbourhood-based predictions.
   */
  class KNNSolver(train: Seq[Rating], test: Seq[Rating], k: Int) extends PersonalizedSolver(train, test, Cosine) {
    // Stores the K-nearest neighbors of a user
    var kNearestUsers: mutable.Map[Int, mutable.Map[Int, Double]] = mutable.Map[Int, mutable.Map[Int, Double]]()

    /**
     * Computes the similarity between 2 users as defined in KNN.
     *
     * @param user1 The first user
     * @param user2 The second user
     * @return The similarity between 2 users
     */
    def kNearestSimilarity(user1: Int, user2: Int): Double = {
      // A user with themself has a similarity of 0
      if (user1 == user2) {
        return 0
      }

      // Check if the K-nearest neighbors of the users were computed
      if (!kNearestUsers.contains(user1)) {
        getKNearestUsers(user1)
      }

      // A user that is not among the nearest neighbors will have a similarity of 0
      if (!kNearestUsers(user1).contains(user2)) {
        return 0
      }

      kNearestUsers(user1)(user2)
    }

    /**
     * Get the k-nearest neighbors by similarity.
     *
     * @param user The user for which the k-nearest neighbours will be computed
     * @return The k-nearest neighbours
     */
    private def getKNearestUsers(user: Int): mutable.Map[Int, Double] = {

      if (kNearestUsers.contains(user)) {
        return kNearestUsers(user)
      }

      // Sort users by similarity, descending
      var userSimilarities: Array[(Double, Int)] = Array[(Double, Int)]()
      for (otherUser <- uniqueUsers) {
        userSimilarities = userSimilarities :+ (-getSimilarity(user, otherUser), otherUser)
      }
      scala.util.Sorting.quickSort(userSimilarities)

      // Return the k-nearest neighbors
      kNearestUsers(user) = mutable.Map[Int, Double]()
      var cnt = 0
      for (userSimilarity <- userSimilarities) {
        if (cnt == k) {
          return kNearestUsers(user)
        }
        if (userSimilarity._2 != user) {
          // Avoid including self in the list of neighbors
          kNearestUsers(user)(userSimilarity._2) = -userSimilarity._1
          cnt += 1
        }
      }
      kNearestUsers(user)
    }


    /**
     * Returns the user-specific weighted-sum deviation for an item, based on the k-nearest neighbors.
     *
     * @param user       The user for which the deviation will be computed
     * @param item       The item for which the deviation will be computed
     * @return The deviation
     */
    override def getUserItemAvgDev(user: Int, item: Int): Double = {
      var numerator = 0.0
      var denominator = 0.0
      if (!ratingsByItem.contains(item)) {
        return 0.0
      }
      val normalizedRatings = getNormalizedRatingsByItem(item)
      for (rating <- normalizedRatings) {
        val similarity = kNearestSimilarity(user, rating.user)
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