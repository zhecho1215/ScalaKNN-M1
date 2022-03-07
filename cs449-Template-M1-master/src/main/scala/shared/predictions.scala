package shared

import scala.math.abs

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
  def normalizeRatings(data: Array[Rating]): Array[Rating] = {
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
   * This class contains the functions that generate the results used only in section B.
   */
  class BSolvers(train: Array[Rating], test: Array[Rating]) {
    // Apply preprocessing operations on the train set
    val normalizedTrain = normalizeRatings(train)

    def getGlobalAvg: Double = {
      mean(train.map(x => x.rating))
    }

    def getItemAvg(item: Int): Double = {
      if (!train.exists(x => x.item == item)) getGlobalAvg
      else mean(train.filter(x => x.item == item).map(x => x.rating))
    }

    def getUserAvg(user: Int): Double = {
      mean(train.filter(x => x.user == user).map(x => x.rating))
    }

    def getItemAvgDev(item: Int): Double = {
      mean(normalizedTrain.filter(x => x.item == item).map(x => x.rating))
    }

    /**
     * Computes a prediction for an item for an user based on train data.
     */
    def getPredUserItem(item: Int, user: Int): Double = {
      val userAvg = getUserAvg(user)
      val itemAvgDev = getItemAvgDev(item)

      if (itemAvgDev == 0 || !train.exists(x => x.item == item)) {
        // No rating for i in the training set of the item average dev is 0
        if (!train.exists(x => x.user == user)) {
          // The user has not rating
          return getGlobalAvg
        }
        return userAvg
      }

      userAvg + itemAvgDev * scaleUserRating(userAvg + itemAvgDev, userAvg)
    }

    /**
     * Returns all of the predicted scores on the test set.
     */
    def getBaselinePredictions: Array[Rating] = {

      test.map(x => Rating(user = x.user, item = x.item, rating = getPredUserItem(item = x.item, user = x.user)))
    }

    /**
     * Returns array of Ratings as large as the test set filled with the global average value
     */
    def getGlobalPredictions: Array[Rating] = {

      test.map(x => Rating(user = x.user, item = x.item, rating = getGlobalAvg))
    }

    /**
     * Returns array of Ratings as large as the test set filled with the average rating of the given user
     */
    def getUserAvgPredictions: Array[Rating] = {

      test.map(x => Rating(user = x.user, item = x.item, rating = getUserAvg(x.user)))
    }

    /**
     * Returns array of Ratings as large as the test set filled with the average rating of the given movie
     */
    def getItemAvgPredictions: Array[Rating] = {
      test.map(x => Rating(user = x.user, item = x.item, rating = getItemAvg(x.item)))
    }

    /**
     * Mean Average Error between the predictions and the trueRatings.
     */
    def getMAE(predictions: Array[Rating], trueRatings: Array[Rating]): Double = {
      (predictions zip trueRatings).map({ case (x, y) => abs(x.rating - y.rating) }).sum / trueRatings.length
    }
  }

  /**
   * This class contains the functions that generate the results used only in section P.
   */
  class PSolvers(train: Array[Rating], test: Array[Rating]) {
    // Apply preprocessing operations on the train set
    val preprocessedTrain = normalizeRatings(train)
  }
}
