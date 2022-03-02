package shared

package object predictions
{
  case class Rating(user: Int, item: Int, rating: Double)

  def timingInMs(f : ()=>Double ) : (Double, Double) = {
    val start = System.nanoTime() 
    val output = f()
    val end = System.nanoTime()
    return (output, (end-start) / 1000000.0)
  }

  def mean(s :Seq[Double]): Double =  if (s.size > 0) s.reduce(_+_) / s.length else 0.0
  def std(s :Seq[Double]): Double = {
    if (s.size == 0) 0.0
    else {
      val m = mean(s)
      scala.math.sqrt(s.map(x => scala.math.pow(m-x, 2)).sum / s.length.toDouble)
    }
  }

  def toInt(s: String): Option[Int] = {
    try {
      Some(s.toInt)
    } catch {
      case e: Exception => None
    }
  }

  def load(spark : org.apache.spark.sql.SparkSession,  path : String, sep : String) : org.apache.spark.rdd.RDD[Rating] = {
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
                   case None => false })
         .map({ case Some(x) => x 
                case None => Rating(-1, -1, -1)})
  }

  /**
  * Function 'scale' which allows for computation of normalized deviation as defined by Equation 2
  * @param currRating The rating for item i, given by user u
  * @param avgRating The average rating, given by user u
  * @return A scaled rating 
  */
  def scaleUserRating(currRating: Double, avgRating: Double) : Double = {
    if (currRating > avgRating) 5 - avgRating
    else if (currRating < avgRating) avgRating - 1
    else 1
  }

  def stdScale(s :Seq[Double]): Double = {
    if (s.size == 0) 0.0
    else {
      val m = mean(s)
      val r_hat = s.map(x => (m-x) / scaleUserRating(m, x))
      r_hat.sum / s.length.toDouble
    }
  }

  /**
   *
   * @param row
   * @param avgRating
   * @return
   */
  def normalizeRating(row: Rating, avgRating: Double): Rating = {
    val normalized_rating = (row.rating - avgRating) / scaleUserRating(row.rating, avgRating)
    Rating(row.user, row.item, normalized_rating)
  }

  /**
   * Function that normalizes the ratings of a dataset.
   * @param data The input dataset, with non-normalized ratings.
   * @return
   */
  def preprocess(data: Array[Rating]) : Array[Rating] = {
    val normalizedData = data.clone
    // Group all ratings by user and compute the average rating for each user
    val avgRatingByUser = normalizedData.groupBy(x => x.user).map {
      case(k, v) => (k, mean(v.map(x => x.rating)))
    }

    // Normalize each rating
    normalizedData.map(x => normalizeRating(x, avgRatingByUser(x.user)))

    (avgRatingByUser, normalizedData)
  }
}
