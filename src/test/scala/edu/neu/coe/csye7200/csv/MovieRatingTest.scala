import edu.neu.coe.csye7200.csv.MovieRating
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructType}
import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.functions._

class MovieRatingTest extends AnyFunSuite {
  val spark: SparkSession = SparkSession.builder()
    .appName("Test")
    .master("local[1]")
    .getOrCreate()

  test("calculateMovieStats should compute correct statistics for movies") {
    import spark.implicits._

    // Create test data with movie_title and imdb_score
    val testData = Seq(
      ("The Shawshank Redemption", 9.3),
      ("The Shawshank Redemption", 9.2),
      ("The Shawshank Redemption", 9.1),
      ("The Godfather", 9.2),
      ("The Godfather", 9.0),
      ("Inception", 8.8),
      ("Inception", 8.8)  // Duplicate rating
    )

    val testDF = testData.toDF("movie_title", "imdb_score")

    // Calculate stats
    val resultDF = MovieRating.calculateMovieStats(testDF)

    // Collect results
    val results = resultDF.orderBy("movie_title").collect()

    // Verify The Godfather stats
    val godfather = results(1)
    assert(godfather.getAs[String]("movie_title") == "The Godfather")
    assert(godfather.getAs[Long]("num_ratings") == 2)
    assert(math.abs(godfather.getAs[Double]("mean_rating") - 9.1) < 0.001)
    assert(math.abs(godfather.getAs[Double]("stddev_rating") - 0.1) > 0.001)

    // Verify Inception stats (identical ratings)
    val inception = results(0)
    assert(inception.getAs[String]("movie_title") == "Inception")
    assert(inception.getAs[Long]("num_ratings") == 2)
    assert(math.abs(inception.getAs[Double]("mean_rating") - 8.8) < 0.001)
    assert(inception.getAs[Double]("stddev_rating") == 0.0)

    // Verify The Shawshank Redemption stats
    val shawshank = results(2)
    assert(shawshank.getAs[String]("movie_title") == "The Shawshank Redemption")
    assert(shawshank.getAs[Long]("num_ratings") == 3)
    assert(math.abs(shawshank.getAs[Double]("mean_rating") - 9.2) < 0.001)
    assert(math.abs(shawshank.getAs[Double]("stddev_rating") - 0.0816) > 0.001)
  }

  test("calculateMovieStats should handle empty DataFrame") {
    import spark.implicits._

    // Create empty DataFrame
    val emptyDF = Seq.empty[(String, Double)].toDF("movie_title", "imdb_score")

    // Calculate stats
    val resultDF = MovieRating.calculateMovieStats(emptyDF)

    // Verify empty result
    assert(resultDF.count() == 0)
  }

  test("calculateMovieStats should filter null imdb_scores") {
    import spark.implicits._

    // Create test data with null values
    val testData = Seq(
      ("Pulp Fiction", 8.9),
      ("Pulp Fiction", null),
      ("Fight Club", null),
      ("Fight Club", null)
    )
  }

  test("calculateMovieStats should trim movie titles") {
    import spark.implicits._

    // Create test data with whitespace
    val testData = Seq(
      ("  The Dark Knight ", 9.0),
      ("The Dark Knight", 9.0),
      (" The Dark Knight", 9.0)
    )

    val testDF = testData.toDF("movie_title", "imdb_score")

    // Calculate stats
    val resultDF = MovieRating.calculateMovieStats(testDF)

    // Should be grouped as one movie
    assert(resultDF.count() == 1)
    val darkKnight = resultDF.first()
    assert(darkKnight.getAs[String]("movie_title") == "The Dark Knight")
    assert(darkKnight.getAs[Long]("num_ratings") == 3)
  }
}