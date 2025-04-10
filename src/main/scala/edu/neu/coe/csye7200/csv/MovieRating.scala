package edu.neu.coe.csye7200.csv
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object MovieRating {
  def main(args: Array[String]): Unit = {
    // Initialize Spark session
    val spark = SparkSession.builder()
      .appName("MovieRatingsAnalyzer")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    try {
      val csvPath = "/Users/parv90/Desktop/Big Data/Assignments/Ass-03 (Movie database part 2)/CSYE7200/spark-csv/src/main/resources/movie_metadata.csv"
      println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ " + csvPath)

      // Read the CSV file
      val ratingsDF = spark.read
        .option("header", "true") // First line is header
        .option("inferSchema", "true") // Infer data types
        .csv(csvPath)

      ratingsDF.printSchema()

      val resultDF = calculateMovieStats(ratingsDF)

      resultDF.show(20, truncate = false)

      println("Analysis completed successfully!")
    } finally {
      spark.stop()
    }
  }

  def calculateMovieStats(ratingsDF: DataFrame): DataFrame = {
    ratingsDF
      // Clean and prepare data
      .withColumn("movie_title", trim(col("movie_title")))
      .withColumn("imdb_score", col("imdb_score").cast("double"))
      .filter(col("imdb_score").isNotNull)
      .groupBy("movie_title")
      .agg(
        count("imdb_score").alias("num_ratings"),
        mean("imdb_score").alias("mean_rating"),
        when(count("imdb_score") > 1, stddev("imdb_score")).otherwise(0.0).alias("stddev_rating")
      )
      .filter(col("num_ratings") >= 1)
  }
}