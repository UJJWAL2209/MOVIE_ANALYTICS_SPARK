import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object list_oldest_movies extends App {

  val spark = SparkSession.builder
    .config("spark.master", "local")
    .appName("Oldest Movies")
    .getOrCreate()

  val moviesDF = spark.read
    .option("delimiter", "::")
    .csv("E:\\MovieAnalysis\\movies.dat")
    .toDF("movieid", "moviename", "genre")

  val oldestMoviesDF = moviesDF
    .select(
      col("movieid"),
      col("moviename"),
      col("genre"),
      expr("substring(moviename, length(moviename) - 3, 4)").alias("year")
    )
    .orderBy("year")

  oldestMoviesDF.show()

  // Optionally, you can save the result to a CSV file or any other desired format.
  // oldestMoviesDF.write.csv("E:\\MovieAnalysis\\oldest_movies_output")

  spark.stop()
}
