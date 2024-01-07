import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.{SparkSession, Row}

object top_10_most_viewed_movie extends App{

  val spark = SparkSession.builder.config("spark.master", "local")
    .appName("top 10 most viewed movies")
    .getOrCreate()

  System.setProperty("hadoop.home.dir", "C:\\Users\\ASUS\\Desktop\\Movie_analytics\\Hadoop")


  // Import implicits for Spark encoder
  import spark.implicits._

  // Load ratings data
  val ratingsDF = spark.read.textFile("E:\\MovieAnalysis\\ratings.dat")
    .map(line => {
      val fields = line.split("::")
      (fields(1).toInt, 1)  // Assuming MovieID is at index 1
    })
    .toDF("movieId", "count")

  // Count the occurrences of each movie
  val movies_count = ratingsDF.groupBy("movieId")
    .agg(sum("count").alias("totalViews"))
    .sort(desc("totalViews"))

  // Get the top 10 most viewed movies
  val mv_top10DF = movies_count.limit(10).select("movieId", "totalViews")

  // Load movies data
  val mv_names = spark.read.textFile("E:\\MovieAnalysis\\movies.dat")
    .map(line => {
      val fields = line.split("::")
      (fields(0).toInt, fields(1))  // Assuming MovieID is at index 0
    })
    .toDF("movieId", "movieTitle")

  // Join movies_count with movie names
  val join_out = mv_names.join(mv_top10DF, Seq("movieId"))

  // Show the result
  join_out.show(truncate = false)

  //data_function.writeDataFrameToHtml(join_out, "Top 10 most Viewed Movies",data_function.outputPath)

  System.exit(0)

}
