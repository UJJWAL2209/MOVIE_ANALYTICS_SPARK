import distinct_generes.genres_distinct_sorted
import movieprepare.spark
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoder, Encoders}
import top_10_most_viewed_movie.join_out


object latest_movies extends App{
  val spark = SparkSession.builder.config("spark.master", "local")
    .appName("latest movies")
    .getOrCreate()

  System.setProperty("hadoop.home.dir", "C:\\Users\\ASUS\\Desktop\\Movie_analytics\\Hadoop")

  // Assuming 'sc' is your SparkContext
  val movies_rdd = spark.read.textFile("E:\\MovieAnalysis\\movies.dat")

  // Import implicits for Spark encoder
  import spark.implicits._

  val movie_nm = movies_rdd.map(lines => lines.split("::")(1))
  val year = movie_nm.map(lines => lines.substring(lines.lastIndexOf("(") + 1, lines.lastIndexOf(")")))
  import org.apache.spark.sql.functions._
  val latest = year.agg(max($"value")).collect()(0)(0).toString

  // Create a DataFrame with a single row containing the latest movie title
  val latestDF = Seq((latest)).toDF("latest")
  //val html_file = "E:\\MovieAnalysis\\latest_movie_release_output.html"
  //data_function.writeDataFrameToHtml(latestDF, "Latest Movie Released is:",html_file)

  println("latest release year is "+latest)
  System.exit(0)

}