import movieprepare.spark
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import top_10_most_viewed_movie.join_out

object distinct_generes extends App{

  val spark = SparkSession.builder.config("spark.master", "local")
    .appName("distinct generes")
    .getOrCreate()

  System.setProperty("hadoop.home.dir", "C:\\Users\\ASUS\\Desktop\\Movie_analytics\\Hadoop")

  import spark.implicits._

  val movies_rdd=spark.read.textFile("E:\\MovieAnalysis\\movies.dat")
  val genres=movies_rdd.map(lines=>lines.split("::")(2))
  val testing=genres.flatMap(line=>line.split('|'))
  val  genres_distinct_sorted=testing.distinct()
  genres_distinct_sorted.foreach(println(_))
  //val html_file = "E:\\MovieAnalysis\\different_genres.html"
  //data_function.writeDataFrameToHtml( selectedGenresDF, "Distinct genres are:",html_file)
  System.exit(0)
}