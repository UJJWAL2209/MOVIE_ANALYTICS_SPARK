import scala.io.Source
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object Main {
  def main(args: Array[String]): Unit = {

    //Spark session for the app
    val sparkSession = SparkSession.builder.config("spark.master", "local")
      .appName("MoviesRanking")
      .getOrCreate()

    val usersDf = data_function.loadUsers(sparkSession, "E:\\MovieAnalysis\\users.dat")

    val data = data_function.loadMovies(sparkSession, "E:\\MovieAnalysis\\movies.dat")

    val ratingsDf = data_function.loadRatings(sparkSession, "E:\\MovieAnalysis\\ratings.dat")

    val rankings = data_function
      .moviesRankingByGender(data, usersDf, ratingsDf)


    //rankings.show()
    //  val conf = new SparkConf().setAppName("MoviesRanking")
    //  new SparkContext(conf)
  }
}