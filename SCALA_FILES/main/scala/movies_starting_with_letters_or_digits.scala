import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.TypedColumn
import top_10_most_viewed_movie.join_out

object movies_starting_with_letters_or_digits extends App{

  val spark = SparkSession.builder.config("spark.master", "local")
    .appName("movies start with letters or digits")
    .getOrCreate()

  System.setProperty("hadoop.home.dir", "C:\\Users\\ASUS\\Desktop\\Movie_analytics\\Hadoop")

  // Import implicits for Spark encoder
  import spark.implicits._

  val movies_rdd = spark.read.textFile("E:\\MovieAnalysis\\movies.dat")
  val movies = movies_rdd.map(lines => lines.split("::")(1))
  val string_flat = movies
    .flatMap(lines => lines.split(" ")(0).toCharArray.map(_.toString)) // Convert Char to String

  // check for the first character for a letter then find the count
  val movies_letter = string_flat.filter(word => Character.isLetter(word.head)).map(word => (word.head.toString, 1))

  // Perform the aggregation
  val movies_letter_count = movies_letter
    .groupByKey(_._1)
    .agg(sum("_2").as[Long]) // Automatically inferred type, no need for TypedColumn
    .sort("key")

  // Show the result
  println("Movies starting with letters:")
  movies_letter_count.show()
  // Convert Dataset[(String, Long)] to DataFrame
  val moviesLetterCountDF = movies_letter_count.toDF("letter", "count")
//val html_file = "E:\\MovieAnalysis\\file.html"
 //data_function.writeDataFrameToHtml(moviesLetterCountDF, "Count of movies starting with each letters",data_function.outputPath)


  System.exit(0)
}
