import org.apache.spark.sql.SparkSession


object movies_in_each_genre extends App{

  val spark = SparkSession.builder.config("spark.master", "local")
    .appName("movies per genre")
    .getOrCreate()

  // Import implicits for Spark encoder
  import spark.implicits._

  val movies_rdd=spark.read.textFile("E:\\MovieAnalysis\\movies.dat")
  val genre=movies_rdd.map(lines=>lines.split("::")(2))
  val flat_genre=genre.flatMap(lines=>lines.split("\\|"))
  val genre_kv = flat_genre.map(k => (k, 1)).toDF("genre", "count")
  val genre_count = genre_kv.groupBy("genre").sum("count")
  val genre_sort = genre_count.sort("genre")

  // Show the result
  genre_sort.show()
  System.exit(0)

}