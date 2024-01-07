import org.apache.spark.sql.SparkSession

object ratingsprepare extends App{
  val spark = SparkSession.builder.config("spark.master", "local")
    .appName("MoviesRanking")
    .getOrCreate()

  System.setProperty("hadoop.home.dir", "C:\\Users\\ASUS\\Desktop\\Movie_analytics\\Hadoop")
  import spark.implicits._
  val ratingsRDD=spark.sparkContext.textFile("E:\\MovieAnalysis\\ratings.dat")

  import org.apache.spark.sql.Row;
  import org.apache.spark.sql.types.{StructType, StructField, StringType};

  val schemaString = "UserID MovieID Rating Timestamp"

  val schema = StructType(schemaString.split(" ").map(fieldName ⇒ StructField(fieldName, StringType, true)))

  val rowRDD = ratingsRDD.map(_.split("::")).map(x ⇒ Row(x(0), x(1), x(2), x(3)))

  val ratings = spark.createDataFrame(rowRDD, schema)

  ratings.show()
}
