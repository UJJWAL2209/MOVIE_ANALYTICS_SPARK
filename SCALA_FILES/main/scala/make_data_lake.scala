import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object make_data_lake extends App {
  System.setProperty("hadoop.home.dir", "C:\\Users\\ASUS\\Desktop\\Movie_analytics\\Hadoop\\home")
  System.setProperty("hadoop.home.bin", "C:\\Users\\ASUS\\Desktop\\Movie_analytics\\Hadoop\\bin")

  val spark = SparkSession.builder.config("spark.master", "local")
    .appName("make datalake")
    .getOrCreate()

  // 2nd method is to read the file directly into a dataFrame and create a temp view
  spark.read.textFile("C:\\Users\\ASUS\\Desktop\\Movie_analytics\\MovieAnalysis\\movies.dat").createOrReplaceTempView("movies_staging")
  spark.read.textFile("C:\\Users\\ASUS\\Desktop\\Movie_analytics\\MovieAnalysis\\ratings.dat").createOrReplaceTempView("ratings_staging")
  spark.read.textFile("C:\\Users\\ASUS\\Desktop\\Movie_analytics\\MovieAnalysis\\users.dat").createOrReplaceTempView("users_staging")

  // Create a database to store the tables
  spark.sql("DROP DATABASE IF EXISTS sparkdatalake CASCADE")
  spark.sql("CREATE DATABASE sparkdatalake")

  // Make appropriate schemas for them
  // movies
  val moviesDataFrame = spark.sql("""
    SELECT
      split(value,'::')[0] as movieid,
      split(value,'::')[1] as title,
      substring(split(value,'::')[1],length(split(value,'::')[1])-4,4) as year,
      split(value,'::')[2] as genre
    FROM movies_staging
  """)
  //.write.mode("overwrite").saveAsTable("sparkdatalake.movies");
  moviesDataFrame.show()
  // Count the number of movies per year
  val resultDataFrame = moviesDataFrame
    .withColumn("year", col("year").cast("integer"))
    .groupBy("year")
    .agg(count("year").alias("Number_of_Movies"))
    .orderBy("year")

  // Show the resulting DataFrame
  resultDataFrame.show()

  // users
  val usersDataFrame = spark.sql(
    """
    SELECT
      split(value,'::')[0] as userid,
      split(value,'::')[1] as gender,
      split(value,'::')[2] as age,
      split(value,'::')[3] as occupation,
      split(value,'::')[4] as zipcode
    FROM users_staging
  """)

    //.write.mode("overwrite").saveAsTable("sparkdatalake.users");
  usersDataFrame.show()

  // ratings
  val ratingsDataFrame = spark.sql("""
    SELECT
      split(value,'::')[0] as userid,
      split(value,'::')[1] as movieid,
      split(value,'::')[2] as rating,
      split(value,'::')[3] as timestamp
    FROM ratings_staging
  """)
    //.write.mode("overwrite").saveAsTable("sparkdatalake.ratings");
  ratingsDataFrame.show()
  // Count the number of movies per rating using DataFrame API
  val result1DataFrame = ratingsDataFrame
    .groupBy("rating")
    .agg(count("rating").alias("Number_of_Movies_per_rating"))
    .orderBy("rating")

  // Show the resulting DataFrame
  result1DataFrame.show()

  // Compute the total ratings per movie using DataFrame API
  val result2DataFrame = ratingsDataFrame
    .groupBy("movieid")
    .agg(sum("rating").alias("Total_ratings"))
    .orderBy(col("movieid").cast("int"))

  // Show the resulting DataFrame
  result2DataFrame.show()

  // Stop the Spark session
  spark.stop()
  System.exit(0)
}
