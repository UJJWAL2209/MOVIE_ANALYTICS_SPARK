import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.rdd.RDD

object data_function {

  //Creating a function for loading the dataset into an RDD dataframe
  //For this, we have created a Spark Session in the other file

  //The parameters for this user-defined method is

  /**
   * Loads and prepares the movies data into a DataFrame. Preprocess the movie
   * release year in a new column.
   *
   * @param sparkSession Spark session to be used for this App
   * @param pathToFile   path to movies.dat
   * @return Dataframe [id, name, categories, year]
   */

  def loadDatFile(sparkSession: SparkSession, pathToFile: String): DataFrame = {
    sparkSession.read
      .option("sep", "::")
      .option("inferSchema", "true")
      .option("header", "false")
      .csv(pathToFile)
  }

  // in this function, the dataset is loaded into a dataframe with some preprocessing processes done to the dataset
  //This function is designed for the movies dataset

  def loadMovies(sparkSession: SparkSession, pathToFile: String): DataFrame = {
    val colNames = Seq("MovieID", "Title","Genres")
    val df = loadDatFile(sparkSession, pathToFile)
      .toDF(colNames: _*)
    //.drop("null1", "null2")

    //Extracts the year from the movie name and set it in a new column for future use
    df.withColumn("Year", regexp_extract(col("Title"), "(\\d{4})", 1))
  }

  //Now, for the users dataset, this function is designed to load the dataset into a Dataframe

  def loadUsers(sparkSession: SparkSession, pathToFile: String): DataFrame = {
    val colNames = Seq(
      "UserID",
      "Gender",
      "Age",
      "Occupation",
      "Zip-code"
    )
    val genders = Seq("M", "F")

    loadDatFile(sparkSession, pathToFile)
      .toDF(colNames: _*)
      .drop("null1", "null2", "null3", "null4")
      // Keep records with genders in the genders Sequence
      .where(col("Gender").isin(genders: _*))
  }

  //For the ratings dataset, this function is designed to load the dataset into a Dataframe
  def loadRatings(sparkSession: SparkSession, pathToFile: String): DataFrame = {
    val colNames =
      Seq("UserID","MovieID","Rating","Timestamp")

    loadDatFile(sparkSession, pathToFile)
      .toDF(colNames: _*)
      .drop("null1", "null2", "null3")
      .filter("rating <= 5")
  }

  val outputPath = "E:\\MovieAnalysis\\output.html"
  def writeDataFrameToHtml(dataFrame: DataFrame,title : String ,outputPath: String): Unit = {
    // Collect DataFrame rows as an array
    val rows = dataFrame.collect()

    // Convert rows to HTML string (adjust this part based on your DataFrame structure)
    val htmlString =
      s"""
         |<html>
         |<head><title>$title</title></head>
         |<body>
         |<h2>$title</h2>
         |<table>
         |${rows.map(row => s"<tr>${row.mkString("<td>", "</td><td>", "</td>")}</tr>").mkString("")}
         |</table>
         |</body>
         |</html>
         |""".stripMargin

    //val htmlString = rows.map(row => s"<tr>${row.mkString("<td>", "</td><td>", "</td>")}</tr>").mkString("<table>", "", "</table>")
    // Write HTML string to file
    import java.nio.file.{Files, Paths}
    Files.write(Paths.get(outputPath), htmlString.getBytes)

    println(s"HTML file written to: $outputPath")
  }

  // Obtain the average range per gender and year

  def moviesRankingByGender(moviesDf: DataFrame,
                            usersDf: DataFrame,
                            ratingsDf: DataFrame): DataFrame = {
    ratingsDf
      .join(usersDf, "UserID")
      .join(moviesDf, "MovieID")
      .groupBy("MovieID", "Title", "Year", "Gender")
      .agg(avg(ratingsDf.col("rating")).alias("Avg_rating"))
      .orderBy("MovieID")
  }

}