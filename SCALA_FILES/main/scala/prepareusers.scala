import org.apache.spark.sql.SparkSession

object prepareusers extends App {
  val spark = SparkSession.builder.config("spark.master", "local")
    .appName("users")
    .getOrCreate()

  System.setProperty("hadoop.home.dir", "C:\\Users\\ASUS\\Desktop\\Movie_analytics\\Hadoop")
  import spark.implicits._

  val usersRDD=spark.sparkContext.textFile("E:\\MovieAnalysis\\users.dat")

  import org.apache.spark.sql.Row;
  import org.apache.spark.sql.types.{StructType, StructField, StringType};

  val schemaString = "UserID Gender Age Occupation Zip-code"

  val schema = StructType(schemaString.split(" ").map(fieldName ⇒ StructField(fieldName, StringType, true)))

  val rowRDD = usersRDD.map(_.split("::")).map(x ⇒ Row(x(0), x(1), x(2), x(3), x(4)))

  val users_1 = spark.createDataFrame(rowRDD, schema)

  users_1.show()

}
