import org.apache.spark.sql.SparkSession

object oldest_movies extends App{

  val spark = SparkSession.builder.config("spark.master", "local")
    .appName("oldest movies")
    .getOrCreate()


  val movies_rdd=spark.read.textFile("E:\\MovieAnalysis\\movies.dat")
  // 1st method, convert existing rdd into DF using toDF function and then make it into a view
  val movies_DF=movies_rdd.toDF.createOrReplaceTempView("movies_view")
  // To use spark.sql, it should be at least a temporary view or even an table
  spark.sql(""" select
split(value,'::')[0] as movieid,
split(value,'::')[1] as moviename,
substring(split(value,'::')[1],length(split(value,'::')[1])-4,4) as year
from movies_view """).createOrReplaceTempView("movies");
  spark.sql("select * from movies").show()
  System.exit(0)

}
