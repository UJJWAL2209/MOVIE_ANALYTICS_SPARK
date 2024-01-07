import org.apache.spark.SparkContext

object begin extends App{

  val SparkContext = new SparkContext(master = "local" ,appName = "hello")

  val sourceRDD = SparkContext.textFile(path = "C:\\Users\\ASUS\\Desktop\\Movie_analytics\\WELCOME_NOTE.txt")

    sourceRDD.take(num = 1).foreach(println)


}
