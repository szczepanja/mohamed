import org.apache.spark.sql.SparkSession

object Mohamed extends App {

  val spark: SparkSession = SparkSession
    .builder()
    .appName("spark-split")
    .master("local[*]")
    .getOrCreate()

}
