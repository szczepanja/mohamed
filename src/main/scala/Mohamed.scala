import org.apache.spark.sql.{DataFrame, SparkSession}

object Mohamed extends App {

  val spark: SparkSession = SparkSession
    .builder()
    .appName("spark-split")
    .master("local[*]")
    .getOrCreate()

  val places = spark.read.option("header", value = true).option("inferSchema", value = true).csv("places.csv")

  places.show
}
