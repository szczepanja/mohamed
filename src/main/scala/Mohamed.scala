import org.apache.spark.sql.{DataFrame, SparkSession}

object Mohamed extends App {

  val spark: SparkSession = SparkSession
    .builder()
    .appName("spark-mohamed")
    .master("local[*]")
    .getOrCreate()

  val (filePath, columnName) = (args.head, args.tail)

  val places = spark.read
    .option("header", value = true)
    .option("inferSchema", value = true)
    .csv(filePath)

}
