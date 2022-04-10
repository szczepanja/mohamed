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

  import org.apache.spark.sql.functions._

  def toUpperColumn: DataFrame = columnName.foldLeft(places)((column, arg) => {
    column.withColumn("upper_" + arg, upper(col(arg)))
  })

  toUpperColumn.show()
}
