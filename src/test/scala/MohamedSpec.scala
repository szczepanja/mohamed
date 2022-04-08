import org.scalatest._
import flatspec._
import org.apache.spark.sql.{DataFrame, SparkSession}

class MohamedSpec extends AnyFlatSpec with BeforeAndAfterAll {

  @transient var spark: SparkSession = _

  def loadPlaces(spark: SparkSession, csv: String): DataFrame = spark.read.option("header", value = true).csv(csv)

  "loadPlaces" should "load csv file and return number of counted rows" in {
    val sampleDF = loadPlaces(spark, "places.csv")
    val count = sampleDF.count()
    assert(count == 6, "Record should be 6")
  }
}
