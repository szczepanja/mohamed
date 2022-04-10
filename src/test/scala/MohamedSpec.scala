import Mohamed._
import spark.implicits._
import org.scalatest._
import flatspec._
import matchers._

class MohamedSpec extends AnyFlatSpec with should.Matchers {

  val source = Seq(
    (0, "Warsaw", "Poland"),
    (1, "Villeneuve-Loubet", "France"),
    (2, "Vranje", "Serbia"),
    (3, "Pittsburgh", "US")
  ).toDF("id", "city", "country")

  val expected = Seq(
    (0, "Warsaw", "Poland", "POLAND"),
    (1, "Villeneuve-Loubet", "France", "FRANCE"),
    (2, "Vranje", "Serbia", "SERBIA"),
    (3, "Pittsburgh", "US", "US")
  ).toDF("id", "city", "country", "upper_country")

  "places" should "transform given CSV file into dataFrame" in {
    places shouldBe source
  }
}
