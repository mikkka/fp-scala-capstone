package observatory


import org.scalatest.FunSuite
import org.scalatest.prop.Checkers

trait VisualizationTest extends FunSuite with Checkers {
  test("d test") {
    check {
      math.round(Visualization.d(Location(0, 0), Location(1, 1))) == 157
      math.round(Visualization.d(Location(23, 12), Location(-4, -29))) == 5366
      math.round(Visualization.d(Location(91, 89), Location(89, 91))) == 222
    }
  }

  test("predictTemperature should use closest (1 < km) point") {
    val temp = Visualization.predictTemperature(
      Seq(
        (Location(0.995, 0.995), 0.0),
        (Location(100.0, 0.0), 100.0),
        (Location(0.0, 100.0), 100.0)
      ),
      Location(1.0, 1.0)
    )

    check{
      temp == 0.0
    }
  }
}
