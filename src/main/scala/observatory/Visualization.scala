package observatory

import com.sksamuel.scrimage.{Image, Pixel}

import scala.collection.GenSeq
import scala.collection.parallel.ParIterable

/**
  * 2nd milestone: basic visualization
  */
object Visualization {
  val dThresh = 1

  def toRadians(x: Double) = x * Math.PI / 180

  def d(loc1: Location, loc2: Location): Double = {
    val R = 6371.0 // kmetres

    val (lat1, lon1) = Location.unapply(loc1).get
    val (lat2, lon2) = Location.unapply(loc2).get

    val φ1 = toRadians(lat1)
    val φ2 = toRadians(lat2)

    val Δφ = toRadians(lat2 - lat1)
    val Δλ = toRadians(lon2 - lon1)

    val a = Math.sin(Δφ / 2) * Math.sin(Δφ / 2) + Math.cos(φ1) * Math.cos(φ2) * Math.sin(Δλ / 2) * Math.sin(Δλ / 2)
    val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))

    R * c
  }

  /**
    * @param temperatures Known temperatures: pairs containing a location and the temperature at this location
    * @param location Location where to predict the temperature
    * @return The predicted temperature at `location`
    */
  def predictTemperature(temperatures: Iterable[(Location, Double)], location: Location): Double = {
    def wFromD(d: Double): Double = 1.0 / math.pow(d, 2)

    val dts = temperatures.map { case (l, t) => (d(location, l), t) }
    val d0 = dts.find(_._1 <= dThresh)
    if (d0.isDefined) d0.get._2
    else {
      val wts = dts.map(x => (wFromD(x._1), x._2))
      wts.aggregate(0.0)(
        seqop = {
          case (acc, (w, t)) => acc + w * t
        },
        combop = {
          case (a1, a2) => a1 + a2
        }
      ) / wts.aggregate(0.0)(
        seqop = {
          case (acc, (w, t)) => acc + w
        },
        combop = {
          case (a1, a2) => a1 + a2
        }
      )
    }
  }

  def interpolate2(p1: (Double, Color), p2: (Double, Color), value: Double): Color = {
    assert(p1._1 <= value && p2._1 >= value)
    val ratio = (value - p1._1) / (p2._1 - p1._1)

    Color(
      p1._2.red + math.round(ratio * (p2._2.red - p1._2.red)).toInt,
      p1._2.green + math.round(ratio * (p2._2.green - p1._2.green)).toInt,
      p1._2.blue + math.round(ratio * (p2._2.blue - p1._2.blue)).toInt
    )
  }

  /**
    * @param points Pairs containing a value and its associated color
    * @param value The value to interpolate
    * @return The color that corresponds to `value`, according to the color scale defined by `points`
    */
  def interpolateColor(points: Iterable[(Double, Color)], value: Double): Color = {
    interpolateColor(points.toArray, value, isSorted = false)
  }

  def interpolateColor(points: Array[(Double, Color)], value: Double, isSorted: Boolean): Color = {
    val sortedPoints =
      if (isSorted) points else points.sortBy(_._1)

    val pairOpt = sortedPoints.sliding(2).find{p =>
      p(0)._1 <= value && p(1)._1 >= value
    }

    pairOpt match {
      case Some(pair) => interpolate2(pair(0), pair(1), value)
      case None =>
        if (value <= sortedPoints.head._1) sortedPoints.head._2
        else sortedPoints.last._2
    }
  }

  /**
    * @param temperatures Known temperatures
    * @param colors Color scale
    * @return A 360×180 image where each pixel shows the predicted temperature at its location
    */
  def visualize(temperatures: Iterable[(Location, Double)], colors: Iterable[(Double, Color)]): Image = {
    val sortedColors = colors.toArray.sortBy(_._1)
    val locations = for {
      lat <- -90  until 90
      lon <- -180 until 180
    } yield (lat.toDouble, lon.toDouble)

    val pixels = locations.toArray.par.map {loc =>
      val temp = predictTemperature(temperatures, Location(loc._1, loc._2))
      val color = interpolateColor(sortedColors, temp, true)
      Pixel(color.red, color.green, color.blue, 255)
    }.toArray

    Image(360, 180, pixels)
  }

  val colorTable: Array[(Double, Color)] = Array(
    (60,  Color(255, 255, 255)),
    (32,  Color(255, 0,   0)),
    (12,  Color(255, 255, 0)),
    (0,   Color(0,   255, 255)),
    (-15, Color(0  , 0,   255)),
    (-27, Color(255, 0,   255)),
    (-50, Color(33 , 0,   107)),
    (-60, Color(0  , 0,   0))
  )
}

