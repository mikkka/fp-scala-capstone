package observatory

import java.nio.file.Paths
import java.time.LocalDate

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.util.Try

/**
  * 1st milestone: data extraction
  */
object Extraction {
  import org.apache.log4j.{Level, Logger}


  private def toCelcius(f: Double) = (f - 32) * 5 / 9

  /**
    * @param year             Year number
    * @param stationsFile     Path of the stations resource file to use (e.g. "/stations.csv")
    * @param temperaturesFile Path of the temperatures resource file to use (e.g. "/1975.csv")
    * @return A sequence containing triplets (date, location, temperature)
    */
  def locateTemperatures(year: Int, stationsFile: String, temperaturesFile: String): Iterable[(LocalDate, Location, Double)] = {
    def fsPath(resource: String): String =
      Paths.get(getClass.getResource(resource).toURI).toString

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val spark: SparkSession =
      SparkSession
        .builder()
        .appName("locateTemperatures")
        .config("spark.master", "local")
        .getOrCreate()
    import spark.implicits._

    val stationsScheme = StructType(List(
      StructField("stn", StringType, false),
      StructField("wban", StringType, false),
      StructField("lat", DoubleType, true),
      StructField("lon", DoubleType, true)
    ))

    val stationsRdd =
      spark.sparkContext.textFile(fsPath(stationsFile))
        .map(line =>
          line.split("\\,", -1).toVector
        )
        .map(x =>
          Row(x(0), x(1),
            Try(x(2).toDouble).getOrElse(null),
            Try(x(3).toDouble).getOrElse(null)
          )
        )

    val stationsFrame =
      spark.createDataFrame(stationsRdd, stationsScheme).filter($"lat".isNotNull && $"lon".isNotNull)

    val temperaturesScheme = StructType(List(
      StructField("stn", StringType, false),
      StructField("wban", StringType, false),
      StructField("month", IntegerType, false),
      StructField("day", IntegerType, false),
      StructField("temperature", DoubleType, false)
    ))
    val temperaturesRdd =
      spark.sparkContext.textFile(fsPath(temperaturesFile))
        .map(_.split("\\,", -1).toVector)
        .map(x =>
          Row(x(0), x(1), x(2).toInt, x(3).toInt, toCelcius(x(4).toDouble)  )
        )
    val temperaturesFrame =
      spark.createDataFrame(temperaturesRdd, temperaturesScheme)

    temperaturesFrame
      .join(stationsFrame, List("stn", "wban"))
      .select(col("month"), col("day"), col("lat"), col("lon"), col("temperature"))
      .rdd
      .map{row =>
        (LocalDate.of(year, row.getAs[Int](0), row.getAs(1)), Location(row.getAs(2), row.getAs(3)), row.getAs(4))
      }.collect()
    }

  /**
    * @param records A sequence containing triplets (date, location, temperature)
    * @return A sequence containing, for each location, the average temperature over the year.
    */
  def locationYearlyAverageRecords(records: Iterable[(LocalDate, Location, Double)]): Iterable[(Location, Double)] = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val spark =
      SparkSession
        .builder()
        .appName("locationYearlyAverageRecords")
        .config("spark.master", "local")
        .getOrCreate()
    import spark.implicits._
    
    val rdd = spark.sparkContext.parallelize(records.toSeq)
      .map(x => Row(x._1.getYear, x._2.lat, x._2.lon, x._3))

    val frame = spark.createDataFrame(rdd, StructType(List(
      StructField("year", IntegerType, false),
      StructField("lat", DoubleType, false),
      StructField("lon", DoubleType, false),
      StructField("temperature", DoubleType, false)
    )))

    frame.groupBy($"year", $"lat", $"lon")
      .avg("temperature")
      .select($"lat", $"lon", $"avg(temperature)")
      .rdd
      .map{row =>
        (Location(row.getAs(0), row.getAs(1)), row.getAs(2))
      }.collect()
  }
}
