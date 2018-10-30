package edu.purdue.knowledgecubes.GEFI.spatial.parsers

import scala.collection.mutable

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col
import org.slf4j.LoggerFactory

import edu.purdue.knowledgecubes.GEFI.spatial.{SpatialEncoder, SpatialEntry}
import edu.purdue.knowledgecubes.metadata.Catalog


class YAGOSpatialParser(spark: SparkSession, dbPath: String, localPath: String) {

  val latPredicate = "http://yago-knowledge.org/resource/hasLatitude"
  val lonPredicate = "http://yago-knowledge.org/resource/hasLongitude"

  val LOG = Logger(LoggerFactory.getLogger(classOf[YAGOSpatialParser]))
  val catalog = new Catalog(localPath, dbPath, spark)
  catalog.loadConfigurations()

  def parseSpatialData(): Map[Long, Int] = {
    import catalog.spark.implicits._
    val propertyList = catalog.tablesInfo.keySet
    var latId = ""
    var lonId = ""

    for (entry <- propertyList) {
      val predicate = catalog.tablesInfo(entry)("predicate").toString
      if(predicate.equals(latPredicate)) {
        latId = catalog.tablesInfo(entry)("uri").toString
      } else if (predicate.equals(lonPredicate)) {
        lonId = catalog.tablesInfo(entry)("uri").toString
      }
    }

    var latDF = spark.read.parquet(catalog.dataPath + latId)
    latDF.cache()
    var lonDF = spark.read.parquet(catalog.dataPath + lonId)
    lonDF.cache()

    latDF = latDF.withColumnRenamed("s", "s1")
    latDF = latDF.withColumnRenamed("p", "p1")
    latDF = latDF.withColumnRenamed("o", "o1")

    val joined = lonDF.join(latDF, latDF.col("s1").equalTo(lonDF.col("s")))
    val rawDf = joined.select(col("s"), col("o"), col("o1")).map(x => {
      SpatialEntry(x.getInt(0), s"POINT(${x.getString(1).replaceAll("\"", "").toFloat} " +
        s"${x.getString(2).replaceAll("\"", "").toFloat})")
    }).as[SpatialEntry]

    val latEntries = joined.select("s1", "o1").toLocalIterator()
    val lonEntries = joined.select("s", "o").toLocalIterator()

    var points = Map[Int, Double]()
    while (latEntries.hasNext) {
      val latEntry = latEntries.next()
      points += (latEntry.getInt(0) -> latEntry.getString(1).replaceAll("\"", "").toDouble)
    }

    val encodedPoints = mutable.Map[Long, Int]()
    while (lonEntries.hasNext) {
      val lonEntry = lonEntries.next()
      if (points.contains(lonEntry.getInt(0))) {
        var lat = points(lonEntry.getInt(0))
        var lon = lonEntry.getString(1).replaceAll("\"", "").toDouble
        val id = SpatialEncoder.encodeLonLat(lon, lat)
        encodedPoints += (id -> lonEntry.getInt(0))
      }
    }
    encodedPoints.toMap
  }
}
