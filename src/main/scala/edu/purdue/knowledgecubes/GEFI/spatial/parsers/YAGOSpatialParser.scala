package edu.purdue.knowledgecubes.GEFI.spatial.parsers

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.functions.first
import org.apache.spark.sql.functions.col
import org.apache.spark.sql._
import org.slf4j.LoggerFactory

import edu.purdue.knowledgecubes.GEFI.spatial.{SpatialEncoder, SpatialEntry}
import edu.purdue.knowledgecubes.metadata.Catalog
import edu.purdue.knowledgecubes.rdf.RDFTriple


class YAGOSpatialParser(spark: SparkSession, dbPath: String, localPath: String) {

  val latPredicate = "http://yago-knowledge.org/resource/hasLatitude"
  val lonPredicate = "http://yago-knowledge.org/resource/hasLongitude"

  val LOG = Logger(LoggerFactory.getLogger(classOf[YAGOSpatialParser]))
  val catalog = new Catalog(localPath, dbPath, spark)
  catalog.loadConfigurations()

  def parseSpatialData(): Map[Long, ListBuffer[Int]] = {
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
    latDF = latDF.filter(x => {
      val latVal = x.getString(2).replaceAll("\"", "").toDouble
      if(latVal > -90 && latVal < 90) {
        true
      } else {
        false
      }
    })

    var lonDF = spark.read.parquet(catalog.dataPath + lonId)
    lonDF.cache()
    lonDF = lonDF.filter(x => {
      val lonVal = x.getString(2).replaceAll("\"", "").toDouble
      if(lonVal > -180 && lonVal < 180) {
        true
      } else {
        false
      }
    })

    latDF = latDF.withColumnRenamed("s", "s1")
    latDF = latDF.withColumnRenamed("p", "p1")
    latDF = latDF.withColumnRenamed("o", "o1")

    val joined = lonDF.join(latDF, latDF.col("s1").equalTo(lonDF.col("s")))
    joined.cache()
    val rawDf = joined.select(col("s"), col("o"), col("o1")).map(x => {
      var latVal = x.getString(2).replaceAll("\"", "").toFloat
      var lonVal = x.getString(1).replaceAll("\"", "").toFloat
      SpatialEntry(x.getInt(0), s"POINT(${lonVal} ${latVal})")
    }).as[SpatialEntry]
    rawDf.cache()

    LOG.info(s"Number of Unique Points: ${joined.select("s").distinct().count()}")
    LOG.info(s"Number of Joined Points: ${joined.count()}")
    val joinedIter = joined.toLocalIterator()
    val encodedPoints = mutable.Map[Long, ListBuffer[Int]]()
    var resources = 0
    var sameEncoding = 0
    while (joinedIter.hasNext) {
      val entry = joinedIter.next()
      val resource = entry.getInt(0)
      val lon = entry.getString(2).replaceAll("\"", "").toDouble
      val lat = entry.getString(5).replaceAll("\"", "").toDouble
      val encoding = SpatialEncoder.encodeLonLat(lon, lat)
        if(encodedPoints.contains(encoding)) {
          encodedPoints(encoding) += resource
          resources += 1
          sameEncoding += 1
        } else {
          var list = ListBuffer[Int]()
          list += resource
          resources += 1
          encodedPoints += (encoding -> list)
        }
    }
    LOG.info(s"Number of encoded resources: ${encodedPoints.size}")
    LOG.info(s"Number of resources with same-encoding: ${sameEncoding}")
    LOG.info("Saving Spatial Index ...")
    rawDf.write.mode(SaveMode.Overwrite).parquet(dbPath + "/spatial")
    LOG.info("Saved.")
    encodedPoints.toMap
  }
}
