package edu.purdue.knowledgecubes

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j.LoggerFactory

import edu.purdue.knowledgecubes.GEFI.GEFIType
import edu.purdue.knowledgecubes.GEFI.spatial.SpatialEncoder
import edu.purdue.knowledgecubes.GEFI.spatial.parsers.YAGOSpatialParser
import edu.purdue.knowledgecubes.utils.CliParser


object SemanticFiltersCLI {

  val LOG = Logger(LoggerFactory.getLogger(getClass))

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName(s"Semantic Filters Creator")
      .getOrCreate()

    val params = CliParser.parseSemanticFiltering(args)
    val localPath = params("local")
    val sType = params("sType")
    val fType = params("fType")
    val fp = params("fp")
    val input = params("input")
    val dataset = params("dataset")
    val dbPath = params("db")

    var filterType = GEFIType.BLOOM
    if (fType == "roaring") {
      filterType = GEFIType.ROARING
      LOG.info("Using Roaring Bitmap")
    } else if (fType == "bitset") {
      filterType = GEFIType.BITSET
      LOG.info("Using Bitset")
    } else {
      LOG.info("Using Bloom Filter")
    }

    val falsePositiveRate = fp.toFloat

    if (sType.equals("spatial")) {
      LOG.info("Processing Spatial Data")
      var resources = mutable.Map[Long, ListBuffer[Int]]()
      if (dataset.equals("yago")) {
        val yago = new YAGOSpatialParser(spark, dbPath, localPath)
        resources = yago.parseSpatialData()
      } else {
        LOG.error("Unsupported Dataset")
        System.exit(-1)
      }
      val numFilters = SpatialEncoder.save(resources, filterType, falsePositiveRate, localPath)
      LOG.info(s"Number of Spatial Filters created: $numFilters")
    }
  }

}
