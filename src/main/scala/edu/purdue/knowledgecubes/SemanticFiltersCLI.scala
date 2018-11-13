package edu.purdue.knowledgecubes

import java.io.{File, FileOutputStream, ObjectOutputStream}

import scala.collection.mutable.ListBuffer

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import edu.purdue.knowledgecubes.GEFI.{GEFI, GEFIType}
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
    val count = params("count")


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
    val numElements = count.toInt

    if (sType.equals("spatial")) {
      LOG.info("Processing Spatial Data")
      var encodedPoints = Map[Long, ListBuffer[Int]]()
      if (dataset.equals("yago")) {
        val yago = new YAGOSpatialParser(spark, dbPath, localPath)
        encodedPoints = yago.parseSpatialData()
      } else {
        LOG.error("Unsupported Dataset")
        System.exit(-1)
      }
      LOG.info("Saving Spatial Filters ...")
      val filters = SpatialEncoder.createFilters(encodedPoints, numElements, localPath)
      val numFilters = save(filters, filterType, falsePositiveRate, localPath, sType)
      LOG.info(s"Number of Spatial Filters created: $numFilters")
    }
  }

  def save(filters: Map[Int, List[Int]],
           filterType: GEFIType.Value,
           falsePositiveRate: Float,
           localPath: String, sType: String): Int = {
    var counter = 0
    for ((k, v) <- filters) {
      counter += 1
      val filterName = k
      val filterSize = v.size
      val filter = new GEFI(filterType, filterSize, falsePositiveRate)

      for (value <- v) {
        filter.add(value)
      }
      val fullPath = localPath + s"/GEFI/${sType}/" + filterType.toString + "/" + falsePositiveRate.toString
      val directory = new File(fullPath)
      if (!directory.exists) directory.mkdirs()
      val fout = new FileOutputStream(fullPath + "/" + filterName)
      val oos = new ObjectOutputStream(fout)
      oos.writeObject(filter)
      oos.close()
    }
    counter
  }
}
