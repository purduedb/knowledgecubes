package edu.purdue.knowledgecubes

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import edu.purdue.knowledgecubes.GEFI.GEFIType
import edu.purdue.knowledgecubes.GEFI.join.GEFIJoinCreator
import edu.purdue.knowledgecubes.utils.{CliParser, Timer}

object FilterCLI {

  val LOG = Logger(LoggerFactory.getLogger(getClass))

  def main(args: Array[String]): Unit = {
    val params = CliParser.parseFilter(args)
    val spark = SparkSession.builder
      .appName(s"Knowledge Cubes Filters Creator")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    val dbPath = params("db")
    val localDBPath = params("local")
    val fp = params("fp").toDouble
    val ftype = params("ftype")

    var filterType = GEFIType.BLOOM
    if (ftype == "cuckoo") {
      filterType = GEFIType.CUCKOO
    } else if (ftype == "roaring") {
      filterType = GEFIType.ROARING
    } else if (ftype == "bitset") {
      filterType = GEFIType.BITSET
    }

    LOG.info(s"GEFI: $filterType")

    val filter = new GEFIJoinCreator(spark, dbPath, localDBPath)
    val time = Timer.timeInSeconds{filter.create(filterType, fp)}
    LOG.info(s"Time: $time seconds")
    LOG.info(s"Filters created Successfully")
    spark.stop
  }
}
