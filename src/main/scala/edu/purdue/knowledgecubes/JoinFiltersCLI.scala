package edu.purdue.knowledgecubes

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import edu.purdue.knowledgecubes.GEFI.GEFIType
import edu.purdue.knowledgecubes.GEFI.join.GEFIJoinCreator
import edu.purdue.knowledgecubes.utils.{CliParser, Timer}

object JoinFiltersCLI {

  val LOG = Logger(LoggerFactory.getLogger(getClass))

  def main(args: Array[String]): Unit = {
    val params = CliParser.parseGEFIFiltering(args)
    val spark = SparkSession.builder
      .appName(s"Join Filters Creator")
      .getOrCreate()

    val dbPath = params("db")
    val localPath = params("local")
    val fp = params("fp").toDouble
    val ftype = params("fType")

    var filterType = GEFIType.BLOOM
    if (ftype == "roaring") {
      filterType = GEFIType.ROARING
    } else if (ftype == "bitset") {
      filterType = GEFIType.BITSET
    }

    LOG.info(s"GEFI: $filterType")

    val filter = new GEFIJoinCreator(spark, dbPath, localPath)
    val time = Timer.timeInSeconds{filter.create(filterType, fp)}
    LOG.info(s"Time: $time seconds")
    LOG.info(s"Filters created Successfully")
    spark.stop
  }
}
