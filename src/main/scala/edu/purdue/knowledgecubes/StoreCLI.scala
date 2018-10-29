package edu.purdue.knowledgecubes

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import edu.purdue.knowledgecubes.GEFI.GEFIType
import edu.purdue.knowledgecubes.storage.persistent.Store
import edu.purdue.knowledgecubes.utils.{CliParser, Timer}


object StoreCLI {

  val LOG = Logger(LoggerFactory.getLogger(getClass))

  def main(args: Array[String]): Unit = {
    val params = CliParser.parseLoader(args)
    val spark = SparkSession.builder
      .appName(s"Knowledge Cubes Loader")
      .config("spark.driver.maxResultSize", "128g")
      .config("spark.network.timeout", "100000000")
      .getOrCreate()

    val localPath = params("local")
    val dbPath = params("db")
    val ntPath = params("ntriples")
    val ftype = params("fType")
    val fp = params("fp").toDouble

    val falsePositiveRate = fp
    var filterType = GEFIType.NONE
    if (ftype == "bloom") {
      filterType = GEFIType.BLOOM
    } else if (ftype == "roaring") {
      filterType = GEFIType.ROARING
    } else if (ftype == "bitset") {
      filterType = GEFIType.BITSET
    }

    val store = Store(spark, dbPath, localPath, filterType, falsePositiveRate)

    LOG.info(s"Creating the store ntriple $dbPath")
    LOG.info(s"Reading NT File $ntPath")
    LOG.info(s"GEFI: $filterType")

    // Run the store creator
    val time = Timer.timeInSeconds{ store.create(ntPath) }
    LOG.info(s"Time: $time seconds")
    LOG.info(s"Database Created Successfully")
    store.close
    spark.stop
  }
}
