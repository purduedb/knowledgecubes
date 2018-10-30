package edu.purdue.knowledgecubes.queryprocessor

import java.io._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.fusesource.leveldbjni.JniDBFactory.{asString, bytes}
import org.slf4j.LoggerFactory

import edu.purdue.knowledgecubes.GEFI.{GEFI, GEFIType}
import edu.purdue.knowledgecubes.metadata.{Catalog, Result}
import edu.purdue.knowledgecubes.queryprocessor.executor.Executor
import edu.purdue.knowledgecubes.queryprocessor.optimizer.Optimizer
import edu.purdue.knowledgecubes.queryprocessor.parser.Parser
import edu.purdue.knowledgecubes.storage.cache.{CacheEntryType, CacheManager}


class QueryProcessor(spark: SparkSession,
                     dbPath: String,
                     localPath: String,
                     filterType: GEFIType.Value,
                     falsePositiveRate: Double) {

  val LOG = Logger(LoggerFactory.getLogger(classOf[QueryProcessor]))

  val catalog = new Catalog(localPath, dbPath, spark)
  catalog.filterType = filterType
  catalog.loadConfigurations()

  def sparql(query: String): List[Row] = {
    val result = benchmark(query)
    var listResult = result.output.collect()
    var numColumns = result.projectionList.size

    var finalResults = ListBuffer[Row]()
    for (res <- listResult) {
      var newVals = ListBuffer[String]()
      for(i <- 0 until numColumns) {
        val v = catalog.dictionaryId2Str.get(bytes(res.get(i).toString))
        if (v == null) {
          newVals += res.get(i).toString
        } else {
          newVals += asString(v)
        }
      }
      finalResults += Row.fromSeq(newVals)
    }
    finalResults.toList
  }

  def benchmark(query: String): Result = {
    // Parser
    val opRoot = Parser.parse(query)
    // Optimizer
    val optimizer = new Optimizer(catalog)
    val opRootOptimized = optimizer.usingStatistics(opRoot)
    // Executor
    val executor = new Executor(catalog)
    val result = executor.run(opRootOptimized)
    result
  }

  def loadFilters(filterType: GEFIType.Value, falsePositiveRate: Double): Unit = {
    val joinFilters = loadJoinFilters(filterType, falsePositiveRate)
    if (joinFilters.isDefined) {
      LOG.info("Loading Join Filters")
      catalog.broadcastJoinFilters = spark.sparkContext.broadcast(joinFilters.get)
    }
  }

  def loadSpatialStructures(): Unit = {
    LOG.info("Creating Spatial Index")

    val rawDf = catalog.spark.read.parquet(catalog.dbPath + "/spatial")
    rawDf.createOrReplaceTempView("rawDf")
    val pointDf = catalog.spark.sql(s"""
                                       |SELECT s, ST_GeomFromWKT(rawDf.geom) AS geom
                                       |FROM rawDf
                                    """.stripMargin)
    pointDf.createOrReplaceTempView("pointDf")
    pointDf.cache()
    val numPoints = pointDf.count()
    LOG.info(s"Indexed: $numPoints")
    val spatialFilters = loadSpatialFilters(GEFIType.ROARING, 0)
    if (spatialFilters.isDefined) {
      LOG.info("Loading Spatial Filters")
      catalog.broadcastSpatialFilters = spark.sparkContext.broadcast(spatialFilters.get)
    }
  }

  private def loadSpatialFilters(filterType: GEFIType.Value,
                              falsePositiveRate: Double): Option[Map[Long, GEFI]] = {
    val fullPath = catalog.localPath + "/GEFI/spatial/" + filterType.toString + "/" + falsePositiveRate.toString
    val folder = new File(fullPath)
    val listOfFiles = folder.listFiles
    LOG.debug("Loading " + listOfFiles.length + " Spatial Filters")

    if (folder.exists()) {
      val spatialFilters = mutable.Map[Long, GEFI]()
      for (fileEntry <- listOfFiles) {
        if (fileEntry.isFile) {
          val name = fileEntry.getName
          try {
            val fin = new FileInputStream(new File(fullPath + "/" + name))
            val ois = new ObjectInputStream(fin)
            val filter = ois.readObject.asInstanceOf[GEFI]
            spatialFilters += (name.toLong -> filter)
          } catch {
            case e: ClassNotFoundException =>
              e.printStackTrace()
            case e: FileNotFoundException =>
              e.printStackTrace()
            case e: IOException =>
              e.printStackTrace()
          }
        }
      }
      Some(spatialFilters.map{case (key, value) => (key, value)}.toMap)
    } else {
      None
    }
  }

  private def loadJoinFilters(filterType: GEFIType.Value,
                              falsePositiveRate: Double): Option[Map[String, Map[String, GEFI]]] = {
    val fullPath = catalog.localPath + "/GEFI/join/" + filterType.toString + "/" + falsePositiveRate.toString
    val folder = new File(fullPath)
    val listOfFiles = folder.listFiles
    LOG.debug("Loading " + listOfFiles.length + " Join Filters")

    if (folder.exists()) {
      val joinFilters = mutable.Map[String, mutable.Map[String, GEFI]]()
      for (fileEntry <- listOfFiles) {
        if (fileEntry.isFile) {
          val name = fileEntry.getName
          var propertyName = ""
          var sub = false
          if (name.startsWith("s_")) {
            val parts = name.split("s\\_")
            propertyName = parts(1)
            sub = true
          } else {
            val parts = name.split("o\\_")
            propertyName = parts(1)
            sub = false
          }
          try {
            val fin = new FileInputStream(new File(fullPath + "/" + name))
            val ois = new ObjectInputStream(fin)
            val filter = ois.readObject.asInstanceOf[GEFI]

            if (!sub && joinFilters.contains(propertyName)) {
              joinFilters(propertyName) += ("o" -> filter)
            } else if (sub && joinFilters.contains(propertyName)) {
              joinFilters(propertyName) += ("s" -> filter)
            } else if (!sub && !joinFilters.contains(propertyName)) {
              val entry = mutable.Map[String, GEFI]()
              entry += ("o" -> filter)
              joinFilters += (propertyName -> entry)
            } else if (sub && !joinFilters.contains(propertyName)) {
              val entry = mutable.Map[String, GEFI]()
              entry += ("s" -> filter)
              joinFilters += (propertyName -> entry)
            }
          } catch {
            case e: ClassNotFoundException =>
              e.printStackTrace()
            case e: FileNotFoundException =>
              e.printStackTrace()
            case e: IOException =>
              e.printStackTrace()
          }
        }
      }
      Some(joinFilters.map{case (key, value) => (key, value.toMap)}.toMap)
    } else {
      None
    }
  }

  def clearCache(): Unit = CacheManager.clear()

  def close(): Unit = {
    saveReductions()
    clearCache()
    catalog.dictionaryStr2Id.close()
  }

  def saveReductions(): Unit = {
    for ((pattern, entry) <- CacheManager.entries) {
      if (entry.entryType == CacheEntryType.JOIN_REDUCTION) {
        if (!catalog.joinReductionsInfo.contains(entry.name)) {
          if (entry.size > 0) {
            val path = catalog.joinReductionsPath + entry.name
            entry.data.write.mode(SaveMode.Overwrite).parquet(path)
          }
          catalog.joinReductionsInfo += (entry.name -> entry.size)
        }
      }
    }
    catalog.save()
  }

}

object QueryProcessor {
  def apply(spark: SparkSession,
            dbPath: String,
            localPath: String,
            filterType: GEFIType.Value,
            falsePositiveRate: Double,
            spatialSupport: Boolean): QueryProcessor = {
    val qp = new QueryProcessor(spark, dbPath, localPath, filterType, falsePositiveRate)
    if (filterType != GEFIType.NONE) {
      qp.loadFilters(filterType, falsePositiveRate)
    }
    if (spatialSupport) {
      qp.loadSpatialStructures()
    }
    qp
  }

  def apply(spark: SparkSession,
            dbPath: String,
            localPath: String): QueryProcessor = {
    val qp = new QueryProcessor(spark, dbPath, localPath, GEFIType.NONE, 0)
    qp
  }
}
