package edu.purdue.knowledgecubes.storage.persistent

import scala.collection.mutable.Map

import com.typesafe.scalalogging.Logger
import org.apache.commons.lang.RandomStringUtils
import org.apache.jena.riot.Lang
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.col
import org.slf4j.LoggerFactory

import edu.purdue.knowledgecubes.GEFI.GEFIType
import edu.purdue.knowledgecubes.GEFI.join.GEFIJoinUtils
import edu.purdue.knowledgecubes.metadata.Catalog
import edu.purdue.knowledgecubes.utils.NTParser


class Store(spark: SparkSession,
            dbPath: String,
            localPath: String,
            filterType: GEFIType.Value,
            falsePositiveRate: Double) {

  val catalog = new Catalog(localPath, dbPath, spark)
  val LOG = Logger(LoggerFactory.getLogger(classOf[Store]))

  def verifyDataFrameColumns(triples: DataFrame): DataFrame = {
    val names = triples.schema.fieldNames
    if (names.size != 3) {
      LOG.error("Illegal triples format - Only 3 fields allowed (s,p,o)")
      System.exit(-1)
    }
    var triplesDataFrame = triples.withColumnRenamed(names(0), "s")
    triplesDataFrame = triplesDataFrame.withColumnRenamed(names(1), "p")
    triplesDataFrame = triplesDataFrame.withColumnRenamed(names(2), "o")
    triplesDataFrame
  }

  def create(triples: DataFrame): Unit = {
    val triplesDataFrame = verifyDataFrameColumns(triples)
    // Get all unique predicates
    val propertyList = triplesDataFrame.select("p").distinct.collect
    val numProperties = propertyList.length
    val numTuples = triplesDataFrame.count

    LOG.debug(s"Number of properties : $numProperties")

    // Catalog information about the data currently ntriple the database
    catalog.dbInfo += ("numProperties" -> numProperties.toString)
    catalog.dbInfo += ("numTuples" -> numTuples.toString)
    catalog.dbInfo += ("dbPath" -> dbPath)

    var fileNames = Map[String, String]()

    var fileCounter = 0
    for (r <- propertyList) {
      fileCounter += 1
      val uri = r.getInt(0).toString
      val fileName = fileCounter.toString
      fileNames += (uri -> fileName)
    }

    var count = 0
    val lang: Lang = Lang.NTRIPLES

    for ((uri, tableName) <- fileNames) {
      count += 1
      val data = triplesDataFrame.filter(col("p").equalTo(uri))
      data.cache()
      val size = data.count()
      // Statistics
      val propTable = Map[String, String]()
      propTable += ("uri" -> uri)
      propTable += ("tableName" -> tableName)
      propTable += ("numTuples" -> size.toString)
      propTable += ("ratio" -> (size.toFloat / numTuples.toFloat).toString)

      val subData = data.select(col("s")).distinct()
      val uniqSub = subData.count()
      propTable +=  ("unique_s" -> uniqSub.toString)

      val objData = data.select(col("o")).distinct()
      val uniqObj = objData.count()
      propTable +=  ("unique_o" -> uniqObj.toString)

      data.write.mode(SaveMode.Overwrite).parquet(catalog.dataPath + tableName)
      LOG.info(s"Processed ($count/$numProperties) : $uri - File: $tableName ($size)")
      catalog.addTable(propTable.toMap)

      if (!filterType.equals(GEFIType.NONE)) {
        GEFIJoinUtils.create(filterType, uniqSub, falsePositiveRate, subData, "s", tableName, catalog.localPath)
        GEFIJoinUtils.create(filterType, uniqObj, falsePositiveRate, objData, "o", tableName, catalog.localPath)
      }
      data.unpersist()
    }
    triplesDataFrame.unpersist()
    catalog.save()
  }

  def create(ntPath: String): Unit = {
    val triplesDataFrame = NTParser.parse(spark, ntPath)
    triplesDataFrame.cache()
    create(triplesDataFrame)
  }
}

object Store {

  def apply(spark: SparkSession,
            dbPath: String,
            localPath: String,
            filterType: GEFIType.Value,
            falsePositiveRate: Double): Store = new Store(spark, dbPath, localPath, filterType, falsePositiveRate)

  def apply(spark: SparkSession,
            dbPath: String,
            localPath: String): Store = new Store(spark, dbPath, localPath, GEFIType.NONE, 0)
}
