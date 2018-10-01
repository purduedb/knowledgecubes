package edu.purdue.knowledgecubes.storage.persistent

import scala.collection.mutable.Map

import com.typesafe.scalalogging.Logger
import org.apache.jena.riot.Lang
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.apache.spark.sql.functions.col
import org.slf4j.LoggerFactory

import edu.purdue.knowledgecubes.GEFI.GEFIType
import edu.purdue.knowledgecubes.GEFI.join.GEFIJoinUtils
import edu.purdue.knowledgecubes.metadata.Catalog
import edu.purdue.knowledgecubes.rdf.RDFTriple
import edu.purdue.knowledgecubes.utils.NTParser


class Store(spark: SparkSession,
            dbPath: String,
            localPath: String,
            filterType: GEFIType.Value,
            falsePositiveRate: Double) {

  val catalog = new Catalog(localPath, dbPath, spark)
  val LOG = Logger(LoggerFactory.getLogger(classOf[Store]))

  def create(triplesDataset: Dataset[RDFTriple]): Unit = {
    LOG.info("Identifying Property Based Files ...")
    // Get all unique predicates
    val iter = triplesDataset.select("p").distinct.toLocalIterator
    var fileNames = Map[String, String]()
    var fileCounter = 0

    while (iter.hasNext) {
      fileCounter += 1
      val uri = iter.next.getInt(0).toString
      val fileName = fileCounter.toString
      fileNames += (uri -> fileName)
    }

    val numProperties = fileCounter
    val numTuples = triplesDataset.count

    LOG.debug(s"Number of properties : $numProperties")

    // Catalog information about the data currently ntriple the database
    catalog.dbInfo += ("numProperties" -> numProperties.toString)
    catalog.dbInfo += ("numTuples" -> numTuples.toString)
    catalog.dbInfo += ("dbPath" -> dbPath)

    var count = 0
    val lang: Lang = Lang.NTRIPLES

    for ((uri, tableName) <- fileNames) {
      count += 1
      val data = triplesDataset.filter(col("p").equalTo(uri))
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
    triplesDataset.unpersist()
    catalog.save()
  }

  def create(ntPath: String): Unit = {
    val triplesDataFrame = NTParser.parse(spark, ntPath)
    triplesDataFrame.cache()
    create(triplesDataFrame)
  }

  def close(): Unit = {
    catalog.dictionary.close()
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
