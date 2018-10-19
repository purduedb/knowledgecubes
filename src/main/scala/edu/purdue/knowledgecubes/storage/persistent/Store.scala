package edu.purdue.knowledgecubes.storage.persistent

import scala.collection.mutable.{ListBuffer, Map}

import com.typesafe.scalalogging.Logger
import org.apache.jena.riot.Lang
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.apache.spark.sql.functions.col
import org.fusesource.leveldbjni.JniDBFactory.{asString, bytes}
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

    var uris = ListBuffer[String]()
    var uriCounter = 0
    while (iter.hasNext) {
      val uri = iter.next.getInt(0).toString
      uris += uri
      uriCounter += 1
    }

    val numProperties = uriCounter
    val numTuples = triplesDataset.count

    LOG.debug(s"Number of properties : $numProperties")

    // Catalog information about the data currently ntriple the database
    catalog.dbInfo += ("numProperties" -> numProperties.toString)
    catalog.dbInfo += ("numTuples" -> numTuples.toString)
    catalog.dbInfo += ("dbPath" -> dbPath)

    var count = 0
    val lang: Lang = Lang.NTRIPLES

    for (uri <- uris) {
      count += 1
      val data = triplesDataset.filter(col("p").equalTo(uri))
      data.cache()
      val size = data.count()
      // Statistics
      val propTable = Map[String, String]()
      val predicateName = asString(catalog.dictionaryId2Str.get(bytes(uri)))
      propTable += ("uri" -> uri)
      propTable += ("predicate" -> predicateName)
      propTable += ("numTuples" -> size.toString)
      propTable += ("ratio" -> (size.toFloat / numTuples.toFloat).toString)

      val subData = data.select(col("s")).distinct()
      val uniqSub = subData.count()
      propTable +=  ("unique_s" -> uniqSub.toString)

      val objData = data.select(col("o")).distinct()
      val uniqObj = objData.count()
      propTable +=  ("unique_o" -> uniqObj.toString)

      data.write.mode(SaveMode.Overwrite).parquet(catalog.dataPath + uri)
      LOG.info(s"Processed ($count/$numProperties) : $predicateName - File: $uri ($size)")
      catalog.addTable(propTable.toMap)

      if (!filterType.equals(GEFIType.NONE)) {
        val filteredObjData = objData.where(!col("o").startsWith("\""))
        GEFIJoinUtils.create(filterType, uniqSub, falsePositiveRate, subData, "s", uri, catalog.localPath)
        GEFIJoinUtils.create(filterType, uniqObj, falsePositiveRate, filteredObjData, "o", uri, catalog.localPath)
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
    catalog.dictionaryStr2Id.close()
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
