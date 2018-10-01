package edu.purdue.knowledgecubes.GEFI.join

import scala.collection.mutable.Map

import com.typesafe.scalalogging.Logger
import org.apache.jena.riot.Lang
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.slf4j.LoggerFactory

import edu.purdue.knowledgecubes.GEFI.GEFIType
import edu.purdue.knowledgecubes.metadata.Catalog

class GEFIJoinCreator(spark: SparkSession, dbPath: String, localPath: String) {

  val LOG = Logger(LoggerFactory.getLogger(classOf[GEFIJoinCreator]))
  val catalog = new Catalog(localPath, dbPath, spark)
  catalog.loadConfigurations()

  def create(filterType: GEFIType.Value, falsePositiveRate: Double): Unit = {
    var fileNames = Map[String, String]()
    val propertyList = catalog.tablesInfo.keySet

    for (uri <- propertyList) {
      val fileName = catalog.tablesInfo(uri)("tableName").toString
      fileNames += (uri -> fileName)
    }

    var count = 0
    val lang: Lang = Lang.NTRIPLES

    for ((uri, fileName) <- fileNames) {
      count += 1
      val data = spark.read.parquet(catalog.dataPath + fileName)
      data.cache()
      val subData = data.select(col("s")).distinct()
      val uniqSub = subData.count()
      GEFIJoinUtils.create(filterType, uniqSub, falsePositiveRate, subData, "s", fileName, catalog.localPath)
      val objData = data.select(col("o")).distinct()
      val uniqObj = objData.count()
      GEFIJoinUtils.create(filterType, uniqObj, falsePositiveRate, objData, "o", fileName, catalog.localPath)
      LOG.info(s"Processed ($count/${fileNames.size}) : $uri - File: $fileName")
      data.unpersist()
    }
  }

  def close(): Unit = {
    spark.sqlContext.clearCache()
  }
}
