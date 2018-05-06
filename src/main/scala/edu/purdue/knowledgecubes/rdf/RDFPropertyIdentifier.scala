package edu.purdue.knowledgecubes.rdf

import scala.collection.mutable.{HashSet, ListBuffer}

import org.apache.jena.graph.Triple
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.col

import edu.purdue.knowledgecubes.metadata.Catalog
import edu.purdue.knowledgecubes.partition.Partition
import edu.purdue.knowledgecubes.storage.cache.{CacheEntry, CacheManager}
import edu.purdue.knowledgecubes.storage.cache.CacheEntryType._


class RDFPropertyIdentifier(catalog: Catalog) {

  import catalog.spark.implicits._

  def identify(triplePattern: Triple, queryJoins: Map[String, HashSet[Triple]]): List[String] = {
    val candidateProperties = identifyUnboundProperties(triplePattern)
    val verifiedProperties = verifyUnboundProperties(triplePattern, candidateProperties)
    verifiedProperties
  }

  def identifyUnboundProperties(triple: Triple): List[String] = {
    val joinFilters = catalog.joinFilters
    val subIdentifiedProperties = HashSet[String]()
    val objIdentifiedProperties = HashSet[String]()
    val filterNames = joinFilters.keySet

    val sub = triple.getSubject.toString
    val obj = triple.getObject.toString

    for (p <- filterNames) {
      if (!triple.getSubject.isVariable) {
        val f = joinFilters(p)("s")
        val subExists = f.contains(sub.toInt)
        if (subExists) {
          subIdentifiedProperties.add(p)
        }
      }
      if (!triple.getObject.isVariable) {
        val f = joinFilters(p)("o")
        val objExists: Boolean = f.contains(obj.toInt)
        if (objExists) {
          objIdentifiedProperties.add(p)
        }
      }
    }

    if (subIdentifiedProperties.nonEmpty && objIdentifiedProperties.nonEmpty) {
      subIdentifiedProperties.retain(objIdentifiedProperties)
      subIdentifiedProperties.toList
    } else if (subIdentifiedProperties.nonEmpty) {
      subIdentifiedProperties.toList
    } else {
      objIdentifiedProperties.toList
    }
  }

  private def verifyUnboundProperties(triple: Triple, identifiedProperties: List[String]): List[String] = {
    val verifiedList = new ListBuffer[String]
    val sub = triple.getSubject.toString
    val obj = triple.getObject.toString

    val spark = catalog.spark

    // Loading & Verification step
    if (identifiedProperties.isEmpty) {
      verifiedList ++= catalog.tablesInfo.keySet
      for (pr <- verifiedList) {
        if (!CacheManager.contains(pr)) {
          var table = spark.read.parquet(catalog.dataPath + catalog.tablesInfo(pr)("tableName")).as[RDFTriple]
          table = Partition.byDefaultCriteria(table)
          table.cache()
          val numTuples = catalog.tablesInfo(pr)("numTuples").toString.toLong
          CacheManager.add(new CacheEntry(pr, numTuples, ORIGINAL, table))
        }
      }
    } else {
      for (pr <- identifiedProperties) {
        var table : Dataset[RDFTriple] = spark.emptyDataset[RDFTriple]
        if (!CacheManager.contains(pr)) {
          table = spark.read.parquet(catalog.dataPath + catalog.tablesInfo(pr)("tableName")).as[RDFTriple]
          table.cache()
          val numTuples = catalog.tablesInfo(pr)("numTuples").toString.toLong
          CacheManager.add(new CacheEntry(pr, numTuples, ORIGINAL, table))
        } else {
          table = CacheManager.get(pr).data
        }

        if (!triple.getSubject.isVariable && !triple.getObject.isVariable) {
          table = table.filter(col("s").equalTo(sub).and(col("o").equalTo(obj)))
        } else if (!triple.getSubject.isVariable && triple.getObject.isVariable) {
          table = table.filter(col("s").equalTo(sub))
        } else {
          table = table.filter(col("o").equalTo(obj))
        }
        table.cache()
        val size = table.count

        if (size > 0) {
          verifiedList += pr
        }
      }
    }
    verifiedList.toList
  }
}
