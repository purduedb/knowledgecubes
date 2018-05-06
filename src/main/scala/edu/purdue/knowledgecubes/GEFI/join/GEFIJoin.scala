package edu.purdue.knowledgecubes.GEFI.join

import scala.collection.mutable.{HashSet, ListBuffer, Map => MutableMap}

import com.typesafe.scalalogging.Logger
import org.apache.jena.graph.Triple
import org.apache.spark.sql.Dataset
import org.slf4j.LoggerFactory

import edu.purdue.knowledgecubes.GEFI.GEFI
import edu.purdue.knowledgecubes.metadata.Catalog
import edu.purdue.knowledgecubes.rdf.RDFTriple

class GEFIJoin(catalog: Catalog) {

  val LOG = Logger(LoggerFactory.getLogger(classOf[GEFIJoin]))

  def getString(property: String, variable: String, triple: Triple): String = {
    if (triple.getSubject.isVariable && triple.getSubject.getName == variable) {
      if (triple.getPredicate.isVariable) {
        catalog.tablesInfo(property)("tableName") + "_TRPS"
      } else {
        catalog.tablesInfo(triple.getPredicate.toString)("tableName") + "_TRPS"
      }
    } else {
      if (triple.getPredicate.isVariable) {
        catalog.tablesInfo(property)("tableName") + "_TRPO"
      } else {
        catalog.tablesInfo(triple.getPredicate.toString)("tableName") + "_TRPO"
      }
    }
  }

  def identify(propName: String, triple: Triple, queryJoins: Map[String, HashSet[Triple]]): Map[String, String] = {
    val filters = MutableMap[String, String]()
    val subFilters = ListBuffer[String]()
    val objFilters = ListBuffer[String]()

    if (triple.getSubject.isVariable) {
      subFilters ++= queryJoins(triple.getSubject.getName).map(tr => getString(propName, triple.getSubject.getName, tr))
    }

    if (triple.getObject.isVariable) {
      objFilters ++= queryJoins(triple.getObject.getName).map(tr => getString(propName, triple.getObject.getName, tr))
    }

    if (subFilters.nonEmpty) {
      subFilters -= (catalog.tablesInfo(propName)("tableName") + "_TRPS")
      if(subFilters.nonEmpty) {
        val subList = subFilters.sortWith(_ < _)
        val subjectFilterName = catalog.tablesInfo(propName)("tableName") +
          "_TRPS" + "_JOIN_" + subList.mkString("_JOIN_")
        filters += ("s" -> subjectFilterName)
      }
    }

    if (objFilters.nonEmpty) {
      objFilters -= (catalog.tablesInfo(propName)("tableName") + "_TRPO")
      if(objFilters.nonEmpty) {
        val objList = objFilters.sortWith(_ < _)
        val objectFilterName = catalog.tablesInfo(propName)("tableName") +
          "_TRPO" + "_JOIN_" + objList.mkString("_JOIN_")
        filters += ("o" -> objectFilterName)
      }
    }
    filters.toMap
  }

  def compute(queryProperty: String,
              column: String,
              semanticFilter: String,
              current: Dataset[RDFTriple]): Dataset[RDFTriple] = {
    val all = getBroadcastedFilters(queryProperty, column, semanticFilter)
    val broadcastVariable = catalog.broadcastFilters
    current.filter(value => {
      def foo(value: RDFTriple): Boolean = {
        val filters = ListBuffer[GEFI]()
        for (entry <- all) {
          if (column == "s") {
            if (!broadcastVariable.value.get(entry._1).get(entry._2).contains(value.s)) {
              return false
            }
          } else {
            if (!broadcastVariable.value.get(entry._1).get(entry._2).contains(value.o)) {
              return false
            }
          }
        }
        true
      }
      foo(value)
    })
  }

  def getBroadcastedFilters(exceptPropertyName: String,
                            column: String,
                            semanticFilter: String): ListBuffer[(String, String)] = {
    var trpTag = ""
    if (column == "s") {
      trpTag = "_TRPS"
    } else {
      trpTag = "_TRPO"
    }

    LOG.debug("Semantic GEFI : " + semanticFilter)

    var filters = semanticFilter.split("\\_JOIN\\_").to[ListBuffer]

    LOG.debug("Filters before : ")
    LOG.debug(filters.mkString(" & "))
    LOG.debug("Except : " + exceptPropertyName + trpTag)
    filters -= (exceptPropertyName + trpTag)
    LOG.debug(filters.mkString(" & "))
    var all = ListBuffer[(String, String)]()

    for (bfName <- filters) {
      // Determine the column name of matching partitions that join with the current partition/property
      val parts = bfName.split("\\_TRP")
      val name = parts(0).split("\\_\\s+")(0)
      val col = parts(1)

      val tableName = name
      // determine the column of the other property and add to appropriate list
      if (col.equalsIgnoreCase("s")) {
        all += ((tableName, "s"))
      } else {
        all += ((tableName, "o"))
      }
    }
    all
  }
}
