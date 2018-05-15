package edu.purdue.knowledgecubes.metadata

import java.io.{File, IOException, PrintWriter}

import scala.io.Source

import net.jcazevedo.moultingyaml._
import net.jcazevedo.moultingyaml.DefaultYamlProtocol._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

import edu.purdue.knowledgecubes.GEFI.{GEFI, GEFIType}

class Catalog(val localPath: String, val dbPath: String, val spark: SparkSession) {

  val LOG: Logger = LoggerFactory.getLogger(classOf[Catalog])

  val dataPath: String = dbPath + "/data/"
  val joinReductionsPath: String = dbPath + "/reductions/join/"
  var dbInfo: Map[String, String] = Map[String, String]()
  var tablesInfo: Map[String, Map[String, String]] = Map[String, Map[String, String]]()
  var joinReductionsInfo: Map[String, Long] = Map[String, Long]()
  var joinFilters: Map[String, Map[String, GEFI]] = Map[String, Map[String, GEFI]]()
  var broadcastFilters: Broadcast[Map[String, Map[String, GEFI]]] = _
  var filterType: GEFIType.Value = GEFIType.NONE

  def loadConfigurations(): Unit = {
    val dbFile = Source.fromFile(localPath + "/dbinfo.yaml")
    val dbDoc = dbFile.mkString
    dbInfo = dbDoc.parseYaml.convertTo[Map[String, String]]
    LOG.debug(this.dbInfo.toString)

    val reductionsFile = Source.fromFile(localPath + "/join-reductions.yaml")
    val reductionsDoc = reductionsFile.mkString
    joinReductionsInfo = reductionsDoc.parseYaml.convertTo[Map[String, Long]]
    LOG.debug(this.joinReductionsInfo.toString)

    val tablesFile = Source.fromFile(localPath + "/tables.yaml")
    val tablesDoc = tablesFile.mkString
    val docs = tablesDoc.parseYaml
    tablesFile.close

    val yamlDocs = docs.convertTo[Iterable[Map[String, String]]]
    for(doc <- yamlDocs) {
      tablesInfo += (doc("uri") -> doc)
    }
    dbFile.close()
    tablesFile.close()
  }

  def addTable(table: Map[String, String]): Unit = {
    val propertyName = table("uri")
    tablesInfo += (propertyName -> table)
  }

  def save(): Unit = {
    try {
      val directory = new File(localPath)
      if (!directory.exists) {
        directory.mkdirs()
      }
      new PrintWriter(localPath +
                      "/dbinfo.yaml") { write(dbInfo.toYaml.print(flowStyle = Flow)); close() }
      new PrintWriter(localPath +
                      "/tables.yaml") { write(tablesInfo.values.toYaml.print(flowStyle = Block)); close() }
      new PrintWriter(localPath +
                      "/join-reductions.yaml") { write(joinReductionsInfo.toYaml.print(flowStyle = Flow)); close() }
    } catch {
      case exp: IOException =>
        exp.printStackTrace()
    }
  }
}
