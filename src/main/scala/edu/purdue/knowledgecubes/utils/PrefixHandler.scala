package edu.purdue.knowledgecubes.utils

import java.io.InputStream

import scala.collection.mutable
import scala.collection.mutable.Map

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.typesafe.scalalogging.Logger
import org.apache.jena.graph.{NodeFactory, Triple}
import org.fusesource.leveldbjni.JniDBFactory.{asString, bytes}
import org.slf4j.LoggerFactory

import edu.purdue.knowledgecubes.metadata.Catalog


object PrefixHandler {

  private var jarStream: InputStream = Thread.currentThread.getContextClassLoader.getResourceAsStream("prefixes.json")
  private val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)
  private val parsedJson = mapper.readValue[Map[String, Map[String, String]]](jarStream)
  private val prefix2Uri = parsedJson("@context")
  private val uri2prefix: mutable.Map[String, String] = prefix2Uri.map(_.swap)

  val LOG = Logger(LoggerFactory.getLogger(getClass))

  def parseBaseURI(triple: Triple, catalog: Catalog): Triple = {
    var s = triple.getSubject
    var p = triple.getPredicate
    var o = triple.getObject

    if(triple.getSubject.isURI && triple.getSubject.toString.contains("http://localhost/resource/")) {
      s = NodeFactory.createURI(
        asString(catalog.dictionaryStr2Id.get(
          bytes(triple.getSubject.toString().split("http://localhost/resource/")(1))))
      )
    } else if (triple.getSubject.isURI) {
      s = NodeFactory.createURI(
        asString(catalog.dictionaryStr2Id.get(
          bytes(triple.getSubject.toString)))
      )
    }

    if(triple.getPredicate.isURI && triple.getPredicate.toString.contains("http://localhost/resource/")) {
      p = NodeFactory.createURI(
        asString(catalog.dictionaryStr2Id.get(
          bytes(triple.getPredicate.toString().split("http://localhost/resource/")(1))))
      )
    } else if (triple.getPredicate.isURI) {
      val uriMatch = asString(catalog.dictionaryStr2Id.get(bytes(triple.getPredicate.toString)))
      if(uriMatch == null) {
        LOG.error("Invalid predicate in query")
        sys.exit(-1)
      }
      p = NodeFactory.createURI(uriMatch)
    }
    if(triple.getObject.isURI && triple.getObject.toString.contains("http://localhost/resource/")) {
      o = NodeFactory.createURI(
        asString(catalog.dictionaryStr2Id.get(
          bytes(triple.getObject.toString().split("http://localhost/resource/")(1))))
      )
    } else if (triple.getObject.isURI) {
      o = NodeFactory.createURI(
        asString(catalog.dictionaryStr2Id.get(
          bytes(triple.getObject.toString)))
      )
    }
    new Triple(s, p, o)
  }

}
