package edu.purdue.knowledgecubes.utils

import scala.collection.mutable.Map

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.apache.jena.graph.{NodeFactory, Triple}


object PrefixHandler {

  var jarStream = Thread.currentThread.getContextClassLoader.getResourceAsStream("prefixes.json")
  private val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)
  private val parsedJson = mapper.readValue[Map[String, Map[String, String]]](jarStream)

  val prefix2Uri = parsedJson("@context")
  val uri2prefix = prefix2Uri.map(_.swap)

  /**
    * Return a prefixed String representing a resource and its prefix URI
    * @param uri resource
    * @return prefixed URI
    */
  def parseURI(uri: String): String = {
    var parentURI = ""
    var resource = ""
    var prefixed = ""
    if (uri.indexOf("#") > -1) {
      parentURI = uri.substring(0, uri.lastIndexOf("#") + 1)
      resource = uri.substring(uri.lastIndexOf("#") + 1, uri.length)
    } else {
      parentURI = uri.substring(0, uri.lastIndexOf("/") + 1)
      resource = uri.substring(uri.lastIndexOf("/") + 1, uri.length)
    }
    prefixed = PrefixHandler.uri2prefix(parentURI)
    prefixed += "__" + resource

    return prefixed
  }

  def parseBaseURI(triple: Triple): Triple = {
    var s = triple.getSubject
    var p = triple.getPredicate
    var o = triple.getObject

    if(triple.getSubject.isURI && triple.getSubject.toString.contains("http://localhost/resource/")) {
      s = NodeFactory.createURI(triple.getSubject.toString().split("http://localhost/resource/")(1))
    }
    if(triple.getPredicate.isURI && triple.getPredicate.toString.contains("http://localhost/resource/")) {
      p = NodeFactory.createURI(triple.getPredicate.toString().split("http://localhost/resource/")(1))
    }
    if(triple.getObject.isURI && triple.getObject.toString.contains("http://localhost/resource/")) {
      o = NodeFactory.createURI(triple.getObject.toString().split("http://localhost/resource/")(1))
    }
    new Triple(s, p, o)
  }

  def expandURI(prefixedString: String): String = {
    val parts = prefixedString.split("__")
    val uri = PrefixHandler.prefix2Uri(parts(0))
    return uri + parts(1)
  }

}
