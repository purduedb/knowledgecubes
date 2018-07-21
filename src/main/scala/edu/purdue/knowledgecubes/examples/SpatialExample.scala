package edu.purdue.knowledgecubes.examples

import org.apache.jena.sparql.algebra.OpWalker

import edu.purdue.knowledgecubes.queryprocessor.parser.{Parser, QueryVisitor}

object SpatialExample {

  def main(args: Array[String]): Unit = {
    val query = """PREFIX f: <java:edu.purdue.knowledgecubes.queryprocessor.spatial.SpatialFunctions.>
                   PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

                  SELECT ?placeName
                  {
                      ?place rdfs:label ?placeName
                      FILTER f:contains("10","20","10","10","10","10")
                  }"""

    val opRoot = Parser.parse(query)
    val visitor = new QueryVisitor()
    OpWalker.walk(opRoot, visitor)
    println(visitor.spatialFilters(0).getFunctionName(null))
    println(visitor.spatialFilters(0).getArgs)
  }

}
