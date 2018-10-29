package edu.purdue.knowledgecubes.utils

import java.io.{FileWriter, PrintWriter}

import scala.io.Source

object LGDPredicateSelector {

  def main(args: Array[String]): Unit = {
    val predicates = List(
      "<http://linkedgeodata.org/ontology/type>",
      "<http://www.w3.org/2000/01/rdf-schema#label>",
      "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
      "<http://linkedgeodata.org/ontology/name%3Aen>",
      "<http://www.opengis.net/ont/geosparql#asWKT>",
      "<http://linkedgeodata.org/ontology/label>")

    val inputFile = args(0)
    val outputFile = args(1)

    val printer = new PrintWriter(new FileWriter(outputFile))

    for (line <- Source.fromFile(inputFile).getLines) {
      val parts = line.split(" ")
      if (predicates.contains(parts(1))) {
        printer.println(line)
      }
    }
    printer.close()
  }
}
