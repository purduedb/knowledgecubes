package edu.purdue.knowledgecubes.queryprocessor.parser

import org.apache.jena.query.{Query, QueryFactory}
import org.apache.jena.sparql.algebra.{Algebra, Op}

object Parser {

  def parse(queryString: String): Op = {
    val query = QueryFactory.create(queryString, "http://localhost/resource/")
    val opRoot: Op = Algebra.compile(query)
    opRoot
  }

}

