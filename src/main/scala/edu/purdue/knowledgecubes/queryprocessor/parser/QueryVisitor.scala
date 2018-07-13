package edu.purdue.knowledgecubes.queryprocessor.parser

import scala.collection.JavaConverters._
import scala.collection.mutable.{HashSet, ListBuffer, Map}

import org.apache.jena.graph.Triple
import org.apache.jena.sparql.algebra.{Op, OpVars, OpVisitorBase}
import org.apache.jena.sparql.algebra.op.{OpBGP, OpProject, OpTriple}

class QueryVisitor extends OpVisitorBase {

  var triples: ListBuffer[Triple] = ListBuffer[Triple]()
  var joinVariables: Map[String, HashSet[Triple]] = Map[String, HashSet[Triple]]()
  var properties: ListBuffer[String] = ListBuffer[String]()
  var unboundPropertyTriples : ListBuffer[Triple] = ListBuffer[Triple]()
  var projectionList: ListBuffer[String] = ListBuffer[String]()

  override def visit(opBGP: OpBGP): Unit = {
    // Get BGP and process triples
    val p = opBGP.getPattern
    for (triple <- p.getList.asScala) {
      triples += triple
      // Check Subject
      val subject = triple.getSubject
      if (subject.isVariable) {
        if (joinVariables.contains(subject.getName)) {
          joinVariables(subject.getName) += triple
        } else {
          val newList = HashSet[Triple]()
          newList += triple
          joinVariables += (subject.getName -> newList)
        }
      }
      // Check Predicate
      val prop = triple.getPredicate
      if (prop.isURI) {
        val pred = triple.getPredicate.toString
        properties += pred
      } else {
        unboundPropertyTriples += triple
      }
      // Check Object
      val obj = triple.getObject
      if (obj.isVariable) {
        if (joinVariables.contains(obj.getName)) {
          joinVariables(obj.getName) += triple
        } else {
          val newList = new HashSet[Triple]()
          newList += triple
          joinVariables += (obj.getName -> newList)
        }
      }
    }
  }

  override def visit(opProject: OpProject): Unit = {
      for (v <- opProject.getVars.asScala) {
        projectionList += v.toString()
      }
  }

  def computeProjectionList(op: Op): ListBuffer[String] = {
    if (projectionList.isEmpty) {
      val res = OpVars.mentionedVars(op)
      val it = res.iterator()
      while(it.hasNext) {
        projectionList += it.next().toString()
      }
    }
    projectionList
  }

  override def visit(opTriple: OpTriple): Unit = {
  }
}
