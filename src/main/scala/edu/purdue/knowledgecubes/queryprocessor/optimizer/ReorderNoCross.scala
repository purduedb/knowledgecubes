package edu.purdue.knowledgecubes.queryprocessor.optimizer

import scala.collection.mutable.ListBuffer

import org.apache.jena.graph.Triple
import org.apache.jena.sparql.core.BasicPattern


class ReorderNoCross() {

  private var joinSchema = ListBuffer[String]()
  private var lastJoinVars = ListBuffer[String]()
  private var outputPattern = new BasicPattern()
  private var inputPattern = new BasicPattern()

  def reorder(pattern: BasicPattern): BasicPattern = {
    inputPattern = pattern
    var triples = inputPattern.getList
    var idx = chooseFirst
    var triple = triples.get(idx)
    outputPattern.add(triple)
    joinSchema ++= getVarsOfTriple(triple)
    triples.remove(idx)
    while (!triples.isEmpty) {
      idx = chooseNext
      triple = triples.get(idx)
      outputPattern.add(triple)
      joinSchema ++= getVarsOfTriple(triple)
      triples.remove(idx)
    }
    outputPattern
  }

  private def chooseNext: Int = {
    var tripleVars = List[String]()
    var sharedVars = Set[String]()

    for(i <- 0 until inputPattern.size) {
      tripleVars = getVarsOfTriple(inputPattern.get(i))
      sharedVars = getSharedVars(joinSchema.toList, tripleVars)
      if (lastJoinVars.nonEmpty &&
        lastJoinVars.size == sharedVars.size &&
        sharedVars.subsetOf(lastJoinVars.toSet)) { // lastJoinVars remain unchanged
        return i
      }
    }

    for( i <- 0 until inputPattern.size) {
      tripleVars = getVarsOfTriple(inputPattern.get(i))
      sharedVars = getSharedVars(joinSchema.toList, tripleVars)
      if (sharedVars.nonEmpty) {
        lastJoinVars.clear()
        lastJoinVars ++= sharedVars
        return i
      }
    }
    lastJoinVars.clear()
    0
  }

  private def chooseFirst: Int = {
    for ( i <- 0 until inputPattern.size) {
      if (hasSharedVars(i)) {
        return i
      }
    }
    0
  }

  private def hasSharedVars(triplePos: Int): Boolean = {
    val triple = inputPattern.get(triplePos)
    val tripleVars = getVarsOfTriple(triple)

    for( i <- 0 until inputPattern.size) {
      if (i != triplePos && getSharedVars(getVarsOfTriple(inputPattern.get(i)), tripleVars).nonEmpty) {
        return true
      }
    }
    false
  }

  private def getVarsOfTriple(t: Triple): List[String] = {
    var vars = ListBuffer[String]()
    val sub = t.getSubject
    val pred = t.getPredicate
    val obj = t.getObject
    if (sub.isVariable) {
      vars += sub.getName
    }
    if (pred.isVariable) {
      vars += pred.getName
    }
    if (obj.isVariable) {
      vars += obj.getName
    }
    vars.toList
  }

  private def getSharedVars(leftSchema: List[String], rightSchema: List[String]): Set[String] = {
    var sharedVars = Set[String]()
    for (i <- rightSchema.indices) {
      if (leftSchema.contains(rightSchema(i))) {
        sharedVars += rightSchema(i)
      }
    }
    sharedVars
  }
}
