package edu.purdue.knowledgecubes.queryprocessor.optimizer

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._

import com.typesafe.scalalogging.Logger
import java.util
import org.apache.jena.graph.Triple
import org.apache.jena.sparql.algebra.{Op, TransformCopy, Transformer}
import org.apache.jena.sparql.algebra.op.OpBGP
import org.apache.jena.sparql.core.BasicPattern
import org.slf4j.LoggerFactory

import edu.purdue.knowledgecubes.metadata.Catalog
import edu.purdue.knowledgecubes.utils.PrefixHandler



class BGPOptimizer(catalog: Catalog) extends TransformCopy {

  val LOG = Logger(LoggerFactory.getLogger(classOf[BGPOptimizer]))

  def optimize(op: Op): Op = Transformer.transform(this, op)

  override def transform(opBGP: OpBGP): Op = {
    if (opBGP.getPattern.size <= 2) {
      return opBGP
    }
    val p = opBGP.getPattern
    val properties = ListBuffer[String]()

    for (t <- p.getList.asScala) {
      val propName = PrefixHandler.parseBaseURI(t).getPredicate.toString
      properties += propName
    }

    val sortedProperties = sortByPropertySelectivity(properties)
    val reorderedTriples = new util.ArrayList[Triple]()
    val sortedTriples = new util.ArrayList[Triple]()
    val numTriples = opBGP.getPattern.size

    for (prop <- sortedProperties) {
      for (t <- p.getList.asScala) {
        if (PrefixHandler.parseBaseURI(t).getPredicate.toString == prop) {
          sortedTriples.add(t)
        }
      }
    }

    // Re-sort so that the first triple be one with a literal
    val tpl = getTripleWithLiteral(sortedTriples)

    if (tpl.isDefined) {
      sortedTriples.remove(tpl.get)
      sortedTriples.add(0, tpl.get)
    }

    val firstTriple = sortedTriples.get(0)
    val joinVars = mutable.HashSet[String]()
    val literalTriples = ListBuffer[Triple]()

    if (firstTriple.getSubject.isVariable) {
      joinVars.add(firstTriple.getSubject.toString)
    } else {
      literalTriples += firstTriple
    }

    if (firstTriple.getObject.isVariable) {
      joinVars.add(firstTriple.getObject.toString())
    } else {
      literalTriples += firstTriple
    }

    reorderedTriples.add(sortedTriples.remove(0))

    while (reorderedTriples.size != numTriples) {
      var joinedTriples = ListBuffer[Triple]()
      var literalTriples = ListBuffer[Triple]()
      var joinFound = false
      for (i <- 0 until sortedTriples.size) {
        var t2 = sortedTriples.get(i)
        var subJoins = false
        var objJoins = false

        if (t2.getSubject.isVariable && joinVars.contains(t2.getSubject.toString)) {
          subJoins = true
        } else if (t2.getSubject.isVariable) {
          if (!joinFound) {
            joinVars.add(t2.getSubject.toString)
          }
        } else {
          if (!literalTriples.contains(t2)) {
            literalTriples += t2
          }
        }

        if (t2.getObject.isVariable && joinVars.contains(t2.getObject.toString)) {
          objJoins = true
        } else if (t2.getObject.isVariable) {
          if (!joinFound) {
            joinVars.add(t2.getObject.toString)
          }
        } else {
          if (!literalTriples.contains(t2)) {
            literalTriples += t2
          }
        }

        if (!joinFound && (subJoins || objJoins)) {
          joinedTriples += t2
          joinFound = true
        }

        if (!subJoins && !objJoins) {
          joinVars.remove(t2.getSubject.toString)
          joinVars.remove(t2.getObject.toString)
          literalTriples -= t2
        }
      }

      if (joinedTriples.nonEmpty) {
        breakable {
          for (t <- joinedTriples) { // Add first triple that joins (to avoid cross-joins by literal-triples)
            if (!literalTriples.contains(t)) {
              sortedTriples.remove(t)
              reorderedTriples.add(t)
              break
            }
          }
        }
        if (literalTriples.nonEmpty) {
          for (i <- literalTriples.indices) { // first one is smallest (add to beginning of the list)
            // sortedTriples.remove(literalTriples(i))
            var added = false
            breakable {
              for (j <- 0 until reorderedTriples.size) {
                if (reorderedTriples.get(j).getSubject.toString().equals(literalTriples(i).getSubject.toString()) ||
                  reorderedTriples.get(j).getSubject.toString().equals(literalTriples(i).getObject.toString()) ||
                  reorderedTriples.get(j).getObject.toString().equals(literalTriples(i).getSubject.toString()) ||
                  reorderedTriples.get(j).getObject.toString().equals(literalTriples(i).getObject.toString())) {

                  if (j == 0) {
                    sortedTriples.remove(literalTriples(i))
                    reorderedTriples.add(0, literalTriples(i))
                    added = true
                    break
                  } else if ((j - 1 >= 0) &&
                    (reorderedTriples.get(j - 1).getSubject.equals(literalTriples(i).getSubject)
                    || reorderedTriples.get(j - 1).getSubject.equals(literalTriples(i).getObject)
                    || reorderedTriples.get(j - 1).getObject.equals(literalTriples(i).getSubject)
                    || reorderedTriples.get(j - 1).getObject.equals(literalTriples(i).getObject))) {
                    sortedTriples.remove(literalTriples(i))
                    reorderedTriples.add(j, literalTriples(i))
                    added = true
                    break
                  }
                }
              }
            }
            if (!added) {
              sortedTriples.remove(literalTriples(i))
              reorderedTriples.add(literalTriples(i))
            }
          }
        }
      } else {
        while (sortedTriples.size > 0) {
          reorderedTriples.add(sortedTriples.remove(0))
        }
      }
    }
    val res = new BasicPattern()
    for (t <- reorderedTriples.asScala) {
      res.add(t)
    }
    new OpBGP(res)
  }

  def getTripleWithLiteral(sortedTriples: util.ArrayList[Triple]): Option[Triple] = {
    for (t <- sortedTriples.asScala) {
      if (!t.getSubject.isVariable || !t.getObject.isVariable) {
        // TODO: Smallest with literal or largest with literal ?
        val tpl = t
        return Option(tpl)
      }
    }
    None
  }

  def sortByPropertySelectivity(candidates: ListBuffer[String]): List[String] = {
    var sortedMatches = ListBuffer[String]()
    var propCount = mutable.Map[String, Integer]()
    for (uri <- candidates) {
      val count = catalog.tablesInfo(uri)("numTuples").toInt
      propCount += (uri -> count)
    }
    val res = mutable.ListMap(propCount.toSeq.sortBy(_._2): _*)
    for (entry <- res) {
      sortedMatches += entry._1
    }
    sortedMatches.toList
  }

}
