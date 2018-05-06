package edu.purdue.knowledgecubes.queryprocessor.optimizer

import com.typesafe.scalalogging.Logger
import org.apache.jena.sparql.algebra.Op
import org.slf4j.LoggerFactory

import edu.purdue.knowledgecubes.metadata.Catalog

class Optimizer(catalog: Catalog) {

  val LOG = Logger(LoggerFactory.getLogger(classOf[Optimizer]))

  if (catalog.tablesInfo.isEmpty) {
    LOG.error("Unable to find statistics - Optimizer not operational")
    System.exit(-1)
  }

  def noOp(opRoot: Op): Op = opRoot

  def usingStatistics(opRoot: Op): Op = new BGPOptimizer(catalog).optimize(opRoot)

  def noStats(opRoot: Op): Op = BGPOptimizerNoStats.optimize(opRoot)
}
