package edu.purdue.knowledgecubes.queryprocessor.optimizer

import org.apache.jena.sparql.algebra.{Op, TransformCopy, Transformer}
import org.apache.jena.sparql.algebra.op.OpBGP
import org.apache.jena.sparql.engine.optimizer.reorder.{ReorderFixed, ReorderLib}


object BGPOptimizerNoStats extends TransformCopy {

  def optimize(op: Op): Op = Transformer.transform(this, op)

  override def transform(opBGP: OpBGP): Op = {

    if (opBGP.getPattern.size <= 2) {
      return opBGP
    }
    // Reorder by Selectivity
    val optimizer1 = ReorderLib.fixed.asInstanceOf[ReorderFixed]
    val optimizedPattern1 = optimizer1.reorder(opBGP.getPattern)
    // Reorder to avoid cross products and reduce the number of joins, if possible
    val optimizer2 = new ReorderNoCross()
    val optimizedPattern2 = optimizer2.reorder(optimizedPattern1)
    val optimizedBGP = new OpBGP(optimizedPattern2)
    optimizedBGP
  }
}
