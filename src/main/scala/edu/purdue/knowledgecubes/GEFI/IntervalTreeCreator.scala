package edu.purdue.knowledgecubes.GEFI

import java.math.BigInteger
import java.util.function.Supplier

import intervalTree.IntervalTree

object IntervalTreeCreator {

  def create(lst: List[Long], theshold: Int): IntervalTree[BigInteger, Int] = {
    val sorted = lst.sortWith(_ < _)
    val bins = sorted.grouped(theshold)

    val supp: Supplier[BigInteger] = new Supplier[BigInteger]() {
      override def get(): BigInteger = BigInteger.valueOf(0)
    }

    var counter = 0
    val tree = new IntervalTree[BigInteger, Int](supp)

    while(bins.hasNext) {
      counter += 1
      val lst = bins.next()
      tree.addInterval(BigInteger.valueOf(lst.head), BigInteger.valueOf(lst.last), counter)
    }
    tree
  }

}
