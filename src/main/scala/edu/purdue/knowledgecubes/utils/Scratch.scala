package edu.purdue.knowledgecubes.utils

import java.math.BigInteger
import java.util.function.Supplier

import intervalTree.IntervalTree

object Scratch {

  def main(args: Array[String]): Unit = {
    val supp: Supplier[BigInteger] = new Supplier[BigInteger]() {
      override def get(): BigInteger = BigInteger.valueOf(0)
    }
    val tree = new IntervalTree[BigInteger, BigInteger](supp)
    tree.addInterval(BigInteger.valueOf(1), BigInteger.valueOf(5), BigInteger.valueOf(4))
    tree.addInterval(BigInteger.valueOf(3), BigInteger.valueOf(7), BigInteger.valueOf(5))
    println(tree.get(BigInteger.valueOf(4)))
  }
}
