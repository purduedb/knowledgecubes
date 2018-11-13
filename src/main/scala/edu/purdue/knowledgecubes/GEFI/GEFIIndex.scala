package edu.purdue.knowledgecubes.GEFI

import java.math.BigInteger
import java.util.function.Supplier

import scala.collection.JavaConverters._
import scala.collection.mutable

import com.typesafe.scalalogging.Logger
import intervalTree.IntervalTree
import org.slf4j.LoggerFactory


class GEFIIndex {

  val LOG = Logger(LoggerFactory.getLogger(getClass))

  val supp: Supplier[BigInteger] = new Supplier[BigInteger]() {
    override def get(): BigInteger = BigInteger.valueOf(0)
  }

  val tree = new IntervalTree[BigInteger, Int](supp)

  def create(intervals: Map[String, Map[String, String]]): Unit = {
    for ((item, doc) <- intervals) {
      val interval = mutable.Map[String, String]()
      tree.addInterval(BigInteger.valueOf(doc("begin").toLong), BigInteger.valueOf(doc("end").toLong), item.toInt)
    }
  }

  def find(start: Long, end: Long): List[Int] = {
    tree.get(BigInteger.valueOf(start), BigInteger.valueOf(end)).asScala.toList
  }
}
