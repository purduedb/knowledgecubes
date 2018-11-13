package edu.purdue.knowledgecubes.GEFI.spatial

import java.io.PrintWriter
import java.math.BigInteger
import java.util.function.Supplier

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import ch.hsr.geohash.GeoHash
import com.google.common.geometry._
import com.typesafe.scalalogging.Logger
import intervalTree.IntervalTree
import net.jcazevedo.moultingyaml._
import net.jcazevedo.moultingyaml.DefaultYamlProtocol._
import org.slf4j.LoggerFactory

import ch.hsr.geohash.WGS84Point

import edu.purdue.knowledgecubes.QueryCLI.getClass

object SpatialEncoder {

  val LOG = Logger(LoggerFactory.getLogger(getClass))

  def encodeLonLat(lon: Double, lat: Double, level: Int): Long = {
    var latlng = S2LatLng.fromDegrees(lat, lon)
    var cell = S2CellId.fromLatLng(latlng)
    var parentId = cell.parent(level).id()
    parentId
  }

  def encodeLonLat(lon: Double, lat: Double): Long = {
    GeoHash.withBitPrecision(lat, lon, 64).longValue()
  }

//  def encodeLonLat(lon: Double, lat: Double): Long = {
//    val latlng = S2LatLng.fromDegrees(lat, lon)
//    val id = S2CellId.fromLatLng(latlng).id()
//    id
//  }

  def encodePoints(list: List[(Double, Double)]): List[Long] = {
    val loop = ListBuffer[S2Point]()
    for (entry <- list) {
      val point = new S2Point(entry._2, entry._1, 0)
      loop += point
    }
    val s2Loop = new S2Loop(loop.asJava)
    val coverer = new S2RegionCoverer()
    val union = coverer.getCovering(s2Loop)
    val matchingCells = union.cellIds().asScala
    val cells = ListBuffer[Long]()
    for(c <- matchingCells) {
      val id = c.id()
      cells += id
    }
    cells.toList
  }

  def createFilters(encodedPoints: Map[Long, ListBuffer[Int]],
                    numElements: Int,
                    localPath: String): Map[Int, List[Int]] = {
    var intervals = mutable.Map[String, Map[String, String]]()
    val sorted = encodedPoints.keySet.toArray.sortWith(_ < _)
    val bins = sorted.grouped(numElements)

    val supp: Supplier[BigInteger] = new Supplier[BigInteger]() {
      override def get(): BigInteger = BigInteger.valueOf(0)
    }

    var counter = 0
    val tree = new IntervalTree[BigInteger, Int](supp)
    val filters = mutable.Map[Int, List[Int]]()
    while(bins.hasNext) {
      val lst = bins.next()
      val intervalInfo = mutable.Map[String, String]()
      counter += 1
      tree.addInterval(BigInteger.valueOf(lst.head), BigInteger.valueOf(lst.last), counter)
      filters += (counter -> lst.flatMap(x => encodedPoints(x).toList).toList)
      // Save intervalInfo info for reconstruction
      intervalInfo += ("id" -> counter.toString)
      intervalInfo += ("begin" -> lst.head.toString)
      intervalInfo += ("end" -> lst.last.toString)
      intervals += (counter.toString -> intervalInfo.toMap)
    }

    new PrintWriter(localPath +
      "/spatial-intervals.yaml") { write(intervals.values.toYaml.print(flowStyle = Block)); close() }

    filters.toMap
  }
}
