package edu.purdue.knowledgecubes.GEFI.spatial

import java.io.{File, FileOutputStream, ObjectOutputStream, PrintWriter}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._

import com.google.common.geometry._
import net.jcazevedo.moultingyaml.Flow
import net.jcazevedo.moultingyaml._
import net.jcazevedo.moultingyaml.DefaultYamlProtocol._


import edu.purdue.knowledgecubes.GEFI.{GEFI, GEFIType}

object SpatialEncoder {

  def encodeLatLon(lon: Double, lat: Double): Long = {
    val lvl = 3
    var latlng = S2LatLng.fromDegrees(lat, lon)
    var cell = S2CellId.fromLatLng(latlng)
    var parentId = cell.parent(lvl).id()
    parentId
  }

  def save(resources: mutable.Map[Long, ListBuffer[Int]],
           filterType: GEFIType.Value,
           falsePositiveRate: Float,
           localPath: String): Int = {
    var sfConf = Map[Int, Long]()
    var counter = 0

    for ((k, v) <- resources) {
      counter += 1
      sfConf += (counter -> k)
      val filterName = counter
      val filterSize = v.size
      val filter = new GEFI(filterType, filterSize, falsePositiveRate)

      for (value <- v) {
        filter.add(value)
      }
      val fullPath = localPath + "/GEFI/spatial/" + filterType.toString + "/" + falsePositiveRate.toString
      val directory = new File(fullPath)
      if (!directory.exists) directory.mkdirs()
      val fout = new FileOutputStream(fullPath + "/" + filterName)
      val oos = new ObjectOutputStream(fout)
      oos.writeObject(filter)
      oos.close()
    }
    // Save resources information
    val confFile = localPath + "/spatial-resources.yaml"
    new PrintWriter(confFile) {
      write(sfConf.toYaml.print(flowStyle = Flow)); close()
    }
    counter
  }
}
