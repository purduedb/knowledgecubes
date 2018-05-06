package edu.purdue.knowledgecubes.GEFI.join

import java.io._

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StringType

import edu.purdue.knowledgecubes.GEFI.{GEFI, GEFIType}


object GEFIJoinUtils {

  def create(filterType: GEFIType.Value,
             size: Long,
             falsePositiveRate: Double,
             data: DataFrame,
             colName: String,
             name: String,
             path: String): Unit = {
    var filterSize = size
    if (filterType == GEFIType.CUCKOO) {
      if (size < 5) {
        filterSize = 10
      }
    }
    var newFilter = new GEFI(filterType, filterSize, falsePositiveRate)

    if(filterType == GEFIType.CUCKOO) {
      val colType = data.schema.head.dataType
      val updateFilter: (GEFI, InternalRow) => Unit = colType match {
        case StringType => (filter, row) => filter.add(row.getInt(0))
        case _ =>
          throw new IllegalArgumentException(
            s"Datatype supported is String only"
          )
      }
      val zero = new GEFI(filterType, filterSize, falsePositiveRate)
      newFilter = data.queryExecution.toRdd.treeAggregate(zero)(
        (filter: GEFI, row: InternalRow) => {
          updateFilter(filter, row)
          filter
        },
        (filter1, filter2) => filter1.union(filter2)
      )
    } else {
      val iter = data.toLocalIterator()
      while (iter.hasNext) {
        newFilter.add(iter.next.getInt(0))
      }
    }
    save(filterType, falsePositiveRate, newFilter, colName + "_" + name, path)
  }

  private def save(filterType: GEFIType.Value,
                   falsePositiveRate: Double,
                   filter: GEFI,
                   filterName: String,
                   path: String): Unit = {
    try {
      val fullPath = path + "GEFI/join/" + filterType.toString + "/" + falsePositiveRate.toString
      val directory = new File(fullPath)
      if (!directory.exists) directory.mkdirs()
      val fout = new FileOutputStream(fullPath + "/" + filterName)
      val oos = new ObjectOutputStream(fout)
      oos.writeObject(filter)
      oos.close()
    } catch {
      case e: FileNotFoundException =>
        e.printStackTrace()
      case e: IOException =>
        e.printStackTrace()
    }
  }
}
