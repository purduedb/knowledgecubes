package edu.purdue.knowledgecubes.GEFI

import scala.collection.mutable

import orestes.bloomfilter.{BloomFilter, FilterBuilder}
import orestes.bloomfilter.HashProvider.HashMethod
import org.roaringbitmap.RoaringBitmap

@SerialVersionUID(7157454730724211874L)
class GEFI(val filterType: GEFIType.Value, val size: Long, val falsePositiveRate: Double) extends Serializable {

  var filter: Any = _

  if(filterType == GEFIType.BLOOM) {
    filter = new FilterBuilder()
      .expectedElements(size.toInt)
      .falsePositiveProbability(falsePositiveRate)
      .hashFunction(HashMethod.Murmur3)
      .buildBloomFilter()
  } else if (filterType == GEFIType.ROARING) {
    filter = new RoaringBitmap()
  } else if (filterType == GEFIType.BITSET) {
    filter = new mutable.BitSet()
  }

  def add(elem: Integer): Unit = {
    if (filterType == GEFIType.BLOOM) {
      filter.asInstanceOf[BloomFilter[Integer]].add(elem)
    } else if (filterType == GEFIType.ROARING) {
      filter.asInstanceOf[RoaringBitmap].add(elem)
    } else if (filterType == GEFIType.BITSET) {
      filter.asInstanceOf[mutable.BitSet] += elem
    }
  }

  def contains(elem: Integer): Boolean = {
    if (filterType == GEFIType.BLOOM) {
      filter.asInstanceOf[BloomFilter[Integer]].contains(elem)
    } else if (filterType == GEFIType.ROARING) {
      filter.asInstanceOf[RoaringBitmap].contains(elem)
    } else if (filterType == GEFIType.BITSET) {
      filter.asInstanceOf[mutable.BitSet](elem)
    } else {
      false
    }
  }

  def union(f: GEFI): GEFI = {
    if (filterType == GEFIType.BLOOM) {
      filter.asInstanceOf[BloomFilter[Integer]].union(f.filter.asInstanceOf[BloomFilter[Integer]])
    } else if (filterType == GEFIType.ROARING) {
      filter.asInstanceOf[RoaringBitmap].or(f.filter.asInstanceOf[RoaringBitmap])
    } else if (filterType == GEFIType.BITSET) {
      filter.asInstanceOf[mutable.BitSet] ++= filter.asInstanceOf[mutable.BitSet]
    }
    this
  }

  override def toString: String = {
    s"$filterType $size $falsePositiveRate"
  }
}

