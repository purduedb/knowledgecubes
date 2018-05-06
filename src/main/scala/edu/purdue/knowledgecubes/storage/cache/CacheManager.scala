package edu.purdue.knowledgecubes.storage.cache

import scala.collection.mutable

import edu.purdue.knowledgecubes.storage.cache.CacheEntryType._

object CacheManager {

  // Entries are stored in HashMap by name to make lookup faster
  var entries : mutable.Map[String, CacheEntry] = mutable.Map[String, CacheEntry]()
  var numTriples: Long = 0
  var numJoinReductions: Long = 0
  var numOriginal: Long = 0

  def add(cacheEntry: CacheEntry): Boolean = {
    // TODO Check capacity before adding
    entries += (cacheEntry.name -> cacheEntry)
    numTriples += cacheEntry.size
    cacheEntry.entryType match {
      case ORIGINAL => numOriginal += 1
      case JOIN_REDUCTION => numJoinReductions += 1
    }
    true // for now
  }

  def get(entryName: String): CacheEntry = entries(entryName)

  def contains(entryName: String): Boolean = entries.contains(entryName)

  def clear(): Unit = {
    entries.foreach{ case(key, value) => value.data.unpersist(true) }
    entries.clear
    numOriginal = 0
    numJoinReductions = 0
    numTriples = 0
  }

  override def toString: String = {
    s"NUMENTRIES: ${entries.size} ORIGINAL: $numOriginal | JOIN_REDUCTIONS: $numJoinReductions | TRIPLES: $numTriples"
  }
}
