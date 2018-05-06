package edu.purdue.knowledgecubes.storage.cache

import org.apache.spark.sql.Dataset

import edu.purdue.knowledgecubes.rdf.RDFTriple
import edu.purdue.knowledgecubes.storage.cache.CacheEntryType.CacheEntryType

class CacheEntry (val name: String, val size: Long, val entryType: CacheEntryType, val data: Dataset[RDFTriple]) {
  override def toString: String = {
    s"[${entryType.toString}] - Name: $name | Size: $size"
  }
}
