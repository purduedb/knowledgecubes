package edu.purdue.knowledgecubes.storage.cache

object CacheEntryType extends Enumeration {
  type CacheEntryType = Value
  val ORIGINAL, JOIN_REDUCTION = Value
}
