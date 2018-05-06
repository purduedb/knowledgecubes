package edu.purdue.knowledgecubes.GEFI

@SerialVersionUID(8613759905650841937L)
object GEFIType extends Enumeration with Serializable {
  val BLOOM, CUCKOO, ROARING, BITSET, NONE = Value
}
