package edu.purdue.knowledgecubes.utils

import java.io._

import org.fusesource.leveldbjni.JniDBFactory._
import org.iq80.leveldb._


object DictionaryToTsv {

  def main(args: Array[String]): Unit = {
    val options = new Options();
    options.createIfMissing(false)
    val db = factory.open(new File(args(0)), options)

    val iterator = db.iterator();
    try {
      iterator.seekToFirst()
      while(iterator.hasNext()) {
        val key = asString(iterator.peekNext().getKey())
        val value = asString(iterator.peekNext().getValue())
        println(value + "\t" + key)
        iterator.next()
      }
    } finally {
      iterator.close()
    }
  }

}
