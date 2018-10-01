package edu.purdue.knowledgecubes

import java.io.{File, FileWriter, PrintWriter}

import scala.collection.mutable
import scala.io.Source

import com.typesafe.scalalogging.Logger
import org.fusesource.leveldbjni.JniDBFactory.{asString, bytes, factory}
import org.iq80.leveldb.Options
import org.slf4j.LoggerFactory

import edu.purdue.knowledgecubes.utils.CliParser

object DictionaryEncoderCLI {

  val LOG = Logger(LoggerFactory.getLogger(getClass))

  def main(args: Array[String]): Unit = {
    val params = CliParser.parseEncoder(args)
    val localPath = params("local")
    val input = params("input")
    val output = params("output")
    val separator = params("separator")

    val directory = new File(localPath)
    if (!directory.exists) {
      directory.mkdirs()
    }

    // Dictionary
    val options = new Options()
    options.createIfMissing()
    options.cacheSize(1000000 * 1048576) // 1G
    val dictionary = factory.open(new File(localPath + "/dictionary"), options)

    // Output file(s)
    val encodedDataset = new PrintWriter(new FileWriter(output))
    val predicatesFile = new PrintWriter(new FileWriter(localPath + "/predicates_ids.tsv"))

    LOG.info(s"Processing N-Triples file $input")
    var counter = 0
    var numLines = 0
    var startTime = System.currentTimeMillis() / 1000
    var stopTime = 0L
    for (line <- Source.fromFile(input).getLines) {
      numLines += 1
      if (numLines % 10000000 == 0) {
        stopTime = (System.currentTimeMillis() / 1000) - startTime
        LOG.info(s"Processed $numLines Triples in $stopTime seconds")
        startTime = System.currentTimeMillis() / 1000
      }
      var value = ""
      if (line.endsWith(".")) {
        value = line.substring(0, line.length - 2)
      }
      var parts = value.split(" ") // Split on space
      if (separator.equals("tab")) {
        parts = value.split("\t") // Split on tabs
      }
      var sub = parts(0)
      if (sub.startsWith("<") && sub.endsWith(">")) {
        sub = sub.replace("<", "").replace(">", "")
      }
      var s = dictionary.get(bytes(sub))
      if (s == null) {
        counter += 1
        s = bytes(counter.toString)
        dictionary.put(bytes(sub), s)
      }
      var pred = parts(1)
      if (pred.startsWith("<") && pred.endsWith(">")) {
        pred = pred.replace("<", "").replace(">", "")
      }
      var p = dictionary.get(bytes(pred))
      if (p == null) {
        counter += 1
        p = bytes(counter.toString)
        dictionary.put(bytes(pred), p)
        predicatesFile.println(s"$counter\t$pred")
      }
      var obj = parts.slice(2, parts.size).mkString(" ")
      if (obj.startsWith("<") && obj.endsWith(">")) {
        obj = obj.replace("<", "").replace(">", "")
      }
      var o = dictionary.get(bytes(obj))
      if (o == null) {
        counter += 1
        o = bytes(counter.toString)
        dictionary.put(bytes(obj), o)
      }
      encodedDataset.println(s"${asString(s)} ${asString(p)} ${asString(o)}")
    }
    dictionary.close()
    encodedDataset.close()
    predicatesFile.close()
  }

}
