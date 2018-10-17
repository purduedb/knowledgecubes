package edu.purdue.knowledgecubes

import java.io.{File, FileWriter, PrintWriter}

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
    val dictDir = new File(localPath + "/dictionary")
    dictDir.mkdir()
    val options = new Options()
    options.createIfMissing()
    options.cacheSize(1000000 * 1048576) // 1G
    val dictionaryStr2Id = factory.open(new File(localPath + "/dictionary/Str2Id"), options)
    val dictionaryId2Str = factory.open(new File(localPath + "/dictionary/Id2Str"), options)

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
      var sId = dictionaryStr2Id.get(bytes(sub))
      if (sId == null) {
        counter += 1
        sId = bytes(counter.toString)
        dictionaryStr2Id.put(bytes(sub), sId)
        dictionaryId2Str.put(sId, bytes(sub))
      }
      var pred = parts(1)
      if (pred.startsWith("<") && pred.endsWith(">")) {
        pred = pred.replace("<", "").replace(">", "")
      }
      var pId = dictionaryStr2Id.get(bytes(pred))
      if (pId == null) {
        counter += 1
        pId = bytes(counter.toString)
        dictionaryStr2Id.put(bytes(pred), pId)
        dictionaryId2Str.put(pId, bytes(pred))
        predicatesFile.println(s"$counter\t$pred")
      }
      var obj = parts.slice(2, parts.size).mkString(" ")
      var objIsResource = false
      if (obj.startsWith("<") && obj.endsWith(">")) {
        objIsResource = true
        obj = obj.replace("<", "").replace(">", "")
      }
      var objVal = ""
      if (objIsResource) {
        var oId = dictionaryStr2Id.get(bytes(obj))
        if (oId == null) {
          counter += 1
          oId = bytes(counter.toString)
          dictionaryStr2Id.put(bytes(obj), oId)
          dictionaryId2Str.put(oId, bytes(obj))
        }
        objVal = asString(oId)
      } else {
        objVal = obj
      }
      encodedDataset.println(s"${asString(sId)} ${asString(pId)} ${objVal}")
    }
    dictionaryStr2Id.close()
    dictionaryId2Str.close()
    encodedDataset.close()
    predicatesFile.close()
  }
}
