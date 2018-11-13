package edu.purdue.knowledgecubes.utils

import java.io.{FileWriter, PrintWriter}

import scala.collection.mutable
import scala.io.Source

object YAGOCleaner {

  def main(args: Array[String]): Unit = {
    val fileName = args(0)
    val outFile = args(1)
    val printer = new PrintWriter(new FileWriter(outFile))
    var lst = mutable.Map[String, (Boolean, Boolean)]()
    for (line <- Source.fromFile(fileName).getLines()) {
      if (line.contains("hasLatitude") || line.contains("hasLongitude")) {
        val parts = line.split(" ")
        if(parts(1).contains("hasLatitude")) {
          if (lst.contains(parts(0))) {
            if (lst(parts(0))._1 == false) {
              printer.println(line)
              lst(parts(0)) = (true, lst(parts(0))._2)
            }
          } else {
            lst += (parts(0) -> (true, false))
            printer.println(line)
          }
        } else if (parts(1).contains("hasLongitude")) {
          if (lst.contains(parts(0))) {
            if (lst(parts(0))._2 == false) {
              printer.println(line)
              lst(parts(0)) = (lst(parts(0))._1, true)
            }
          } else {
            lst += (parts(0) -> (false, true))
            printer.println(line)
          }
        }
      } else {
        printer.println(line)
      }
    }
    printer.close()
  }
}
